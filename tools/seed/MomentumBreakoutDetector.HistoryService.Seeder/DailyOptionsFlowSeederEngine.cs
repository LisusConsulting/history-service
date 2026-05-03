using System.Diagnostics;
using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;
using TradingCalendar = MomentumBreakoutDetector.HistoryService.Domain.TradingCalendar;

namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// PR 2 — backfill engine for the <c>daily_options_flow</c> hypertable.
///
/// <para>
/// <b>Algorithm</b> (lifted from MBD's deleted
/// <c>OptionsVolumeBackfillService</c> — git commit <c>96ffcdd^</c>):
/// for each (underlying, trade_date) in the requested window:
/// <list type="number">
///   <item>Read short-DTE contracts from <c>historical_options_contracts</c>
///         (DTE 0..<see cref="SeedOptions.FlowMaxDte"/>, default 60). The
///         table is populated by the bars-surface seeder run that ran
///         earlier; the daily-flow seeder is a strict consumer of the
///         already-cached chain.</item>
///   <item>Fan out Polygon <c>/v2/aggs/ticker/{contract}/range/1/day/{date}/{date}</c>
///         per contract at concurrency = <see cref="SeedOptions.Concurrency"/>
///         (default 32). Each call yields <c>volume</c> for that contract
///         on that day; OI is not in this endpoint, so OI columns persist
///         as 0 (acceptable per migration-012 schema notes).</item>
///   <item>Aggregate <c>call_volume</c> + <c>put_volume</c> across the
///         contract universe; compute <c>flow_score = clamp((1 - put_side / call_side) * 0.7, -1, +1)</c>
///         and <c>put_call_ratio = put_side / call_side</c> where each
///         side = volume + 0.1 × OI (OI=0 in backfill).</item>
///   <item>UPSERT through <see cref="IDailyOptionsFlowProvider.UpsertAsync"/>.
///         Empty contract universe (no chain rows for the day) writes a
///         miss-marker via <see cref="IDailyOptionsFlowProvider.RecordMissAsync"/>.</item>
/// </list>
/// </para>
///
/// <para>
/// <b>Why direct DB rather than gRPC.</b> The daily-flow write surface
/// is not exposed through gRPC by design — it's a backfill operation
/// driven by the seeder + the daily 08:00 ET cron (PR 3) inside the
/// service container. Consumers (backtest engine) only ever READ via
/// <c>GetDailyOptionsFlow</c>. The seeder therefore writes through a
/// direct Postgres connection rather than going through a write-RPC the
/// service does not have.
/// </para>
/// </summary>
public sealed class DailyOptionsFlowSeederEngine
{
    private readonly SeedOptions m_Opts;
    private readonly Checkpoint m_Cp;
    private readonly StreamWriter? m_LogWriter;
    private readonly IOptionsService m_PolygonOptions;
    private readonly DailyOptionsFlowProvider m_FlowProvider;
    private readonly string m_PostgresConn;

    private long m_ContractsFetchedThisDay;
    private long m_ContractsFetchedTotal;
    private long m_ContractFailuresTotal;
    private long m_DaysWithRowsTotal;
    private long m_DaysWithMissTotal;
    private readonly Stopwatch m_RunSw = new();

    public DailyOptionsFlowSeederEngine(
        SeedOptions inOpts,
        Checkpoint inCp,
        IOptionsService inPolygonOptions,
        string inPostgresConn,
        StreamWriter? inLogWriter)
    {
        m_Opts = inOpts;
        m_Cp = inCp;
        m_PolygonOptions = inPolygonOptions;
        m_PostgresConn = inPostgresConn;
        m_LogWriter = inLogWriter;
        m_FlowProvider = new DailyOptionsFlowProvider(
            inPostgresConn,
            NullLogger<DailyOptionsFlowProvider>.Instance);
    }

    public async Task RunAsync(CancellationToken inCt)
    {
        m_RunSw.Start();

        var tmpStartFrom = m_Cp.LastCompletedDate is { } lc ? lc.AddDays(1) : m_Opts.From;
        var tmpDays = TradingCalendar.EnumerateTradingDays(tmpStartFrom, m_Opts.To).ToList();
        Log($"plan: surface=daily_options_flow {tmpDays.Count} trading day(s) to process " +
            $"(resume-from={tmpStartFrom:yyyy-MM-dd}) maxDte={m_Opts.FlowMaxDte} " +
            $"concurrency={m_Opts.Concurrency}");

        for (var i = 0; i < tmpDays.Count; i++)
        {
            inCt.ThrowIfCancellationRequested();
            var tmpDate = tmpDays[i];
            await ProcessDayAsync(tmpDate, i + 1, tmpDays.Count, inCt).ConfigureAwait(false);

            m_Cp.LastCompletedDate = tmpDate;
            m_Cp.TotalDaysFetched++;
            await m_Cp.SaveAsync(m_Opts.CheckpointFile, inCt).ConfigureAwait(false);
        }

        m_RunSw.Stop();
        Log("==================== final report (daily_options_flow) ====================");
        Log($"symbol               : {m_Opts.Symbol}");
        Log($"window               : {m_Opts.From:yyyy-MM-dd} .. {m_Opts.To:yyyy-MM-dd}");
        Log($"trading days         : {tmpDays.Count} (newly processed)");
        Log($"days with rows       : {m_DaysWithRowsTotal}");
        Log($"days with miss-marker: {m_DaysWithMissTotal}");
        Log($"polygon /v2/aggs calls: {m_ContractsFetchedTotal:N0} ({m_ContractFailuresTotal:N0} failures)");
        Log($"wall clock           : {FormatHms(m_RunSw.Elapsed)}");
        Log($"checkpoint           : {Path.GetFullPath(m_Opts.CheckpointFile)}");
    }

    private async Task ProcessDayAsync(DateOnly inDate, int inIndex, int inTotal, CancellationToken inCt)
    {
        var tmpDaySw = Stopwatch.StartNew();
        m_ContractsFetchedThisDay = 0;

        // 1. Read short-DTE contracts from historical_options_contracts
        //    for this (underlying, as_of=trade_date). The bars-surface
        //    seeder is expected to have populated this — if it returns
        //    empty, write a miss-marker for the (symbol, day) and move on.
        var tmpContracts = await ReadShortDteContractsAsync(inDate, inCt).ConfigureAwait(false);
        Log($"day {inDate:yyyy-MM-dd} ({inIndex}/{inTotal}): {tmpContracts.Count} short-DTE contract(s)");

        if (tmpContracts.Count == 0)
        {
            await m_FlowProvider.RecordMissAsync(
                m_Opts.Symbol, inDate, inDate, "no-chain-rows-cached", inCt).ConfigureAwait(false);
            Interlocked.Increment(ref m_DaysWithMissTotal);
            tmpDaySw.Stop();
            Log($"day {inDate:yyyy-MM-dd}: miss-marker (no chain rows cached) ({FormatHms(tmpDaySw.Elapsed)})");
            return;
        }

        // 2. Fan out per-contract /v2/aggs/.../1/day/{date}/{date} fetches.
        //    Concurrency-bounded — Polygon Advanced is uncapped per-endpoint
        //    so 32 is comfortable. Each fetch yields one daily volume value
        //    per contract (or 0 if Polygon returns no aggregate row).
        var tmpDateStr = inDate.ToString("yyyy-MM-dd");
        var tmpSem = new SemaphoreSlim(m_Opts.Concurrency, m_Opts.Concurrency);
        var tmpVolumes = new System.Collections.Concurrent.ConcurrentBag<(string ContractType, ulong Volume)>();
        var tmpTasks = new List<Task>(tmpContracts.Count);
        foreach (var tmpContract in tmpContracts)
        {
            inCt.ThrowIfCancellationRequested();
            await tmpSem.WaitAsync(inCt).ConfigureAwait(false);
            var tmpC = tmpContract;
            tmpTasks.Add(Task.Run(async () =>
            {
                try
                {
                    var tmpVol = await FetchContractDailyVolumeAsync(tmpC.Ticker, tmpDateStr, inCt)
                        .ConfigureAwait(false);
                    tmpVolumes.Add((tmpC.ContractType, tmpVol));
                    Interlocked.Increment(ref m_ContractsFetchedThisDay);
                    Interlocked.Increment(ref m_ContractsFetchedTotal);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Interlocked.Increment(ref m_ContractFailuresTotal);
                    Log($"WARN /v2/aggs fail {tmpC.Ticker}@{tmpDateStr}: {ex.GetType().Name}: {ex.Message}");
                }
                finally { tmpSem.Release(); }
            }, inCt));
        }
        await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        // 3. Aggregate. Same formula as MBD's
        //    OptionsAnalysisService.ComputeFlowScoreDetailed:
        //      call_side  = call_volume + 0.1 * call_oi   (OI=0 here)
        //      put_side   = put_volume  + 0.1 * put_oi    (OI=0 here)
        //      flow_score = clamp((1 - put_side / call_side) * 0.7, -1, +1)
        //      put_call_ratio = put_side / call_side
        var (tmpCallVol, tmpPutVol) = AggregateVolumes(tmpVolumes);
        var tmpAggregated = ComputeFlowScore(tmpCallVol, tmpPutVol);

        // 4. UPSERT one row through the provider. Idempotent on
        //    (underlying_ticker, trade_date). xmax-based RETURNING tells
        //    us insert vs update for the heartbeat log.
        var tmpRow = new DailyOptionsFlowRow(
            UnderlyingTicker: m_Opts.Symbol,
            TradeDate: inDate,
            CallVolume: (long)tmpCallVol,
            PutVolume: (long)tmpPutVol,
            CallOi: 0L,
            PutOi: 0L,
            PutCallRatio: tmpAggregated.PutCallRatio,
            FlowScore: tmpAggregated.FlowScore,
            ContractCount: tmpContracts.Count);
        await m_FlowProvider.UpsertAsync(new[] { tmpRow }, inCt).ConfigureAwait(false);
        Interlocked.Increment(ref m_DaysWithRowsTotal);

        tmpDaySw.Stop();
        Log($"day {inDate:yyyy-MM-dd}: UPSERT contracts={tmpContracts.Count} " +
            $"call_vol={tmpCallVol:N0} put_vol={tmpPutVol:N0} " +
            $"flow_score={(tmpAggregated.FlowScore?.ToString("F4") ?? "NULL")} " +
            $"pcr={(tmpAggregated.PutCallRatio?.ToString("F4") ?? "NULL")} " +
            $"({FormatHms(tmpDaySw.Elapsed)}, {m_ContractsFetchedThisDay} polygon calls)");
    }

    /// <summary>
    /// Read the short-DTE contract universe for (<see cref="SeedOptions.Symbol"/>,
    /// <paramref name="inTradeDate"/>) from the cache. DTE = expiration_date − as_of_date.
    /// The query joins the chain row's expiration to compute DTE inline so
    /// we don't pull 500-row chains that are mostly out-of-window.
    /// </summary>
    /// <returns>List of (ticker, contract_type) tuples — call/put downcased.</returns>
    private async Task<IReadOnlyList<DailyFlowContractRow>> ReadShortDteContractsAsync(
        DateOnly inTradeDate, CancellationToken inCt)
    {
        await using var tmpConn = new NpgsqlConnection(m_PostgresConn);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        var tmpRows = (await tmpConn.QueryAsync<DailyFlowContractRow>(
            """
            SELECT
              ticker          AS "Ticker",
              LOWER(contract_type) AS "ContractType"
            FROM historical_options_contracts
            WHERE underlying_ticker = @Symbol
              AND as_of_date = @AsOf::date
              AND contract_type IS NOT NULL
              AND expiration_date - as_of_date BETWEEN 0 AND @MaxDte
            ORDER BY ticker
            """,
            new
            {
                Symbol = m_Opts.Symbol,
                AsOf = inTradeDate.ToString("yyyy-MM-dd"),
                MaxDte = m_Opts.FlowMaxDte,
            }).ConfigureAwait(false)).ToList();

        return tmpRows;
    }

    /// <summary>
    /// Fetch one day of OHLC for one contract via Polygon
    /// <c>/v2/aggs/ticker/{contract}/range/1/day/{date}/{date}</c>.
    /// Returns 0 when the response has no aggregate (a contract with no
    /// trades that day; legitimate). Throws on transport / 5xx errors so
    /// the caller can record a fetch failure.
    /// </summary>
    private async Task<ulong> FetchContractDailyVolumeAsync(
        string inTicker, string inDateStr, CancellationToken inCt)
    {
        var tmpReq = new GetBarsRequest
        {
            OptionsTicker = inTicker,
            Multiplier = 1,
            Timespan = AggregateInterval.Day,
            From = inDateStr,
            To = inDateStr,
            Adjusted = true,
            Limit = 1,
        };
        var tmpResp = await m_PolygonOptions.GetBarsAsync(tmpReq, inCt).ConfigureAwait(false);
        var tmpBars = tmpResp?.Results;
        if (tmpBars is null || tmpBars.Count == 0) return 0UL;
        return tmpBars[0].Volume ?? 0UL;
    }

    /// <summary>
    /// Sum daily volume by call/put across the contract universe.
    /// Internal so the provider integration test can pin the math directly.
    /// </summary>
    internal static (ulong CallVolume, ulong PutVolume) AggregateVolumes(
        IEnumerable<(string ContractType, ulong Volume)> inRows)
    {
        ulong tmpCall = 0, tmpPut = 0;
        foreach (var tmpRow in inRows)
        {
            switch (tmpRow.ContractType)
            {
                case "call": tmpCall += tmpRow.Volume; break;
                case "put":  tmpPut += tmpRow.Volume;  break;
                default: break;
            }
        }
        return (tmpCall, tmpPut);
    }

    /// <summary>
    /// MBD <c>OptionsAnalysisService.ComputeFlowScoreDetailed</c>-equivalent
    /// score. Internal so tests can pin the math without standing up the
    /// full Polygon + DB stack.
    /// </summary>
    internal static AggregatedFlow ComputeFlowScore(
        ulong inCallVolume, ulong inPutVolume,
        ulong inCallOi = 0UL, ulong inPutOi = 0UL)
    {
        var tmpCallSide = (decimal)inCallVolume + 0.1m * (decimal)inCallOi;
        var tmpPutSide = (decimal)inPutVolume + 0.1m * (decimal)inPutOi;

        if (tmpCallSide <= 0m)
        {
            // Undefined ratio + score → NULL columns. Same shape as the
            // legacy MBD aggregator in commit 96ffcdd^.
            return new AggregatedFlow(PutCallRatio: null, FlowScore: null);
        }

        var tmpRatio = tmpPutSide / tmpCallSide;
        var tmpRaw = (1m - tmpRatio) * 0.7m;
        var tmpClamped = tmpRaw < -1m ? -1m : (tmpRaw > 1m ? 1m : tmpRaw);
        return new AggregatedFlow(
            PutCallRatio: Math.Round(tmpRatio, 4),
            FlowScore: Math.Round(tmpClamped, 4));
    }

    /// <summary>Aggregator output. Both nullable (call-side=0 ⇒ undefined).</summary>
    internal sealed record AggregatedFlow(decimal? PutCallRatio, decimal? FlowScore);

    /// <summary>Internal Dapper mapping row for the chain-cache read.</summary>
    private sealed record DailyFlowContractRow(string Ticker, string ContractType);

    private static string FormatHms(TimeSpan inTs)
        => $"{(int)inTs.TotalHours}h {inTs.Minutes:D2}m {inTs.Seconds:D2}s";

    private void Log(string inMsg)
    {
        var tmpLine = $"[{DateTime.UtcNow:HH:mm:ss}] {inMsg}";
        Console.WriteLine(tmpLine);
        m_LogWriter?.WriteLine(tmpLine);
        m_LogWriter?.Flush();
    }
}
