using Dapper;
using Microsoft.Extensions.Logging;
using Npgsql;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Per-day computer for the daily_options_flow surface. Reads short-DTE
/// contracts from <c>historical_options_contracts</c>, fans out Polygon
/// <c>/v2/aggs/ticker/{contract}/range/1/day/{date}/{date}</c> at the
/// configured concurrency, aggregates per the canonical formula, and
/// returns a populated <see cref="DailyOptionsFlowRow"/> (or null when
/// the contract universe is empty — caller writes a miss-marker).
///
/// <para>
/// PR 3 — extracted so the daily 08:00 ET cron
/// (<see cref="HostedServices.DailyOptionsFlowRefreshService"/>) and the
/// PR 2 seeder share one implementation of the per-day computation. The
/// seeder's <c>DailyOptionsFlowSeederEngine</c> drives this for an
/// arbitrary date window; the cron drives it once per trading day for
/// "yesterday" against a tracked-symbol list.
/// </para>
///
/// <para>
/// Algorithm (verified against MBD's deleted
/// <c>OptionsVolumeBackfillService</c> and live
/// <c>OptionsAnalysisService.ComputeFlowScoreDetailed</c>):
/// <code>
///   call_side  = call_volume + 0.1 * call_oi   (OI=0 from /v2/aggs)
///   put_side   = put_volume  + 0.1 * put_oi
///   flow_score = clamp((1 - put_side / call_side) * 0.7, -1, +1)
///   put_call_ratio = put_side / call_side
/// </code>
/// </para>
/// </summary>
public interface IDailyOptionsFlowComputer
{
    /// <summary>
    /// Compute the aggregated put/call flow row for
    /// (<paramref name="inSymbol"/>, <paramref name="inTradeDate"/>).
    /// Returns <c>null</c> when the chain cache has no rows for that
    /// (symbol, day) — caller treats as a miss and may write a
    /// miss-marker via <see cref="IDailyOptionsFlowProvider.RecordMissAsync"/>.
    /// </summary>
    /// <param name="inSymbol">Underlying ticker (e.g. "TSLA").</param>
    /// <param name="inTradeDate">Trading day to aggregate.</param>
    /// <param name="inMaxDte">Maximum DTE (days-to-expiry) for the contract universe. Default 60.</param>
    /// <param name="inConcurrency">Concurrency cap on per-contract /v2/aggs fetches. Default 32.</param>
    /// <param name="inCt">Cancellation token.</param>
    Task<DailyOptionsFlowRow?> ComputeAsync(
        string inSymbol,
        DateOnly inTradeDate,
        int inMaxDte = 60,
        int inConcurrency = 32,
        CancellationToken inCt = default);
}

/// <summary>
/// Postgres + Polygon-SDK backed implementation of
/// <see cref="IDailyOptionsFlowComputer"/>.
/// </summary>
public sealed class DailyOptionsFlowComputer : IDailyOptionsFlowComputer
{
    private readonly string m_ConnectionString;
    private readonly IOptionsService m_PolygonOptions;
    private readonly ILogger<DailyOptionsFlowComputer> m_Logger;

    public DailyOptionsFlowComputer(
        string inConnectionString,
        IOptionsService inPolygonOptions,
        ILogger<DailyOptionsFlowComputer> inLogger)
    {
        m_ConnectionString = inConnectionString;
        m_PolygonOptions = inPolygonOptions;
        m_Logger = inLogger;
    }

    public async Task<DailyOptionsFlowRow?> ComputeAsync(
        string inSymbol,
        DateOnly inTradeDate,
        int inMaxDte = 60,
        int inConcurrency = 32,
        CancellationToken inCt = default)
    {
        // 1. Read short-DTE contracts (DTE 0..MaxDte) from the chain cache.
        var tmpContracts = await ReadShortDteContractsAsync(inSymbol, inTradeDate, inMaxDte, inCt)
            .ConfigureAwait(false);
        if (tmpContracts.Count == 0)
        {
            m_Logger.LogInformation(
                "DailyOptionsFlowComputer: no chain rows cached for {Symbol} {Date} (DTE 0..{Dte}) — caller should record miss",
                inSymbol, inTradeDate, inMaxDte);
            return null;
        }

        // 2. Fan out per-contract /v2/aggs/...range/1/day/{date}/{date}.
        var tmpDateStr = inTradeDate.ToString("yyyy-MM-dd");
        var tmpSem = new SemaphoreSlim(inConcurrency, inConcurrency);
        var tmpVolumes = new System.Collections.Concurrent.ConcurrentBag<(string ContractType, ulong Volume)>();
        var tmpFailures = 0;
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
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Interlocked.Increment(ref tmpFailures);
                    m_Logger.LogWarning(ex,
                        "DailyOptionsFlowComputer: /v2/aggs failed for {Ticker} {Date}",
                        tmpC.Ticker, tmpDateStr);
                }
                finally { tmpSem.Release(); }
            }, inCt));
        }
        await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        // 3. Aggregate + compute flow score.
        var (tmpCallVol, tmpPutVol) = AggregateVolumes(tmpVolumes);
        var (tmpRatio, tmpScore) = ComputeFlowScore(tmpCallVol, tmpPutVol);

        m_Logger.LogInformation(
            "DailyOptionsFlowComputer: {Symbol} {Date} contracts={Contracts} call_vol={CallVol:N0} put_vol={PutVol:N0} ratio={Ratio} score={Score} failures={Failures}",
            inSymbol, inTradeDate, tmpContracts.Count, tmpCallVol, tmpPutVol,
            tmpRatio?.ToString("F4") ?? "NULL", tmpScore?.ToString("F4") ?? "NULL", tmpFailures);

        return new DailyOptionsFlowRow(
            UnderlyingTicker: inSymbol,
            TradeDate: inTradeDate,
            CallVolume: (long)tmpCallVol,
            PutVolume: (long)tmpPutVol,
            CallOi: 0L,
            PutOi: 0L,
            PutCallRatio: tmpRatio,
            FlowScore: tmpScore,
            ContractCount: tmpContracts.Count);
    }

    /// <summary>
    /// Read the short-DTE contract universe for (<paramref name="inSymbol"/>,
    /// <paramref name="inTradeDate"/>) from <c>historical_options_contracts</c>.
    /// DTE filter <c>0..inMaxDte</c> matches MBD <c>OptionsAnalysisService.MAX_DTE</c>.
    /// </summary>
    private async Task<IReadOnlyList<ContractKey>> ReadShortDteContractsAsync(
        string inSymbol, DateOnly inTradeDate, int inMaxDte, CancellationToken inCt)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        var tmpRows = (await tmpConn.QueryAsync<ContractKey>(
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
                Symbol = inSymbol,
                AsOf = inTradeDate.ToString("yyyy-MM-dd"),
                MaxDte = inMaxDte,
            }).ConfigureAwait(false)).ToList();

        return tmpRows;
    }

    /// <summary>
    /// Fetch one day of OHLC for one contract via Polygon
    /// <c>/v2/aggs/ticker/{contract}/range/1/day/{date}/{date}</c>.
    /// Returns 0 when the response has no aggregate row (legitimate —
    /// a contract with no trades that day).
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
    /// Internal so tests can pin the math.
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
    internal static (decimal? PutCallRatio, decimal? FlowScore) ComputeFlowScore(
        ulong inCallVolume, ulong inPutVolume,
        ulong inCallOi = 0UL, ulong inPutOi = 0UL)
    {
        var tmpCallSide = (decimal)inCallVolume + 0.1m * (decimal)inCallOi;
        var tmpPutSide = (decimal)inPutVolume + 0.1m * (decimal)inPutOi;

        if (tmpCallSide <= 0m)
        {
            return (PutCallRatio: null, FlowScore: null);
        }

        var tmpRatio = tmpPutSide / tmpCallSide;
        var tmpRaw = (1m - tmpRatio) * 0.7m;
        var tmpClamped = tmpRaw < -1m ? -1m : (tmpRaw > 1m ? 1m : tmpRaw);
        return (
            PutCallRatio: Math.Round(tmpRatio, 4),
            FlowScore: Math.Round(tmpClamped, 4));
    }

    /// <summary>Internal Dapper mapping row for the chain-cache read.</summary>
    private sealed record ContractKey(string Ticker, string ContractType);
}
