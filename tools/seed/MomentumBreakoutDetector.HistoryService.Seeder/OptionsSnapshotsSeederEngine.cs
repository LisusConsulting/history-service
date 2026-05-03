using System.Diagnostics;
using Dapper;
using MomentumBreakoutDetector.HistoryService.Pricing.BlackScholes;
using Npgsql;
using TradingCalendar = MomentumBreakoutDetector.HistoryService.Domain.TradingCalendar;

namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// Wave B / PR 3 — backfill engine for the
/// <c>historical_options_snapshots</c> hypertable using the Black-Scholes
/// solver added in Wave A / PR 2.
///
/// <para>
/// <b>Algorithm</b> (per plan
/// docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md, STEP 3):
/// for each trading day in the requested window,
/// <list type="number">
///   <item>Look up the underlying's daily close from
///         <c>historical_bars</c> (timeframe='day').</item>
///   <item>Compute the ATM strike band [S * (1 - <see cref="SeedOptions.StrikeBandPct"/>),
///         S * (1 + StrikeBandPct)] (default ±5%).</item>
///   <item>Read short-DTE contracts from <c>historical_options_contracts</c>
///         filtered to the strike band × DTE [0, 60].</item>
///   <item>Look up the latest NBBO ≤ EOD timestamp from
///         <c>historical_options_quotes</c> for each contract.</item>
///   <item>Look up the latest DGS3MO observation ≤ trade_date from
///         <c>macro_data</c> for the risk-free rate.</item>
///   <item>Compute mid = (bid + ask) / 2; skip rows with bid &gt;= ask /
///         bid &lt;= 0 / ask &lt;= 0 (write the snapshot row with NULL
///         IV/greeks).</item>
///   <item>Compute T = (expiration_date - trade_date in trading days) / 252.
///         Same-day expiry → skip the contract entirely (T=0 is degenerate
///         per BS).</item>
///   <item>Call <see cref="IBlackScholesSolver.Solve"/> → outputs or null.
///         null on solver failure → snapshot row written with NULL
///         IV/greeks (matches Polygon's "deep ITM/OTM not computable"
///         pattern).</item>
///   <item>UPSERT one row per contract into <c>historical_options_snapshots</c>
///         with <c>source='computed_bs'</c>.</item>
///   <item>Track per-day stats: contracts attempted, contracts with NULL
///         IV (failure rate), wall-clock per day. Plan's failure-rate
///         target is &lt;15% (≥85% convergence).</item>
/// </list>
/// </para>
///
/// <para>
/// <b>Direct-DB write.</b> Same pattern as
/// <see cref="DailyOptionsFlowSeederEngine"/>: this surface has no gRPC
/// write contract by design (PR 4 live-capture writes directly too).
/// Consumers READ via the gRPC GetDailyAtmIv (PR 5).
/// </para>
///
/// <para>
/// <b>Idempotency.</b> The UPSERT keys on (ticker, snapshot_date), and
/// the snapshot_date is a deterministic per-contract EOD timestamp
/// (trade_date 21:00 UTC = 16:00 ET, the standard NYSE closing instant
/// post-DST conversion). Re-running the seeder on the same window
/// rewrites the same rows. Safe to abort + resume.
/// </para>
/// </summary>
public sealed class OptionsSnapshotsSeederEngine
{
    private readonly SeedOptions m_Opts;
    private readonly Checkpoint m_Cp;
    private readonly StreamWriter? m_LogWriter;
    private readonly IBlackScholesSolver m_Solver;
    private readonly string m_PostgresConn;

    private long m_ContractsAttemptedTotal;
    private long m_RowsPersistedTotal;
    private long m_NullIvTotal;
    private long m_DaysWithRowsTotal;
    private long m_DaysWithMissTotal;
    private readonly Stopwatch m_RunSw = new();

    public OptionsSnapshotsSeederEngine(
        SeedOptions inOpts,
        Checkpoint inCp,
        IBlackScholesSolver inSolver,
        string inPostgresConn,
        StreamWriter? inLogWriter)
    {
        m_Opts = inOpts;
        m_Cp = inCp;
        m_Solver = inSolver;
        m_PostgresConn = inPostgresConn;
        m_LogWriter = inLogWriter;
    }

    public async Task RunAsync(CancellationToken inCt)
    {
        m_RunSw.Start();

        var tmpStartFrom = m_Cp.LastCompletedDate is { } lc ? lc.AddDays(1) : m_Opts.From;
        var tmpDays = TradingCalendar.EnumerateTradingDays(tmpStartFrom, m_Opts.To).ToList();
        Log($"plan: surface=options_snapshots compute=bs {tmpDays.Count} trading day(s) " +
            $"(resume-from={tmpStartFrom:yyyy-MM-dd}) strike-band=±{m_Opts.StrikeBandPct:P0} " +
            $"dte-max={m_Opts.SnapshotDteMaxDays}");

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
        var tmpFailureRate = m_ContractsAttemptedTotal > 0
            ? (double)m_NullIvTotal / m_ContractsAttemptedTotal
            : 0.0;
        Log("==================== final report (options_snapshots / bs) ====================");
        Log($"symbol               : {m_Opts.Symbol}");
        Log($"window               : {m_Opts.From:yyyy-MM-dd} .. {m_Opts.To:yyyy-MM-dd}");
        Log($"trading days         : {tmpDays.Count} (newly processed)");
        Log($"days with rows       : {m_DaysWithRowsTotal}");
        Log($"days with miss-marker: {m_DaysWithMissTotal}");
        Log($"contracts attempted  : {m_ContractsAttemptedTotal:N0}");
        Log($"rows persisted       : {m_RowsPersistedTotal:N0}");
        Log($"null-IV (solver fail): {m_NullIvTotal:N0} ({tmpFailureRate:P2})");
        if (tmpFailureRate > 0.15)
        {
            Log("WARNING: null-IV rate > 15% — plan calibration target not met. " +
                "Investigate solver tuning or NBBO data quality before declaring done.");
        }
        Log($"wall clock           : {FormatHms(m_RunSw.Elapsed)}");
        Log($"checkpoint           : {Path.GetFullPath(m_Opts.CheckpointFile)}");
    }

    private async Task ProcessDayAsync(DateOnly inDate, int inIndex, int inTotal, CancellationToken inCt)
    {
        var tmpDaySw = Stopwatch.StartNew();

        await using var tmpConn = new NpgsqlConnection(m_PostgresConn);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        // 1. Underlying close. Day timeframe = "day". If absent, can't
        //    compute the ATM band — record a miss-marker and continue.
        var tmpClose = await GetUnderlyingCloseAsync(tmpConn, m_Opts.Symbol, inDate, inCt)
            .ConfigureAwait(false);
        if (tmpClose is null)
        {
            await RecordDayMissAsync(tmpConn, inDate, "no-bars-cached", inCt).ConfigureAwait(false);
            Interlocked.Increment(ref m_DaysWithMissTotal);
            tmpDaySw.Stop();
            Log($"day {inDate:yyyy-MM-dd} ({inIndex}/{inTotal}): MISS (no daily bar) ({FormatHms(tmpDaySw.Elapsed)})");
            return;
        }

        // 2. Strike band.
        var tmpBandPct = (decimal)m_Opts.StrikeBandPct;
        var tmpKLow = tmpClose.Value * (1m - tmpBandPct);
        var tmpKHigh = tmpClose.Value * (1m + tmpBandPct);

        // 3. ATM-band × short-DTE contracts.
        var tmpContracts = await ReadAtmBandContractsAsync(
            tmpConn, m_Opts.Symbol, inDate, tmpKLow, tmpKHigh,
            m_Opts.SnapshotDteMaxDays, inCt).ConfigureAwait(false);

        if (tmpContracts.Count == 0)
        {
            await RecordDayMissAsync(tmpConn, inDate, "no-atm-band-contracts", inCt).ConfigureAwait(false);
            Interlocked.Increment(ref m_DaysWithMissTotal);
            tmpDaySw.Stop();
            Log($"day {inDate:yyyy-MM-dd} ({inIndex}/{inTotal}): MISS (no ATM-band contracts) " +
                $"close={tmpClose:F2} band=[{tmpKLow:F2}, {tmpKHigh:F2}] ({FormatHms(tmpDaySw.Elapsed)})");
            return;
        }

        // 4. DGS3MO at trade_date — pulled once per day, not per contract.
        var tmpRfRate = await GetDgs3MoRateAsync(tmpConn, inDate, inCt).ConfigureAwait(false);

        // 5. EOD timestamp (16:00 ET converted to UTC). Used as the
        //    snapshot_date primary key value AND as the as-of cap for
        //    NBBO lookup.
        var tmpEodUtc = ComputeRthCloseUtc(inDate);

        // 6. Per-contract: NBBO lookup → BS solve → row persist.
        //    Sequential — no Polygon fan-out needed (everything is local
        //    DB reads + pure-CPU solves). Single-threaded simplicity wins
        //    over the marginal speed-up of parallel BS solves.
        var tmpAttempted = 0;
        var tmpRowsPersisted = 0;
        var tmpNullIv = 0;
        foreach (var tmpContract in tmpContracts)
        {
            inCt.ThrowIfCancellationRequested();
            tmpAttempted++;

            var tmpNbbo = await GetLatestNbboAsync(tmpConn, tmpContract.Ticker, tmpEodUtc, inCt)
                .ConfigureAwait(false);

            // No NBBO at all → can't compute. Persist the snapshot row
            // with NULL IV/greeks (and NULL bid/ask) so consumers see the
            // attempted day; no miss-marker for this single contract.
            BlackScholesOutputs? tmpOutputs = null;
            var tmpDte = TradingCalendar.EnumerateTradingDays(
                inDate.AddDays(1), tmpContract.ExpirationDate).Count();

            if (tmpNbbo is { } nbbo
                && tmpContract.StrikePrice is { } strike
                && nbbo.BidPrice > 0 && nbbo.AskPrice > 0
                && nbbo.BidPrice <= nbbo.AskPrice
                && tmpDte > 0)
            {
                var tmpMid = (nbbo.BidPrice + nbbo.AskPrice) / 2m;
                var tmpType = string.Equals(tmpContract.ContractType, "call", StringComparison.OrdinalIgnoreCase)
                    ? OptionType.Call : OptionType.Put;
                var tmpT = (decimal)tmpDte / 252m;
                tmpOutputs = m_Solver.Solve(new BlackScholesInputs(
                    UnderlyingPrice: tmpClose.Value,
                    Strike: strike,
                    TimeToExpirationYears: tmpT,
                    RiskFreeRate: tmpRfRate,
                    MidPrice: tmpMid,
                    Type: tmpType), inCt);
            }

            if (tmpOutputs is null) tmpNullIv++;

            await UpsertSnapshotAsync(
                tmpConn,
                inTicker: tmpContract.Ticker,
                inSnapshotDate: tmpEodUtc,
                inBid: tmpNbbo?.BidPrice,
                inAsk: tmpNbbo?.AskPrice,
                inUnderlyingPrice: tmpClose.Value,
                inOutputs: tmpOutputs,
                inCt: inCt).ConfigureAwait(false);
            tmpRowsPersisted++;
        }

        Interlocked.Add(ref m_ContractsAttemptedTotal, tmpAttempted);
        Interlocked.Add(ref m_RowsPersistedTotal, tmpRowsPersisted);
        Interlocked.Add(ref m_NullIvTotal, tmpNullIv);
        Interlocked.Increment(ref m_DaysWithRowsTotal);

        tmpDaySw.Stop();
        var tmpDayFailureRate = tmpAttempted > 0 ? (double)tmpNullIv / tmpAttempted : 0.0;
        Log($"day {inDate:yyyy-MM-dd} ({inIndex}/{inTotal}): close={tmpClose:F2} " +
            $"band=[{tmpKLow:F2}, {tmpKHigh:F2}] r={tmpRfRate:F4} " +
            $"contracts={tmpContracts.Count} persisted={tmpRowsPersisted} null-iv={tmpNullIv} " +
            $"({tmpDayFailureRate:P1}) ({FormatHms(tmpDaySw.Elapsed)})");
    }

    /// <summary>
    /// Read the underlying close from <c>historical_bars</c> for
    /// timeframe='day' on <paramref name="inDate"/>. Returns null if the
    /// row is absent (caller writes a miss-marker).
    /// </summary>
    private static async Task<decimal?> GetUnderlyingCloseAsync(
        NpgsqlConnection inConn, string inSymbol, DateOnly inDate, CancellationToken inCt)
    {
        // Day-timeframe bars are stored at 00:00 UTC on the trade_date.
        // Filter on the date alone (a small UTC window covers any
        // off-by-DST drift).
        var tmpRows = await inConn.QueryAsync<decimal?>(
            """
            SELECT close FROM historical_bars
            WHERE symbol = @Symbol
              AND timeframe = 'day'
              AND timestamp::date = @Date::date
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            new { Symbol = inSymbol, Date = inDate.ToString("yyyy-MM-dd") }).ConfigureAwait(false);
        return tmpRows.FirstOrDefault();
    }

    /// <summary>
    /// Read the latest DGS3MO observation at-or-before <paramref name="inDate"/>
    /// from <c>macro_data</c>. Returns 0 if nothing is cached (caller's
    /// BS solve still runs; r=0 is a recoverable approximation rather
    /// than a fatal failure).
    /// </summary>
    private static async Task<decimal> GetDgs3MoRateAsync(
        NpgsqlConnection inConn, DateOnly inDate, CancellationToken inCt)
    {
        var tmpRows = await inConn.QueryAsync<decimal?>(
            """
            SELECT value FROM macro_data
            WHERE series_id = 'DGS3MO'
              AND observation_date <= @Date::date
            ORDER BY observation_date DESC
            LIMIT 1
            """,
            new { Date = inDate.ToString("yyyy-MM-dd") }).ConfigureAwait(false);

        // FRED publishes DGS3MO as a percent (e.g. 5.25 means 5.25%).
        // BS expects it as a decimal fraction (0.0525).
        var tmpPct = tmpRows.FirstOrDefault() ?? 0m;
        return tmpPct / 100m;
    }

    /// <summary>
    /// Read ATM-band contracts (strike in [<paramref name="inKLow"/>,
    /// <paramref name="inKHigh"/>], DTE in [0, <paramref name="inMaxDte"/>])
    /// for (<paramref name="inSymbol"/>, <paramref name="inDate"/>) from
    /// <c>historical_options_contracts</c>.
    /// </summary>
    private static async Task<IReadOnlyList<SnapshotContractRow>> ReadAtmBandContractsAsync(
        NpgsqlConnection inConn, string inSymbol, DateOnly inDate,
        decimal inKLow, decimal inKHigh, int inMaxDte, CancellationToken inCt)
    {
        var tmpRows = (await inConn.QueryAsync<RawContractRow>(
            """
            SELECT
              ticker          AS Ticker,
              LOWER(contract_type) AS ContractType,
              expiration_date::text AS ExpirationDateText,
              strike_price    AS StrikePrice
            FROM historical_options_contracts
            WHERE underlying_ticker = @Symbol
              AND as_of_date = @AsOf::date
              AND contract_type IS NOT NULL
              AND strike_price IS NOT NULL
              AND strike_price BETWEEN @KLow AND @KHigh
              AND expiration_date - as_of_date BETWEEN 0 AND @MaxDte
            ORDER BY ticker
            """,
            new
            {
                Symbol = inSymbol,
                AsOf = inDate.ToString("yyyy-MM-dd"),
                KLow = inKLow,
                KHigh = inKHigh,
                MaxDte = inMaxDte,
            }).ConfigureAwait(false))
            .Select(r => new SnapshotContractRow(
                Ticker: r.Ticker,
                ContractType: r.ContractType,
                ExpirationDate: DateOnly.Parse(r.ExpirationDateText),
                StrikePrice: r.StrikePrice))
            .ToList();
        return tmpRows;
    }

    /// <summary>
    /// Read the latest NBBO row for <paramref name="inTicker"/> at-or-before
    /// <paramref name="inAtUtc"/> from <c>historical_options_quotes</c>.
    /// Returns null when no row exists (the caller persists the snapshot
    /// with NULL bid/ask + NULL IV/greeks).
    /// </summary>
    private static async Task<NbboRow?> GetLatestNbboAsync(
        NpgsqlConnection inConn, string inTicker, DateTime inAtUtc, CancellationToken inCt)
    {
        var tmpRows = await inConn.QueryAsync<NbboRow>(
            """
            SELECT
              bid_price        AS BidPrice,
              ask_price        AS AskPrice
            FROM historical_options_quotes
            WHERE ticker = @Ticker
              AND ts <= @AtUtc
              AND bid_price IS NOT NULL
              AND ask_price IS NOT NULL
            ORDER BY ts DESC
            LIMIT 1
            """,
            new { Ticker = inTicker, AtUtc = inAtUtc }).ConfigureAwait(false);
        return tmpRows.FirstOrDefault();
    }

    /// <summary>
    /// UPSERT one snapshot row. <c>source='computed_bs'</c> is hardcoded;
    /// the bootstrap import (Wave C / PR 9) is the only writer that
    /// persists <c>polygon_live</c> rows for historical dates and runs
    /// from a dedicated import tool, not this seeder.
    /// </summary>
    private static async Task UpsertSnapshotAsync(
        NpgsqlConnection inConn,
        string inTicker, DateTime inSnapshotDate,
        decimal? inBid, decimal? inAsk, decimal inUnderlyingPrice,
        BlackScholesOutputs? inOutputs, CancellationToken inCt)
    {
        await inConn.ExecuteAsync(
            """
            INSERT INTO historical_options_snapshots
              (ticker, snapshot_date, bid_price, ask_price,
               implied_volatility, delta, gamma, theta, vega,
               underlying_price, source)
            VALUES
              (@Ticker, @SnapshotDate, @Bid, @Ask,
               @Iv, @Delta, @Gamma, @Theta, @Vega,
               @Underlying, 'computed_bs')
            ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
              bid_price          = EXCLUDED.bid_price,
              ask_price          = EXCLUDED.ask_price,
              implied_volatility = EXCLUDED.implied_volatility,
              delta              = EXCLUDED.delta,
              gamma              = EXCLUDED.gamma,
              theta              = EXCLUDED.theta,
              vega               = EXCLUDED.vega,
              underlying_price   = EXCLUDED.underlying_price,
              source             = EXCLUDED.source
            """,
            new
            {
                Ticker = inTicker,
                SnapshotDate = inSnapshotDate,
                Bid = (object?)inBid ?? DBNull.Value,
                Ask = (object?)inAsk ?? DBNull.Value,
                Iv = (object?)inOutputs?.Iv ?? DBNull.Value,
                Delta = (object?)inOutputs?.Delta ?? DBNull.Value,
                Gamma = (object?)inOutputs?.Gamma ?? DBNull.Value,
                Theta = (object?)inOutputs?.Theta ?? DBNull.Value,
                Vega = (object?)inOutputs?.Vega ?? DBNull.Value,
                Underlying = inUnderlyingPrice,
            }).ConfigureAwait(false);
    }

    /// <summary>
    /// Record a per-day miss marker. We use a one-day range (range_from =
    /// range_to = the day's EOD timestamp converted to a midnight UTC for
    /// each end). The misses table coalesces adjacent ranges via the
    /// shared writer downstream — out of scope for the seeder, which
    /// uses the simpler INSERT ON CONFLICT DO NOTHING shape.
    /// </summary>
    private static async Task RecordDayMissAsync(
        NpgsqlConnection inConn, DateOnly inDate, string inReason, CancellationToken inCt)
    {
        // The misses table for snapshot uses TIMESTAMPTZ bounds — record
        // the day as 00:00 UTC to 23:59:59 UTC.
        var tmpFrom = new DateTime(inDate.Year, inDate.Month, inDate.Day, 0, 0, 0, DateTimeKind.Utc);
        var tmpTo = tmpFrom.AddDays(1).AddSeconds(-1);
        // Use a synthetic ticker placeholder for whole-day misses since
        // the misses table is keyed on (ticker, range_from, range_to).
        // The convention is to record one miss per (symbol, day) using
        // the underlying ticker as the "ticker" value; consumers that
        // probe by contract ticker won't false-match because the prefix
        // differs.
        var tmpKey = $"_DAY:{inDate:yyyy-MM-dd}";
        await inConn.ExecuteAsync(
            """
            INSERT INTO historical_options_snapshots_misses
              (ticker, range_from, range_to, reason, fetched_at)
            VALUES (@Key, @From, @To, @Reason, NOW())
            ON CONFLICT (ticker, range_from, range_to) DO NOTHING
            """,
            new { Key = tmpKey, From = tmpFrom, To = tmpTo, Reason = inReason }).ConfigureAwait(false);
    }

    /// <summary>
    /// Compute the 16:00 ET wall-clock instant for <paramref name="inDate"/>
    /// converted to UTC. DST-aware via the trading calendar's resolved
    /// Eastern timezone. Used as the deterministic snapshot_date primary
    /// key value so re-running the seeder writes the same row.
    /// </summary>
    internal static DateTime ComputeRthCloseUtc(DateOnly inDate)
    {
        // Same-day half-day handling is a complication that would require
        // diverging snapshot timestamps for half-days vs full days, which
        // breaks the (ticker, snapshot_date) primary-key identity across
        // re-runs that go through different "is half day" branches. Use
        // 16:00 ET unconditionally — half-day NBBO at 13:00 → 16:00 ET
        // simply carries forward 3 hours of stale quote, which the
        // downstream freshness gate handles.
        var tmpEt = new DateTime(inDate.Year, inDate.Month, inDate.Day, 16, 0, 0,
            DateTimeKind.Unspecified);
        var tmpTz = ResolveEasternTz();
        var tmpUtc = TimeZoneInfo.ConvertTimeToUtc(tmpEt, tmpTz);
        return DateTime.SpecifyKind(tmpUtc, DateTimeKind.Utc);
    }

    private static TimeZoneInfo ResolveEasternTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
    }

    /// <summary>Internal Dapper mapping row for the chain-band SELECT.</summary>
    private sealed record RawContractRow(
        string Ticker, string ContractType, string ExpirationDateText, decimal? StrikePrice);

    /// <summary>Resolved chain row used by <see cref="ProcessDayAsync"/>.</summary>
    internal sealed record SnapshotContractRow(
        string Ticker, string ContractType, DateOnly ExpirationDate, decimal? StrikePrice);

    /// <summary>NBBO row read from <c>historical_options_quotes</c>.</summary>
    internal sealed record NbboRow(decimal BidPrice, decimal AskPrice);

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
