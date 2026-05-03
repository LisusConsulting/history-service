using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Wave C / PR 6 of the ATM-IV full historical coverage plan
/// (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md).
/// Per-day aggregator that rolls per-contract rows from
/// <c>historical_options_snapshots</c> into a single
/// <see cref="DailyAtmIvRow"/> for one (symbol, trade_date).
///
/// <para>
/// Shared by both the daily 08:00 ET refresh cron
/// (<see cref="HostedServices.DailyAtmIvRefreshService"/>) and the seeder
/// backfill mode (<c>--surface daily_atm_iv</c> in the seeder driver) so
/// the two paths produce byte-identical rows for the same input.
/// </para>
///
/// <para>
/// <b>Algorithm</b> per the plan brief (Step 3, concern G — cross-source
/// asymmetry mitigation):
/// <list type="number">
///   <item>Read all <c>historical_options_snapshots</c> rows for
///         (<paramref name="inSymbol"/>, <paramref name="inTradeDate"/>)
///         filtered to ATM ± 5 % × non-NULL implied_volatility.</item>
///   <item><c>DISTINCT ON (ticker, date_trunc('day', snapshot_date))</c>
///         picking the row with the latest <c>snapshot_date</c> per
///         (contract, day) — i.e. the EOD reading. This collapses the
///         intraday <c>polygon_live</c> 5-min cadence (~78 rows/contract/day)
///         and the single <c>computed_bs</c> EOD row to the same shape:
///         one EOD row per contract per day. Re-aggregating across both
///         sources produces the same result regardless of which source
///         contributed.</item>
///   <item><c>AVG(implied_volatility)</c> across the EOD-per-contract
///         set + <c>COUNT(*)</c> for the contract count.</item>
/// </list>
/// </para>
///
/// <para>
/// <b>Empty-set semantics.</b> When the WHERE clause matches zero rows
/// (no snapshot rows for the day, or every match has NULL IV), the
/// aggregator returns null. Callers (cron / seeder) record a miss-marker
/// via <see cref="IDailyAtmIvProvider.RecordMissAsync"/>. Callers do NOT
/// write a NULL-IV row to the daily table for empty days — that would
/// conflate "we tried and got nothing" with "we never tried" on the read
/// path. Days where computed-BS partially succeeded (some contracts
/// converged, others didn't) AVG only over the successful subset
/// (NULL IVs excluded by the WHERE clause).
/// </para>
///
/// <para>
/// <b>Idempotent.</b> The aggregator is a pure read — it reads from
/// snapshots and returns a row, no writes. The caller wraps in an
/// UPSERT, which makes the end-to-end pipeline idempotent (UPSERT with
/// the same numbers + <c>fetched_at</c> bump).
/// </para>
/// </summary>
public interface IDailyAtmIvAggregator
{
    /// <summary>
    /// Aggregate per-contract snapshot rows for
    /// (<paramref name="inSymbol"/>, <paramref name="inTradeDate"/>) into
    /// a single daily row. Returns null when zero contracts contributed
    /// (empty snapshot set, or every snapshot has NULL IV after the
    /// strike-band + non-NULL filter).
    /// </summary>
    Task<DailyAtmIvRow?> AggregateAsync(
        string inSymbol,
        DateOnly inTradeDate,
        CancellationToken inCt = default);

    /// <summary>
    /// Aggregate a contiguous range of trading days. Yields one
    /// <see cref="DailyAtmIvRow"/> per day where the per-day aggregate
    /// was non-empty. Days with zero contributing contracts are dropped
    /// from the result — the seeder driver records miss-markers for them
    /// out-of-band so a re-run is a no-op.
    /// </summary>
    Task<IReadOnlyList<DailyAtmIvRow>> AggregateRangeAsync(
        string inSymbol,
        DateOnly inFrom,
        DateOnly inTo,
        CancellationToken inCt = default);
}

/// <summary>
/// Postgres-backed implementation of <see cref="IDailyAtmIvAggregator"/>.
/// One DB connection per call; the per-day query is cheap (one scan
/// over the day's snapshot chunks via TimescaleDB chunk pruning) so
/// pooling is unnecessary for the cron / seeder cadence.
/// </summary>
public sealed class DailyAtmIvAggregator : IDailyAtmIvAggregator
{
    /// <summary>
    /// ATM strike band as a fraction of underlying. Matches the
    /// live-capture cron and the BS-compute seeder so the read aggregate
    /// covers the same universe forward + backward.
    /// </summary>
    public const decimal AtmBandPct = 0.05m;

    private readonly string m_ConnectionString;
    private readonly ILogger<DailyAtmIvAggregator> m_Logger;

    public DailyAtmIvAggregator(
        IOptions<HistoryServiceOptions> inOptions,
        ILogger<DailyAtmIvAggregator> inLogger)
        : this(inOptions.Value.ConnectionString, inLogger)
    {
    }

    /// <summary>Test-friendly ctor that takes the connection string directly.</summary>
    public DailyAtmIvAggregator(string inConnectionString, ILogger<DailyAtmIvAggregator> inLogger)
    {
        m_ConnectionString = inConnectionString;
        m_Logger = inLogger;
    }

    /// <inheritdoc />
    public async Task<DailyAtmIvRow?> AggregateAsync(
        string inSymbol, DateOnly inTradeDate, CancellationToken inCt = default)
    {
        if (string.IsNullOrWhiteSpace(inSymbol)) return null;

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        var tmpResult = await tmpConn.QuerySingleOrDefaultAsync<AggregateRow>(
            new CommandDefinition(
                AggregateOneSql,
                new
                {
                    Symbol = inSymbol,
                    Date = inTradeDate.ToString("yyyy-MM-dd"),
                    Band = AtmBandPct,
                },
                cancellationToken: inCt)).ConfigureAwait(false);

        if (tmpResult is null || tmpResult.ContractCount == 0)
        {
            m_Logger.LogDebug(
                "DailyAtmIv aggregate {Symbol} {Date}: zero rows after filter",
                inSymbol, inTradeDate);
            return null;
        }

        return new DailyAtmIvRow(
            UnderlyingTicker: inSymbol,
            TradeDate: inTradeDate,
            AtmIv: tmpResult.AtmIv,
            ContractCount: tmpResult.ContractCount);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<DailyAtmIvRow>> AggregateRangeAsync(
        string inSymbol, DateOnly inFrom, DateOnly inTo, CancellationToken inCt = default)
    {
        if (string.IsNullOrWhiteSpace(inSymbol)) return Array.Empty<DailyAtmIvRow>();
        if (inFrom > inTo) return Array.Empty<DailyAtmIvRow>();

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        var tmpRaw = await tmpConn.QueryAsync<RangeRow>(
            new CommandDefinition(
                AggregateRangeSql,
                new
                {
                    Symbol = inSymbol,
                    From = inFrom.ToString("yyyy-MM-dd"),
                    To = inTo.ToString("yyyy-MM-dd"),
                    Band = AtmBandPct,
                },
                cancellationToken: inCt)).ConfigureAwait(false);

        var tmpResult = new List<DailyAtmIvRow>();
        foreach (var tmpRow in tmpRaw)
        {
            if (tmpRow.ContractCount == 0) continue;
            tmpResult.Add(new DailyAtmIvRow(
                UnderlyingTicker: inSymbol,
                TradeDate: DateOnly.Parse(tmpRow.TradeDateText),
                AtmIv: tmpRow.AtmIv,
                ContractCount: tmpRow.ContractCount));
        }

        m_Logger.LogInformation(
            "DailyAtmIv range aggregate {Symbol} {From}..{To}: produced {Count} non-empty day(s)",
            inSymbol, inFrom, inTo, tmpResult.Count);

        return tmpResult;
    }

    /// <summary>
    /// Per-day aggregation. Filter logic:
    ///   strike within ATM ± @Band of underlying_price (skips rows where
    ///   underlying_price is NULL or 0), implied_volatility IS NOT NULL,
    ///   and the (ticker, day) row is the EOD reading via DISTINCT ON.
    ///   The outer SELECT averages IV + counts contributing contracts.
    /// </summary>
    internal const string AggregateOneSql = """
        WITH eod_per_contract AS (
            SELECT DISTINCT ON (ticker, date_trunc('day', snapshot_date))
                ticker, snapshot_date, implied_volatility, strike_price, underlying_price
            FROM historical_options_snapshots
            WHERE underlying_ticker = @Symbol
              AND snapshot_date >= @Date::date::timestamptz
              AND snapshot_date <  (@Date::date + 1)::timestamptz
              AND implied_volatility IS NOT NULL
              AND strike_price IS NOT NULL
              AND underlying_price IS NOT NULL
              AND underlying_price > 0
              AND ABS((strike_price - underlying_price) / underlying_price) <= @Band
            ORDER BY ticker, date_trunc('day', snapshot_date), snapshot_date DESC
        )
        SELECT
            AVG(implied_volatility) AS "AtmIv",
            COUNT(*)::int AS "ContractCount"
        FROM eod_per_contract
        """;

    /// <summary>
    /// Range aggregation (one row per non-empty trading day in [from, to]).
    /// Same EOD-per-contract DISTINCT-ON logic as the per-day query, just
    /// grouped by date. Empty days drop out of the result via the
    /// HAVING clause.
    /// </summary>
    internal const string AggregateRangeSql = """
        WITH eod_per_contract AS (
            SELECT DISTINCT ON (ticker, date_trunc('day', snapshot_date))
                ticker,
                snapshot_date::date AS trade_date,
                implied_volatility, strike_price, underlying_price
            FROM historical_options_snapshots
            WHERE underlying_ticker = @Symbol
              AND snapshot_date >= @From::date::timestamptz
              AND snapshot_date <  (@To::date + 1)::timestamptz
              AND implied_volatility IS NOT NULL
              AND strike_price IS NOT NULL
              AND underlying_price IS NOT NULL
              AND underlying_price > 0
              AND ABS((strike_price - underlying_price) / underlying_price) <= @Band
            ORDER BY ticker, date_trunc('day', snapshot_date), snapshot_date DESC
        )
        SELECT
            trade_date::text AS "TradeDateText",
            AVG(implied_volatility) AS "AtmIv",
            COUNT(*)::int AS "ContractCount"
        FROM eod_per_contract
        GROUP BY trade_date
        HAVING COUNT(*) > 0
        ORDER BY trade_date
        """;

    private sealed record AggregateRow(decimal? AtmIv, int ContractCount);
    private sealed record RangeRow(string TradeDateText, decimal? AtmIv, int ContractCount);
}
