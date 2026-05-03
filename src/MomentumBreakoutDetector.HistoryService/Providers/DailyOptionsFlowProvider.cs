using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// One row of <c>daily_options_flow</c>. Mirrors the DB schema verbatim;
/// the gRPC layer maps to <see cref="Contracts.V1.DailyOptionsFlowRow"/>
/// at the edge.
/// </summary>
/// <remarks>
/// <para>
/// Nullable <see cref="PutCallRatio"/> / <see cref="FlowScore"/> reflect
/// the SQL schema: when <c>call_side &lt;= 0</c> the formula is undefined
/// and the row is written with NULL on those two columns (see
/// <c>tools/migrations/012-daily-options-flow.sql</c>). Consumers must
/// NULL-check; the gRPC contract carries an explicit <c>_is_null</c> flag
/// so the wire payload doesn't conflate 0.0 with NULL.
/// </para>
/// </remarks>
public sealed record DailyOptionsFlowRow(
    string UnderlyingTicker,
    DateOnly TradeDate,
    long CallVolume,
    long PutVolume,
    long CallOi,
    long PutOi,
    decimal? PutCallRatio,
    decimal? FlowScore,
    int ContractCount);

/// <summary>
/// Read-only abstraction over <c>daily_options_flow</c>. PR 1 surfaces
/// the read path only — the write path (UPSERT) lands in PR 2 (backfill
/// seeder mode) and is invoked from the daily 08:00 ET cron in PR 3.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why no on-demand fetch.</b> Unlike the bars / chains / NBBO / macro
/// providers, this surface has no point-in-time upstream — Polygon's
/// /v2/aggs returns per-contract daily volume, and aggregating it into
/// the (underlying, day) flow score requires fanning out across the entire
/// short-DTE chain for that day. That's a backfill-shaped operation, not
/// a request-shaped one. So <see cref="GetRangeAsync"/> reads cached rows
/// only; missing days return as gaps in the response with no upstream
/// round-trip. Consumers (backtest engine) call this AFTER the seeder /
/// daily cron has populated the window.
/// </para>
/// </remarks>
public interface IDailyOptionsFlowProvider
{
    /// <summary>
    /// Read all rows for <paramref name="inSymbol"/> with
    /// <c>trade_date BETWEEN inFrom AND inTo</c> (inclusive). Returns rows
    /// sorted ascending by <c>trade_date</c>. Empty list if no rows match
    /// — there is NO on-miss upstream fetch (see class remarks).
    /// </summary>
    Task<IReadOnlyList<DailyOptionsFlowRow>> GetRangeAsync(
        string inSymbol, DateOnly inFrom, DateOnly inTo,
        CancellationToken inCt = default);
}

/// <summary>
/// Postgres-backed reader for <c>daily_options_flow</c>. Read-only at PR 1.
/// Concurrency-safety patterns (GapLockExecutor, RangeMarkerWriter) are
/// reserved for PR 2 (write path) — the read path is a single SELECT and
/// needs no advisory lock.
/// </summary>
public sealed class DailyOptionsFlowProvider : IDailyOptionsFlowProvider
{
    private readonly string m_ConnectionString;
    private readonly ILogger<DailyOptionsFlowProvider> m_Logger;

    public DailyOptionsFlowProvider(
        IOptions<HistoryServiceOptions> inOptions,
        ILogger<DailyOptionsFlowProvider> inLogger)
        : this(inOptions.Value.ConnectionString, inLogger)
    {
    }

    /// <summary>Test-friendly ctor that takes the connection string directly.</summary>
    public DailyOptionsFlowProvider(
        string inConnectionString,
        ILogger<DailyOptionsFlowProvider> inLogger)
    {
        m_ConnectionString = inConnectionString;
        m_Logger = inLogger;
    }

    public async Task<IReadOnlyList<DailyOptionsFlowRow>> GetRangeAsync(
        string inSymbol, DateOnly inFrom, DateOnly inTo,
        CancellationToken inCt = default)
    {
        if (string.IsNullOrWhiteSpace(inSymbol))
        {
            return Array.Empty<DailyOptionsFlowRow>();
        }
        if (inFrom > inTo)
        {
            return Array.Empty<DailyOptionsFlowRow>();
        }

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        // Cast date columns to text + parse client-side to avoid Dapper's
        // DATE → DateTime default mapping (the package version pinned in
        // this repo binds DATE → DateTime). Same pattern OptionChainProvider
        // uses for the marker-shadow read in EnsureRangeCachedAsync.
        var tmpRaw = await tmpConn.QueryAsync<RawRow>(
            """
            SELECT
              underlying_ticker  AS "UnderlyingTicker",
              trade_date::text   AS "TradeDateText",
              call_volume        AS "CallVolume",
              put_volume         AS "PutVolume",
              call_oi            AS "CallOi",
              put_oi             AS "PutOi",
              put_call_ratio     AS "PutCallRatio",
              flow_score         AS "FlowScore",
              contract_count     AS "ContractCount"
            FROM daily_options_flow
            WHERE underlying_ticker = @Symbol
              AND trade_date >= @From::date
              AND trade_date <= @To::date
            ORDER BY trade_date
            """,
            new
            {
                Symbol = inSymbol,
                From = inFrom.ToString("yyyy-MM-dd"),
                To = inTo.ToString("yyyy-MM-dd"),
            }).ConfigureAwait(false);

        var tmpResult = tmpRaw
            .Select(r => new DailyOptionsFlowRow(
                UnderlyingTicker: r.UnderlyingTicker,
                TradeDate: DateOnly.Parse(r.TradeDateText),
                CallVolume: r.CallVolume,
                PutVolume: r.PutVolume,
                CallOi: r.CallOi,
                PutOi: r.PutOi,
                PutCallRatio: r.PutCallRatio,
                FlowScore: r.FlowScore,
                ContractCount: r.ContractCount))
            .ToList();

        m_Logger.LogInformation(
            "DailyOptionsFlow read {Count} row(s) for {Symbol} {From}..{To}",
            tmpResult.Count, inSymbol, inFrom, inTo);

        return tmpResult;
    }

    /// <summary>
    /// Internal Dapper mapping row. <see cref="TradeDateText"/> is the
    /// text-cast result of <c>trade_date::text</c>; we parse to DateOnly
    /// in the projection.
    /// </summary>
    private sealed record RawRow(
        string UnderlyingTicker,
        string TradeDateText,
        long CallVolume,
        long PutVolume,
        long CallOi,
        long PutOi,
        decimal? PutCallRatio,
        decimal? FlowScore,
        int ContractCount);
}
