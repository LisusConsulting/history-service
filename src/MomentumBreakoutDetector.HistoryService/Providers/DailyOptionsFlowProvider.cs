using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Concurrency;
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

    /// <summary>
    /// PR 2 — write path. Idempotent UPSERT of aggregated flow rows keyed
    /// on <c>(underlying_ticker, trade_date)</c>. A re-run for the same
    /// (symbol, day) overwrites <see cref="DailyOptionsFlowRow.CallVolume"/>
    /// + sibling fields, leaving the row's <c>fetched_at</c> bumped to NOW().
    /// Wraps the persist step in a <see cref="GapLockExecutor{TKey}"/>
    /// advisory-lock window so two concurrent writers (e.g. seeder running
    /// against the same DB as the daily 08:00 ET cron) serialize on the
    /// same key.
    /// </summary>
    Task UpsertAsync(
        IReadOnlyList<DailyOptionsFlowRow> inRows,
        CancellationToken inCt = default);

    /// <summary>
    /// PR 2 — write path. Record a contiguous trading-day range as a miss
    /// marker via the shared <see cref="RangeMarkerWriter"/> (coalesces
    /// adjacent existing markers within 1 calendar day, same shape as the
    /// chains and macro providers). Used by the seeder + the daily cron
    /// when Polygon returns no aggregable contracts for a (symbol, day).
    /// </summary>
    Task RecordMissAsync(
        string inSymbol, DateOnly inFromDate, DateOnly inToDate, string inReason,
        CancellationToken inCt = default);
}

/// <summary>
/// Per-(symbol, trade_date) gap key used to serialize <see cref="DailyOptionsFlowProvider.UpsertAsync"/>
/// invocations across replicas. Wrapping each row's UPSERT in
/// <see cref="GapLockExecutor{TKey}.WithPersistLockAsync"/> means two
/// writers (seeder + cron, or two seeder shards) for the same
/// (TSLA, 2025-04-15) collapse onto the same advisory lock — the second
/// caller waits for the first's UPSERT to commit before re-attempting,
/// preventing the harmless-but-noisy double-write race.
/// </summary>
internal sealed record DailyOptionsFlowGapKey(string Symbol, DateOnly TradeDate);

/// <summary>
/// Postgres-backed reader + writer for <c>daily_options_flow</c>.
/// PR 2 added the write surface (<see cref="UpsertAsync"/>,
/// <see cref="RecordMissAsync"/>); read path unchanged from PR 1.
/// Concurrency-safety patterns:
/// <list type="bullet">
///   <item><see cref="GapLockExecutor{TKey}"/> wraps each per-day UPSERT
///         with a pg_advisory_xact_lock so cross-replica writers on the
///         same (symbol, day) serialize.</item>
///   <item><see cref="RangeMarkerWriter"/> handles miss-range coalescence
///         + cross-replica safety on the misses table.</item>
/// </list>
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

    // ── PR 2 — write path ──────────────────────────────────────────────

    /// <inheritdoc />
    public async Task UpsertAsync(
        IReadOnlyList<DailyOptionsFlowRow> inRows,
        CancellationToken inCt = default)
    {
        if (inRows is null || inRows.Count == 0) return;

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        var tmpInserted = 0;
        var tmpUpdated = 0;

        // Per-row advisory lock so two writers on the same
        // (underlying, trade_date) serialize their UPSERTs. The lock is
        // released on transaction commit (microseconds-to-millis) so a
        // backfill writing N independent (TSLA, 2024-01-02..03..04..) keys
        // does not block other surfaces. Same pattern as RangeMarkerWriter,
        // but here we own a lighter-weight UPSERT body rather than a
        // DELETE-then-INSERT merge.
        foreach (var tmpRow in inRows)
        {
            var tmpSeed = $"{tmpRow.UnderlyingTicker}|{tmpRow.TradeDate:yyyy-MM-dd}";
            await GapLockExecutor<DailyOptionsFlowGapKey>.WithPersistLockAsync(
                tmpConn,
                inLockNamespace: "daily_options_flow",
                inLockKeySeed: tmpSeed,
                inWork: async (conn, tx, ct) =>
                {
                    await using var tmpCmd = new NpgsqlCommand(
                        """
                        INSERT INTO daily_options_flow
                          (underlying_ticker, trade_date,
                           call_volume, put_volume, call_oi, put_oi,
                           put_call_ratio, flow_score, contract_count, fetched_at)
                        VALUES
                          (@under, @date,
                           @cv, @pv, @co, @po,
                           @ratio, @score, @cnt, NOW())
                        ON CONFLICT (underlying_ticker, trade_date) DO UPDATE SET
                          call_volume    = EXCLUDED.call_volume,
                          put_volume     = EXCLUDED.put_volume,
                          call_oi        = EXCLUDED.call_oi,
                          put_oi         = EXCLUDED.put_oi,
                          put_call_ratio = EXCLUDED.put_call_ratio,
                          flow_score     = EXCLUDED.flow_score,
                          contract_count = EXCLUDED.contract_count,
                          fetched_at     = NOW()
                        RETURNING (xmax = 0) AS inserted
                        """, conn, tx);
                    tmpCmd.Parameters.AddWithValue("under", tmpRow.UnderlyingTicker);
                    tmpCmd.Parameters.AddWithValue("date", tmpRow.TradeDate);
                    tmpCmd.Parameters.AddWithValue("cv", tmpRow.CallVolume);
                    tmpCmd.Parameters.AddWithValue("pv", tmpRow.PutVolume);
                    tmpCmd.Parameters.AddWithValue("co", tmpRow.CallOi);
                    tmpCmd.Parameters.AddWithValue("po", tmpRow.PutOi);
                    tmpCmd.Parameters.AddWithValue("ratio",
                        (object?)tmpRow.PutCallRatio ?? DBNull.Value);
                    tmpCmd.Parameters.AddWithValue("score",
                        (object?)tmpRow.FlowScore ?? DBNull.Value);
                    tmpCmd.Parameters.AddWithValue("cnt", tmpRow.ContractCount);

                    // RETURNING xmax=0 distinguishes INSERT (xmax==0) from
                    // UPDATE-on-conflict (xmax!=0) — pg-canonical pattern
                    // for "did this UPSERT actually create a new row?".
                    var tmpScalar = await tmpCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                    if (tmpScalar is bool tmpNewRow && tmpNewRow) Interlocked.Increment(ref tmpInserted);
                    else Interlocked.Increment(ref tmpUpdated);
                },
                inCt).ConfigureAwait(false);
        }

        m_Logger.LogInformation(
            "DailyOptionsFlow upsert: {Inserted} inserted, {Updated} updated, {Total} rows",
            tmpInserted, tmpUpdated, inRows.Count);
    }

    /// <inheritdoc />
    public async Task RecordMissAsync(
        string inSymbol, DateOnly inFromDate, DateOnly inToDate, string inReason,
        CancellationToken inCt = default)
    {
        if (string.IsNullOrWhiteSpace(inSymbol)) return;
        if (inFromDate > inToDate) return;

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        try
        {
            var tmpFrom = new DateTimeOffset(inFromDate.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
            var tmpTo = new DateTimeOffset(inToDate.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);

            // 1-day adjacency, mirroring chains/macro: a marker for
            // 2024-01-02 and one for 2024-01-03 collapse on-write into a
            // single 2024-01-02..03 row.
            var tmpFinalCount = await RangeMarkerWriter.WriteAsync(
                tmpConn,
                DailyOptionsFlowMissTableSpec,
                inKeyValues: new[]
                {
                    new KeyValuePair<string, object>("UnderlyingTicker", inSymbol),
                },
                inNewRanges: new[] { (tmpFrom, tmpTo) },
                inReason: inReason,
                inAdjacencyTicks: TimeSpan.FromDays(1).Ticks,
                inCt: inCt).ConfigureAwait(false);

            m_Logger.LogInformation(
                "DailyOptionsFlow miss-marker recorded: {Symbol} {From}..{To} reason={Reason} (table now has {Total} row(s) for this symbol)",
                inSymbol, inFromDate, inToDate, inReason, tmpFinalCount);
        }
        catch (Exception ex)
        {
            // Don't break the seeder/cron just because miss-marker write
            // failed (e.g. table absent on a brand-new DB before
            // migration 012 runs). Same fail-quiet behaviour as the
            // chain/macro provider.
            m_Logger.LogWarning(ex,
                "Failed to record daily-options-flow miss-marker for {Symbol} {From}..{To}",
                inSymbol, inFromDate, inToDate);
        }
    }

    /// <summary>
    /// Schema descriptor for <c>daily_options_flow_misses</c>. Same
    /// range-shape as <c>historical_options_chains_misses</c> /
    /// <c>macro_data_misses_v2</c> (DATE-typed bounds), so the shared
    /// <see cref="RangeMarkerWriter"/> can write into it unchanged.
    /// Internal so tests can pin the spec for setup.
    /// </summary>
    internal static readonly RangeMarkerTableSpec DailyOptionsFlowMissTableSpec = new(
        TableName: "daily_options_flow_misses",
        KeyColumns: new[] { "underlying_ticker" },
        RangeFromColumn: "range_from",
        RangeToColumn: "range_to",
        FetchedAtColumn: "fetched_at",
        HasReasonColumn: true,
        ReasonColumn: "reason",
        RangeColumnType: RangeMarkerColumnType.Date);
}
