using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// One row of <c>daily_atm_iv</c>. Mirrors the DB schema verbatim; the
/// gRPC layer maps to <see cref="Contracts.V1.DailyAtmIvRow"/> at the edge.
/// </summary>
/// <remarks>
/// <para>
/// Both <see cref="AtmIv"/> and <see cref="ContractCount"/> are nullable
/// to reflect the schema (migration 014). The aggregator (Wave C / PR 6
/// cron + seeder) writes NULL on days where every contributing snapshot
/// has NULL IV (e.g. solver failed across the entire band). The gRPC
/// wire payload carries explicit <c>_is_null</c> flags so consumers
/// don't conflate 0.0 with NULL.
/// </para>
/// </remarks>
public sealed record DailyAtmIvRow(
    string UnderlyingTicker,
    DateOnly TradeDate,
    decimal? AtmIv,
    int? ContractCount);

/// <summary>
/// Read-only abstraction over <c>daily_atm_iv</c>, surfaced via
/// <see cref="HistoryServiceImpl.GetDailyAtmIv"/> in Wave B / PR 5 of
/// the ATM-IV full historical coverage plan
/// (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md).
/// Identical shape to <see cref="IDailyOptionsFlowProvider"/> — the only
/// significant difference is the Source-of-Truth: rows here are the
/// aggregate of <c>historical_options_snapshots</c> (a hybrid live +
/// computed-BS table) rather than per-day Polygon /v2/aggs sums.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why no on-demand fetch.</b> Unlike bars / chains / NBBO / macro,
/// this surface has no point-in-time upstream. Aggregating the per-day
/// row requires fanning out across the entire ATM±5% × 0-60 DTE
/// snapshot universe, which is a cron-shaped operation, not a
/// request-shaped one. <see cref="GetRangeAsync"/> reads cached rows
/// only; missing days return as gaps in the response with no upstream
/// round-trip. The Wave C / PR 6 daily cron + seeder backfill populate
/// the table.
/// </para>
///
/// <para>
/// <b>Past-only guard.</b> Enforced at the gRPC layer in
/// <see cref="HistoryServiceImpl.GetDailyAtmIv"/> via
/// <see cref="Validation.PastOnlyRangeValidator"/> — same pattern as the
/// other 4 surfaces. The provider itself does not duplicate the check.
/// </para>
/// </remarks>
public interface IDailyAtmIvProvider
{
    /// <summary>
    /// Read all rows for <paramref name="inSymbol"/> with
    /// <c>trade_date BETWEEN inFrom AND inTo</c> (inclusive). Returns
    /// rows sorted ascending by <c>trade_date</c>. Empty list if no rows
    /// match — there is NO on-miss upstream fetch.
    /// </summary>
    Task<IReadOnlyList<DailyAtmIvRow>> GetRangeAsync(
        string inSymbol, DateOnly inFrom, DateOnly inTo,
        CancellationToken inCt = default);

    /// <summary>
    /// Wave C / PR 6 — write path. Idempotent UPSERT keyed on
    /// <c>(underlying_ticker, trade_date)</c>. A re-run for the same
    /// (symbol, day) overwrites the aggregate values and bumps
    /// <see cref="DailyAtmIvRow"/>'s persisted <c>fetched_at</c> to NOW().
    /// Wraps the persist step in a <see cref="GapLockExecutor{TKey}"/>
    /// advisory-lock window so two concurrent writers (e.g. seeder
    /// running against the same DB as the daily 08:00 ET cron) serialize
    /// on the same key. Same write-path pattern as
    /// <see cref="DailyOptionsFlowProvider.UpsertAsync"/>.
    /// </summary>
    Task UpsertAsync(
        IReadOnlyList<DailyAtmIvRow> inRows,
        CancellationToken inCt = default);

    /// <summary>
    /// Wave C / PR 6 — write path. Record a contiguous trading-day range
    /// as a miss marker via the shared
    /// <see cref="RangeMarkerWriter"/>. Used by the cron when an
    /// aggregation finds zero valid snapshots for a (symbol, day).
    /// </summary>
    Task RecordMissAsync(
        string inSymbol, DateOnly inFromDate, DateOnly inToDate, string inReason,
        CancellationToken inCt = default);
}

/// <summary>
/// Per-(symbol, trade_date) gap key used to serialize
/// <see cref="DailyAtmIvProvider.UpsertAsync"/> across replicas.
/// Same shape as <see cref="DailyOptionsFlowGapKey"/>.
/// </summary>
internal sealed record DailyAtmIvGapKey(string Symbol, DateOnly TradeDate);

/// <summary>
/// Postgres-backed reader + writer for <c>daily_atm_iv</c>. PR 5 ships
/// the read path; the write path (UpsertAsync, RecordMissAsync) is wired
/// here so the Wave C cron + seeder land cleanly without re-touching
/// this provider.
/// </summary>
public sealed class DailyAtmIvProvider : IDailyAtmIvProvider
{
    private readonly string m_ConnectionString;
    private readonly ILogger<DailyAtmIvProvider> m_Logger;

    public DailyAtmIvProvider(
        IOptions<HistoryServiceOptions> inOptions,
        ILogger<DailyAtmIvProvider> inLogger)
        : this(inOptions.Value.ConnectionString, inLogger)
    {
    }

    /// <summary>Test-friendly ctor that takes the connection string directly.</summary>
    public DailyAtmIvProvider(
        string inConnectionString,
        ILogger<DailyAtmIvProvider> inLogger)
    {
        m_ConnectionString = inConnectionString;
        m_Logger = inLogger;
    }

    public async Task<IReadOnlyList<DailyAtmIvRow>> GetRangeAsync(
        string inSymbol, DateOnly inFrom, DateOnly inTo,
        CancellationToken inCt = default)
    {
        if (string.IsNullOrWhiteSpace(inSymbol)) return Array.Empty<DailyAtmIvRow>();
        if (inFrom > inTo) return Array.Empty<DailyAtmIvRow>();

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        // Cast trade_date to text + parse client-side to avoid Dapper's
        // DATE → DateTime default mapping. Same pattern as
        // DailyOptionsFlowProvider.GetRangeAsync.
        var tmpRaw = await tmpConn.QueryAsync<RawRow>(
            """
            SELECT
              underlying_ticker AS "UnderlyingTicker",
              trade_date::text  AS "TradeDateText",
              atm_iv            AS "AtmIv",
              contract_count    AS "ContractCount"
            FROM daily_atm_iv
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
            .Select(r => new DailyAtmIvRow(
                UnderlyingTicker: r.UnderlyingTicker,
                TradeDate: DateOnly.Parse(r.TradeDateText),
                AtmIv: r.AtmIv,
                ContractCount: r.ContractCount))
            .ToList();

        m_Logger.LogInformation(
            "DailyAtmIv read {Count} row(s) for {Symbol} {From}..{To}",
            tmpResult.Count, inSymbol, inFrom, inTo);

        return tmpResult;
    }

    /// <inheritdoc />
    public async Task UpsertAsync(
        IReadOnlyList<DailyAtmIvRow> inRows, CancellationToken inCt = default)
    {
        if (inRows is null || inRows.Count == 0) return;

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        var tmpInserted = 0;
        var tmpUpdated = 0;

        foreach (var tmpRow in inRows)
        {
            var tmpSeed = $"{tmpRow.UnderlyingTicker}|{tmpRow.TradeDate:yyyy-MM-dd}";
            await GapLockExecutor<DailyAtmIvGapKey>.WithPersistLockAsync(
                tmpConn,
                inLockNamespace: "daily_atm_iv",
                inLockKeySeed: tmpSeed,
                inWork: async (conn, tx, ct) =>
                {
                    await using var tmpCmd = new NpgsqlCommand(
                        """
                        INSERT INTO daily_atm_iv
                          (underlying_ticker, trade_date, atm_iv, contract_count, fetched_at)
                        VALUES (@under, @date, @iv, @cnt, NOW())
                        ON CONFLICT (underlying_ticker, trade_date) DO UPDATE SET
                          atm_iv         = EXCLUDED.atm_iv,
                          contract_count = EXCLUDED.contract_count,
                          fetched_at     = NOW()
                        RETURNING (xmax = 0) AS inserted
                        """, conn, tx);
                    tmpCmd.Parameters.AddWithValue("under", tmpRow.UnderlyingTicker);
                    tmpCmd.Parameters.AddWithValue("date", tmpRow.TradeDate);
                    tmpCmd.Parameters.AddWithValue("iv", (object?)tmpRow.AtmIv ?? DBNull.Value);
                    tmpCmd.Parameters.AddWithValue("cnt", (object?)tmpRow.ContractCount ?? DBNull.Value);

                    var tmpScalar = await tmpCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                    if (tmpScalar is bool tmpNewRow && tmpNewRow) Interlocked.Increment(ref tmpInserted);
                    else Interlocked.Increment(ref tmpUpdated);
                },
                inCt).ConfigureAwait(false);
        }

        m_Logger.LogInformation(
            "DailyAtmIv upsert: {Inserted} inserted, {Updated} updated, {Total} rows",
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

            var tmpFinalCount = await RangeMarkerWriter.WriteAsync(
                tmpConn,
                DailyAtmIvMissTableSpec,
                inKeyValues: new[]
                {
                    new KeyValuePair<string, object>("UnderlyingTicker", inSymbol),
                },
                inNewRanges: new[] { (tmpFrom, tmpTo) },
                inReason: inReason,
                inAdjacencyTicks: TimeSpan.FromDays(1).Ticks,
                inCt: inCt).ConfigureAwait(false);

            m_Logger.LogInformation(
                "DailyAtmIv miss-marker recorded: {Symbol} {From}..{To} reason={Reason} (table now has {Total} row(s) for this symbol)",
                inSymbol, inFromDate, inToDate, inReason, tmpFinalCount);
        }
        catch (Exception ex)
        {
            // Fail-quiet: same shape as DailyOptionsFlowProvider.RecordMissAsync.
            m_Logger.LogWarning(ex,
                "Failed to record daily-atm-iv miss-marker for {Symbol} {From}..{To}",
                inSymbol, inFromDate, inToDate);
        }
    }

    /// <summary>
    /// Internal Dapper mapping row. <see cref="TradeDateText"/> is the
    /// text-cast result of <c>trade_date::text</c>; we parse to DateOnly
    /// in the projection.
    /// </summary>
    private sealed record RawRow(
        string UnderlyingTicker,
        string TradeDateText,
        decimal? AtmIv,
        int? ContractCount);

    /// <summary>
    /// Schema descriptor for <c>daily_atm_iv_misses</c>. Identical shape
    /// to <see cref="DailyOptionsFlowProvider.DailyOptionsFlowMissTableSpec"/>
    /// — DATE-typed bounds, 1-day adjacency, single (underlying_ticker)
    /// key column.
    /// </summary>
    internal static readonly RangeMarkerTableSpec DailyAtmIvMissTableSpec = new(
        TableName: "daily_atm_iv_misses",
        KeyColumns: new[] { "underlying_ticker" },
        RangeFromColumn: "range_from",
        RangeToColumn: "range_to",
        FetchedAtColumn: "fetched_at",
        HasReasonColumn: true,
        ReasonColumn: "reason",
        RangeColumnType: RangeMarkerColumnType.Date);
}
