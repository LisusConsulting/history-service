using System.Globalization;
using System.Text;
using Dapper;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Generic helper that coalesces newly-discovered miss ranges with any
/// adjacent / overlapping existing miss markers and persists the merged
/// set as a single row per contiguous range. Shared across all 4
/// providers (bars, NBBO, chains, macro) so the coalesce-on-write logic
/// has one home.
/// </summary>
/// <remarks>
/// <para>
/// Why a shared helper: every range-shaped miss-marker table has the same
/// "merge with existing markers that overlap or abut the new range" need.
/// Without it, repeated <c>EnsureRangeCachedAsync</c> calls fragment the
/// marker table — each run writes its own miss row, and the table grows
/// unboundedly. With the writer, two adjacent markers (e.g. 09:30..10:00
/// and 10:01..10:30 from two separate runs) collapse on the second
/// run's write into one 09:30..10:30 row.
/// </para>
/// <para>
/// Per-table column shape varies (bars: symbol+timeframe;
/// NBBO: ticker; chains/macro: symbol/series_id with date-typed range),
/// so the writer is parameterised by a <see cref="RangeMarkerTableSpec"/>
/// rather than hard-coding any one schema. The bars provider uses
/// <see cref="WriteAsync(NpgsqlConnection, RangeMarkerTableSpec, IReadOnlyList{KeyValuePair{string, object}}, IReadOnlyList{ValueTuple{DateTimeOffset, DateTimeOffset}}, string, CancellationToken)"/>
/// directly; date-typed providers (chains/macro) wrap their DateOnly →
/// DateTimeOffset conversion at the edge.
/// </para>
/// </remarks>
public static class RangeMarkerWriter
{
    /// <summary>
    /// Persist <paramref name="inNewRanges"/> into
    /// <paramref name="inSpec"/>.<see cref="RangeMarkerTableSpec.TableName"/>
    /// after merging with any existing rows for the same key whose
    /// [range_from, range_to] overlaps or abuts (within <paramref name="inAdjacencyTicks"/>
    /// of) any new range. The merge runs as DELETE-then-INSERT inside a
    /// single Npgsql transaction so concurrent writers don't see partial
    /// state.
    /// </summary>
    /// <param name="inConn">Open connection. Caller owns lifecycle.</param>
    /// <param name="inSpec">Schema descriptor (table, key columns, range columns).</param>
    /// <param name="inKeyValues">
    /// Composite key values matching <paramref name="inSpec"/>.<see cref="RangeMarkerTableSpec.KeyColumns"/>
    /// in declaration order (e.g. [("Symbol","TSLA"), ("Timeframe","1min")]
    /// for bars; [("Ticker","O:TSLA260417C500")] for NBBO).
    /// </param>
    /// <param name="inNewRanges">
    /// Newly-discovered missing ranges (inclusive [from, to]). Will be
    /// merged with each other AND with any existing markers for the same
    /// key. Pass empty for a no-op.
    /// </param>
    /// <param name="inReason">Free-text reason persisted on every merged row.</param>
    /// <param name="inAdjacencyTicks">
    /// Two ranges A and B are considered abutting (and thus mergeable) if
    /// <c>B.From - A.To &lt;= inAdjacencyTicks</c>. Default: 1 tick (i.e.
    /// touch-merge only). Bars use the timeframe-step
    /// (TimeSpan.FromMinutes(1).Ticks for 1-min) so a marker for 09:30..09:59
    /// and a new marker starting at 10:00 collapse into 09:30..10:00.
    /// </param>
    /// <returns>Number of rows the table contains for this key after the write.</returns>
    public static async Task<int> WriteAsync(
        NpgsqlConnection inConn,
        RangeMarkerTableSpec inSpec,
        IReadOnlyList<KeyValuePair<string, object>> inKeyValues,
        IReadOnlyList<(DateTimeOffset From, DateTimeOffset To)> inNewRanges,
        string inReason,
        long inAdjacencyTicks,
        CancellationToken inCt)
    {
        if (inNewRanges.Count == 0) return 0;
        if (inKeyValues.Count != inSpec.KeyColumns.Count)
            throw new ArgumentException(
                $"Spec declares {inSpec.KeyColumns.Count} key columns; got {inKeyValues.Count} values.",
                nameof(inKeyValues));

        var tmpKeyParams = new DynamicParameters();
        var tmpWhereClauses = new List<string>();
        for (var i = 0; i < inSpec.KeyColumns.Count; i++)
        {
            var tmpCol = inSpec.KeyColumns[i];
            var tmpName = inKeyValues[i].Key;
            tmpKeyParams.Add(tmpName, inKeyValues[i].Value);
            tmpWhereClauses.Add($"{tmpCol} = @{tmpName}");
        }
        var tmpKeyWhere = string.Join(" AND ", tmpWhereClauses);

        // Read existing markers for this key. We pull every row regardless
        // of overlap with inNewRanges — the merge logic below decides what
        // is mergeable. For tables with very large marker counts per key
        // this could be optimised to a windowed read, but per-key marker
        // counts are bounded by the seed-window size (worst case: O(days)
        // per surface) which is well within an in-memory scan.
        //
        // Cast columns to TIMESTAMPTZ before binding so the helper works
        // uniformly across TIMESTAMPTZ-typed marker tables (bars / NBBO)
        // and DATE-typed ones (chains / macro post PR #21 / #22). Without
        // the cast, Dapper's binder fights Npgsql's DATE → DateOnly
        // mapping when the destination type is DateTimeOffset.
        var tmpExistingRows = (await inConn.QueryAsync<(DateTimeOffset From, DateTimeOffset To)>(
            $"""
            SELECT {inSpec.RangeFromColumn}::timestamptz AS "From",
                   {inSpec.RangeToColumn}::timestamptz   AS "To"
            FROM {inSpec.TableName}
            WHERE {tmpKeyWhere}
            """, tmpKeyParams).ConfigureAwait(false)).ToList();

        // Merge: union of existing + new, sorted by From, sweep & coalesce
        // any two ranges where the gap between them is <= adjacencyTicks.
        var tmpAll = new List<(DateTimeOffset From, DateTimeOffset To)>(
            tmpExistingRows.Count + inNewRanges.Count);
        tmpAll.AddRange(tmpExistingRows);
        tmpAll.AddRange(inNewRanges);
        var tmpMerged = Coalesce(tmpAll, inAdjacencyTicks);

        // DELETE-then-INSERT under a transaction so a concurrent reader
        // sees either the old or the new marker set, not a half-written
        // intermediate.
        //
        // Concurrency hazard (fixed 2026-05-02): two writers racing on the
        // same key would both read the existing-row set, both DELETE 0
        // rows (or the same N rows), and both INSERT — second writer hits
        // 23505 unique-violation on the table's PK
        // (e.g. macro_data_misses_v2_pkey). Default READ COMMITTED
        // isolation does not prevent this because each writer's snapshot
        // sees no rows the other has written until that other commits.
        //
        // Fix: serialize writers competing for the same composite key via
        // pg_advisory_xact_lock(table_name_hash, key_hash). The lock is
        // held until the transaction ends (commit or rollback) and only
        // collides with other writers on the same (table, key) pair —
        // writers for different keys (e.g. T10Y2Y vs CPIAUCSL) proceed
        // in parallel as before. Single round-trip; no extra schema.
        await using var tmpTx = await inConn.BeginTransactionAsync(inCt).ConfigureAwait(false);
        try
        {
            var tmpTableHash = StableHashInt32(inSpec.TableName);
            var tmpKeyHash = StableHashInt32(BuildKeyHashSeed(inKeyValues));
            await inConn.ExecuteAsync(
                "SELECT pg_advisory_xact_lock(@TableHash, @KeyHash)",
                new { TableHash = tmpTableHash, KeyHash = tmpKeyHash },
                tmpTx).ConfigureAwait(false);

            // Re-read existing rows under the lock. The earlier read
            // (above, outside the lock) was speculative — useful so we
            // don't take the lock when there is nothing to write — but
            // a concurrent writer may have committed between that read
            // and acquiring the lock. Re-read so the merge reflects the
            // final post-lock state and the DELETE-then-INSERT is
            // serialised correctly.
            var tmpExistingUnderLock = (await inConn.QueryAsync<(DateTimeOffset From, DateTimeOffset To)>(
                $"""
                SELECT {inSpec.RangeFromColumn}::timestamptz AS "From",
                       {inSpec.RangeToColumn}::timestamptz   AS "To"
                FROM {inSpec.TableName}
                WHERE {tmpKeyWhere}
                """, tmpKeyParams, tmpTx).ConfigureAwait(false)).ToList();
            tmpAll.Clear();
            tmpAll.AddRange(tmpExistingUnderLock);
            tmpAll.AddRange(inNewRanges);
            tmpMerged = Coalesce(tmpAll, inAdjacencyTicks);

            await inConn.ExecuteAsync(
                $"DELETE FROM {inSpec.TableName} WHERE {tmpKeyWhere}",
                tmpKeyParams, tmpTx).ConfigureAwait(false);

            foreach (var tmpRange in tmpMerged)
            {
                var tmpInsertParams = new DynamicParameters(tmpKeyParams);
                tmpInsertParams.Add("__From", tmpRange.From);
                tmpInsertParams.Add("__To", tmpRange.To);
                tmpInsertParams.Add("__Reason", inReason);

                var tmpKeyCols = string.Join(", ", inSpec.KeyColumns);
                var tmpKeyVals = string.Join(", ",
                    inKeyValues.Select(kv => "@" + kv.Key));
                var tmpExtraCols = inSpec.HasReasonColumn
                    ? $", {inSpec.ReasonColumn}, {inSpec.FetchedAtColumn}"
                    : $", {inSpec.FetchedAtColumn}";
                var tmpExtraVals = inSpec.HasReasonColumn
                    ? ", @__Reason, NOW()"
                    : ", NOW()";

                // Cast each parameter to the column's storage type so the
                // helper writes uniformly into TIMESTAMPTZ-typed tables
                // (bars / NBBO) and DATE-typed ones (chains / macro). The
                // input is always a DateTimeOffset (UTC midnight for date-
                // typed callers); postgres handles TIMESTAMPTZ → DATE
                // truncation via the explicit cast.
                var tmpRangeCast = inSpec.RangeColumnType switch
                {
                    RangeMarkerColumnType.Date => "::date",
                    _ => "::timestamptz",
                };

                await inConn.ExecuteAsync(
                    $"""
                    INSERT INTO {inSpec.TableName}
                      ({tmpKeyCols}, {inSpec.RangeFromColumn}, {inSpec.RangeToColumn}{tmpExtraCols})
                    VALUES
                      ({tmpKeyVals}, @__From{tmpRangeCast}, @__To{tmpRangeCast}{tmpExtraVals})
                    """, tmpInsertParams, tmpTx).ConfigureAwait(false);
            }

            await tmpTx.CommitAsync(inCt).ConfigureAwait(false);
        }
        catch
        {
            await tmpTx.RollbackAsync(inCt).ConfigureAwait(false);
            throw;
        }

        return tmpMerged.Count;
    }

    /// <summary>
    /// Coalesce a list of ranges into the minimal set of non-overlapping,
    /// non-abutting (within <paramref name="inAdjacencyTicks"/>) ranges.
    /// Pure function — exposed so providers can pre-merge in-memory before
    /// hitting the DB write path.
    /// </summary>
    public static List<(DateTimeOffset From, DateTimeOffset To)> Coalesce(
        IEnumerable<(DateTimeOffset From, DateTimeOffset To)> inRanges,
        long inAdjacencyTicks)
    {
        var tmpSorted = inRanges
            .Where(r => r.From <= r.To)
            .OrderBy(r => r.From)
            .ToList();
        if (tmpSorted.Count == 0) return tmpSorted;

        var tmpResult = new List<(DateTimeOffset From, DateTimeOffset To)>();
        var (tmpCurFrom, tmpCurTo) = tmpSorted[0];
        for (var i = 1; i < tmpSorted.Count; i++)
        {
            var (tmpNextFrom, tmpNextTo) = tmpSorted[i];
            // Mergeable iff overlap OR gap <= adjacency.
            var tmpGapTicks = (tmpNextFrom - tmpCurTo).Ticks;
            if (tmpGapTicks <= inAdjacencyTicks)
            {
                if (tmpNextTo > tmpCurTo) tmpCurTo = tmpNextTo;
            }
            else
            {
                tmpResult.Add((tmpCurFrom, tmpCurTo));
                tmpCurFrom = tmpNextFrom;
                tmpCurTo = tmpNextTo;
            }
        }
        tmpResult.Add((tmpCurFrom, tmpCurTo));
        return tmpResult;
    }

    /// <summary>
    /// Build a stable string seed from a composite-key value list for
    /// hashing into a pg advisory-lock key. Format:
    /// <c>name1=value1|name2=value2|…</c> with invariant-culture
    /// formatting on each value so culture-sensitive callers (e.g. a
    /// numeric key in a non-US locale) hash identically across hosts.
    /// </summary>
    private static string BuildKeyHashSeed(
        IReadOnlyList<KeyValuePair<string, object>> inKeyValues)
    {
        var tmpSb = new StringBuilder(inKeyValues.Count * 24);
        for (var i = 0; i < inKeyValues.Count; i++)
        {
            if (i > 0) tmpSb.Append('|');
            tmpSb.Append(inKeyValues[i].Key);
            tmpSb.Append('=');
            tmpSb.Append(Convert.ToString(inKeyValues[i].Value, CultureInfo.InvariantCulture));
        }
        return tmpSb.ToString();
    }

    /// <summary>
    /// Deterministic 32-bit hash for an arbitrary string. Used as a
    /// pg advisory-lock key argument. We deliberately avoid
    /// <see cref="string.GetHashCode()"/> because the runtime randomises
    /// it per-process — two history-service replicas would lock on
    /// different keys and not see each other. FNV-1a 32-bit gives a
    /// stable, fast, dependency-free hash that suits the 32-bit
    /// pg_advisory_xact_lock(int4,int4) contract.
    /// </summary>
    internal static int StableHashInt32(string inValue)
    {
        // FNV-1a 32-bit
        const uint kFnvOffsetBasis = 2166136261u;
        const uint kFnvPrime = 16777619u;
        var tmpHash = kFnvOffsetBasis;
        for (var i = 0; i < inValue.Length; i++)
        {
            tmpHash ^= inValue[i];
            tmpHash *= kFnvPrime;
        }
        return unchecked((int)tmpHash);
    }
}

/// <summary>
/// Storage type of the <c>range_from</c> / <c>range_to</c> columns on a
/// miss-marker table. Bars + NBBO are <see cref="Timestamptz"/>; chains +
/// macro are <see cref="Date"/>. The writer uses this to insert with the
/// correct postgres cast so a uniform <see cref="DateTimeOffset"/>
/// in-memory range value lands correctly in both shapes.
/// </summary>
public enum RangeMarkerColumnType
{
    Timestamptz,
    Date,
}

/// <summary>
/// Schema descriptor for a range-shaped miss-marker table. Used by
/// <see cref="RangeMarkerWriter"/> to produce SQL that's table-agnostic
/// without resorting to an ORM. Capture column names verbatim — bars
/// uses <c>range_from</c> / <c>range_to</c>, but the date-typed tables
/// might use <c>range_from_date</c> / <c>range_to_date</c> after their
/// migration in PRs 4/5, and the writer should not care.
/// </summary>
public sealed record RangeMarkerTableSpec(
    string TableName,
    /// <summary>Composite-key column names in the order
    /// <see cref="RangeMarkerWriter.WriteAsync"/> expects values to be
    /// supplied (e.g. ["symbol", "timeframe"] for bars).</summary>
    IReadOnlyList<string> KeyColumns,
    string RangeFromColumn,
    string RangeToColumn,
    string FetchedAtColumn,
    bool HasReasonColumn,
    string ReasonColumn,
    RangeMarkerColumnType RangeColumnType = RangeMarkerColumnType.Timestamptz);
