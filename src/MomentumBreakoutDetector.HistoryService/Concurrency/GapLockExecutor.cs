using Dapper;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Concurrency;

/// <summary>
/// Two-tier mutex for "fetch-and-persist this gap exactly once" semantics
/// across all four providers (bars, NBBO, chains, macro). Composes the
/// existing in-process <see cref="SingleFlight{TKey, TResult}"/> primitive
/// with a short Postgres <c>pg_advisory_xact_lock</c> wrapping the
/// persistence write so the same gap key serializes both within a single
/// history-service replica AND across replicas.
///
/// <para>
/// <b>Layer 1 — in-process SingleFlight (gap-range granularity).</b>
/// Two concurrent callers requesting overlapping ranges that share a gap
/// will collapse on the gap's key via
/// <see cref="ExecuteFetchAndPersistAsync{TPayload}"/>: only one runs the
/// upstream-fetch + DB-persist; the other awaits the same Task. That alone
/// makes duplicate-key INSERT races structurally impossible within the
/// same process — the second writer never executes its INSERT because it
/// never enters the body.
/// </para>
///
/// <para>
/// <b>Layer 3 — Postgres advisory lock (cross-replica granularity).</b>
/// The persist step (<see cref="WithPersistLockAsync"/>) opens a fresh
/// transaction, takes <c>pg_advisory_xact_lock(table_hash, key_hash)</c>,
/// runs the supplied DML, and commits. Held only across DB I/O — NOT
/// across the upstream fetch — so a 30s Polygon RTT does not pin a
/// postgres lock. A second replica racing on the same gap will block at
/// lock acquisition for the brief persist window, then either find the
/// data already there (ON CONFLICT DO NOTHING) or harmlessly overwrite
/// equal data. Hash uses <see cref="RangeMarkerWriter.StableHashInt32"/>
/// (FNV-1a 32-bit) so two history-service replicas resolve the same lock
/// key — <see cref="string.GetHashCode"/> is randomised per process and
/// would defeat this.
/// </para>
///
/// <para>
/// <b>Why split fetch from persist into two methods.</b> Holding a postgres
/// transaction across an upstream HTTP call is brittle: a slow Polygon
/// response or a network blip pins the connection + the lock for 30s+,
/// raising contention for unrelated callers. Splitting them keeps the
/// lock window proportional to DB work alone (microseconds-to-millis).
/// SingleFlight covers the duplicate-fetch case in-process; cross-replica
/// duplicate fetches are acceptable (Polygon Advanced is effectively
/// uncapped) — only the writes need to serialize.
/// </para>
/// </summary>
/// <typeparam name="TKey">Gap-key shape. Use a <c>record</c> so equality
/// + hash are value-based and immutable.</typeparam>
public sealed class GapLockExecutor<TKey> where TKey : notnull
{
    private readonly SingleFlight<TKey, GapResult> m_InProcess = new();

    /// <summary>
    /// Marker result tracked for diagnostics. We don't actually need a
    /// payload from <see cref="ExecuteFetchAndPersistAsync{TPayload}"/>
    /// for the on-demand-fill use case — providers persist as a side
    /// effect — but SingleFlight requires <c>TResult</c>, so we use
    /// this token.
    /// </summary>
    private sealed record GapResult(bool RanInThisCaller);

    /// <summary>
    /// Run <paramref name="inFetchAndPersist"/> exactly once for the given
    /// <paramref name="inKey"/> across all concurrent callers in this
    /// process. Cross-replica duplicates are NOT collapsed by this layer —
    /// use <see cref="WithPersistLockAsync"/> inside the body to serialize
    /// the actual DB writes.
    /// </summary>
    /// <returns>True iff this caller actually ran the body (i.e. won the
    /// SingleFlight). False iff this caller joined an in-flight invocation
    /// started by an earlier caller.</returns>
    public async Task<bool> ExecuteFetchAndPersistAsync(
        TKey inKey, Func<Task> inFetchAndPersist)
    {
        var tmpRanHere = false;
        await m_InProcess.ExecuteAsync(inKey, async () =>
        {
            await inFetchAndPersist().ConfigureAwait(false);
            tmpRanHere = true;
            return new GapResult(RanInThisCaller: true);
        }).ConfigureAwait(false);
        return tmpRanHere;
    }

    /// <summary>
    /// Run a short, lock-protected persistence transaction. Opens a
    /// transaction on <paramref name="inConn"/>, takes
    /// <c>pg_advisory_xact_lock(namespace_hash, key_hash)</c>, invokes
    /// <paramref name="inWork"/> with the open transaction, then commits
    /// (or rolls back on exception). Hold-time is bounded by DB I/O; the
    /// upstream fetch should NOT live inside this scope.
    /// </summary>
    /// <param name="inConn">Open <see cref="NpgsqlConnection"/>. Caller
    /// owns lifecycle.</param>
    /// <param name="inLockNamespace">String hashed into the first half of
    /// the advisory-lock pair. Use the table name (e.g. "historical_bars",
    /// "macro_data") so locks for different surfaces don't collide.</param>
    /// <param name="inLockKeySeed">Stable string for the second half of the
    /// advisory-lock pair. Encode the full gap identity, e.g.
    /// <c>"TSLA|1min|2025-04-15T13:30:00Z|2025-04-15T20:00:00Z"</c>.</param>
    /// <param name="inWork">DML body. Receives the connection + active
    /// transaction; the helper commits/rolls back.</param>
    /// <param name="inCt">Cancellation token.</param>
    public static async Task WithPersistLockAsync(
        NpgsqlConnection inConn,
        string inLockNamespace,
        string inLockKeySeed,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task> inWork,
        CancellationToken inCt)
    {
        await using var tmpTx = await inConn.BeginTransactionAsync(inCt).ConfigureAwait(false);
        try
        {
            var tmpNamespaceHash = RangeMarkerWriter.StableHashInt32(inLockNamespace);
            var tmpKeyHash = RangeMarkerWriter.StableHashInt32(inLockKeySeed);
            await inConn.ExecuteAsync(
                "SELECT pg_advisory_xact_lock(@NamespaceHash, @KeyHash)",
                new { NamespaceHash = tmpNamespaceHash, KeyHash = tmpKeyHash },
                tmpTx).ConfigureAwait(false);

            await inWork(inConn, tmpTx, inCt).ConfigureAwait(false);

            await tmpTx.CommitAsync(inCt).ConfigureAwait(false);
        }
        catch
        {
            await tmpTx.RollbackAsync(inCt).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Diagnostic: number of in-flight gap keys currently being coalesced
    /// in this process.
    /// </summary>
    public int InFlightCount => m_InProcess.InFlightCount;
}
