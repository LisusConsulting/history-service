using System.Collections.Concurrent;

namespace MomentumBreakoutDetector.HistoryService.Concurrency;

/// <summary>
/// Concurrent-dedup ("single-flight") primitive: collapses N concurrent
/// callers asking for the same <typeparamref name="TKey"/> into a single
/// upstream invocation of <c>factory</c>, then fans out the result to all
/// waiters.
///
/// Phase 1, micro-PR #6. The motivating shape: 50 concurrent backtests on
/// the same env hitting <c>HistoryService.GetBars(TSLA, 2024-01-15, 5min)</c>
/// must result in exactly ONE Polygon /v2/aggs call, not 50. Cache lookups
/// upstream of the fetcher do NOT solve this on a cold-start window — by
/// the time caller #2 looks at the cache, caller #1 hasn't written yet.
/// SingleFlight closes that race at the fetcher boundary.
///
/// Implementation notes:
///   - Backed by a <see cref="ConcurrentDictionary{TKey, TValue}"/> keyed
///     on the request shape. Each value is a
///     <see cref="Lazy{Task{TResult}}"/> so the factory runs at most once
///     per key, even if <c>GetOrAdd</c>'s value-factory races.
///   - On task completion (success OR failure), the entry is removed via
///     a key-and-value <see cref="KeyValuePair{TKey, TValue}"/> compare-on-
///     remove. This guarantees:
///       (a) a faulted Task does NOT permanently block retries — the next
///           caller for the same key sees an empty slot and re-runs the
///           factory.
///       (b) a fresh in-flight call that started AFTER the original
///           completed is not accidentally evicted by a stale finally
///           block.
///   - Cancellation is the caller's responsibility: the factory closure
///     captures the originator's <see cref="CancellationToken"/>. Late
///     joiners share that fate. This matches Go's <c>singleflight.Group</c>
///     and is the right semantics for our use case (all callers want the
///     same result; cancelling one shouldn't kill the upstream call the
///     others are waiting on).
/// </summary>
/// <typeparam name="TKey">Request shape used to dedup. Records work
/// well — value-equality + immutable.</typeparam>
/// <typeparam name="TResult">Fetch result type.</typeparam>
public sealed class SingleFlight<TKey, TResult> where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, Lazy<Task<TResult>>> m_InFlight = new();

    /// <summary>
    /// Execute <paramref name="inFactory"/> exactly once for the given
    /// <paramref name="inKey"/> across all concurrent callers, and return
    /// the same result to every caller.
    /// </summary>
    public async Task<TResult> ExecuteAsync(TKey inKey, Func<Task<TResult>> inFactory)
    {
        var tmpLazy = m_InFlight.GetOrAdd(
            inKey,
            _ => new Lazy<Task<TResult>>(inFactory, LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return await tmpLazy.Value.ConfigureAwait(false);
        }
        finally
        {
            // Compare-on-remove: only evict if the slot still holds OUR
            // Lazy. Prevents racing with a fresh in-flight that started
            // after we resolved.
            ((ICollection<KeyValuePair<TKey, Lazy<Task<TResult>>>>)m_InFlight)
                .Remove(new KeyValuePair<TKey, Lazy<Task<TResult>>>(inKey, tmpLazy));
        }
    }

    /// <summary>
    /// Diagnostic: number of in-flight keys currently being coalesced.
    /// Used by tests + (later) the cache stats endpoint in micro-PR #8.
    /// </summary>
    public int InFlightCount => m_InFlight.Count;
}
