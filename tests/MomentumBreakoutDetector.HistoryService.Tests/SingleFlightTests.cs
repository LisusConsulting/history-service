using MomentumBreakoutDetector.HistoryService.Concurrency;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Phase 1, micro-PR #6 — unit tests for <see cref="SingleFlight{TKey, TResult}"/>.
///
/// The contract under test:
///   - 100 concurrent calls with the same key → factory invoked exactly 1×.
///   - Different keys → independent factory invocations.
///   - Faulted task does NOT permanently poison the cache; subsequent
///     calls re-run the factory.
///   - Slot is removed on completion (InFlightCount returns to 0).
/// </summary>
public class SingleFlightTests
{
    [Fact]
    public async Task Concurrent100SameKey_OneFactoryCall()
    {
        // Arrange — coalescer with a delaying factory so the 100 callers
        // pile up on the same in-flight Lazy. Without the delay the first
        // caller could finish + remove the slot before others arrive,
        // and we'd see >1 factory invocation legitimately.
        var tmpSf = new SingleFlight<string, int>();
        var tmpFactoryCalls = 0;
        var tmpStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tmpRelease = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<int> Factory()
        {
            Interlocked.Increment(ref tmpFactoryCalls);
            tmpStarted.TrySetResult();
            await tmpRelease.Task.ConfigureAwait(false);
            return 42;
        }

        // Act — kick off 100 callers, wait until at least one has entered
        // the factory, then release.
        var tmpTasks = new Task<int>[100];
        for (var i = 0; i < tmpTasks.Length; i++)
        {
            tmpTasks[i] = tmpSf.ExecuteAsync("same-key", Factory);
        }

        await tmpStarted.Task.ConfigureAwait(false);
        // Brief settle so all 100 have time to GetOrAdd before we release.
        await Task.Delay(50).ConfigureAwait(false);
        tmpRelease.TrySetResult();
        var tmpResults = await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        // Assert — exactly one factory invocation, and every caller got 42.
        tmpFactoryCalls.ShouldBe(1);
        tmpResults.ShouldAllBe(r => r == 42);
        tmpSf.InFlightCount.ShouldBe(0);
    }

    [Fact]
    public async Task DifferentKeys_DifferentFactoryCalls()
    {
        var tmpSf = new SingleFlight<string, string>();
        var tmpCallsByKey = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();

        Func<string, Func<Task<string>>> factoryFor = key => async () =>
        {
            tmpCallsByKey.AddOrUpdate(key, 1, (_, v) => v + 1);
            await Task.Delay(20).ConfigureAwait(false);
            return $"value-{key}";
        };

        var tmpFactoryA = factoryFor("A");
        var tmpFactoryB = factoryFor("B");

        var tmpTasks = new List<Task<string>>(100);
        for (var i = 0; i < 50; i++)
        {
            tmpTasks.Add(tmpSf.ExecuteAsync("A", tmpFactoryA));
            tmpTasks.Add(tmpSf.ExecuteAsync("B", tmpFactoryB));
        }

        var tmpResults = await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        // Each key's factory ran exactly once.
        tmpCallsByKey["A"].ShouldBe(1);
        tmpCallsByKey["B"].ShouldBe(1);

        // Half the callers got "value-A", the other half "value-B".
        tmpResults.Count(r => r == "value-A").ShouldBe(50);
        tmpResults.Count(r => r == "value-B").ShouldBe(50);
        tmpSf.InFlightCount.ShouldBe(0);
    }

    [Fact]
    public async Task FaultedTask_DoesNotPersist()
    {
        // Arrange — first invocation throws; second invocation must
        // see a fresh empty slot and call factory again.
        var tmpSf = new SingleFlight<string, int>();
        var tmpAttempts = 0;

        Func<Task<int>> factory = async () =>
        {
            var tmpN = Interlocked.Increment(ref tmpAttempts);
            await Task.Yield();
            if (tmpN == 1) throw new InvalidOperationException("boom");
            return 7;
        };

        // Act — first call throws.
        await Should.ThrowAsync<InvalidOperationException>(
            () => tmpSf.ExecuteAsync("k", factory)).ConfigureAwait(false);

        // Subsequent call against the same key must re-run the factory
        // (i.e. the previous faulted Lazy was evicted).
        var tmpResult = await tmpSf.ExecuteAsync("k", factory).ConfigureAwait(false);

        tmpAttempts.ShouldBe(2);
        tmpResult.ShouldBe(7);
        tmpSf.InFlightCount.ShouldBe(0);
    }

    [Fact]
    public async Task Removal_OnSuccess()
    {
        // Arrange + Act — single successful execution.
        var tmpSf = new SingleFlight<string, int>();
        var tmpResult = await tmpSf.ExecuteAsync("k", () => Task.FromResult(99)).ConfigureAwait(false);

        // Assert — slot evicted, count is 0.
        tmpResult.ShouldBe(99);
        tmpSf.InFlightCount.ShouldBe(0);
    }
}
