using MomentumBreakoutDetector.HistoryService.MessageHandlers;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Phase E unit test for <see cref="ConcurrencyLimitingHandler"/>.
///
/// Asserts the handler enforces the configured concurrency cap on the
/// in-flight HTTP pipeline: 100 concurrent requests against a slow
/// in-memory mock handler, with a SemaphoreSlim(8) cap, must observe
/// max-concurrent-in-flight ≤ 8.
///
/// This is the contract the original raw-HttpClient fetchers' per-fetcher
/// SemaphoreSlim guaranteed; preserving it here proves the SDK refactor
/// kept the dollar-cost ceiling intact.
/// </summary>
public class ConcurrencyLimitingHandlerTests
{
    [Fact]
    public async Task LimitsConcurrentInFlightRequests_ToConfiguredMax()
    {
        // Arrange — reset the static gate to a known cap of 8 (the
        // production default) so this test is independent of any other
        // test that may have initialized the static at a different value.
        const int tmpMax = 8;
        const int tmpFanout = 100;
        ConcurrencyLimitingHandler.ResetGateForTests(tmpMax);

        var tmpInner = new MaxConcurrentObservingHandler(perRequestDelayMs: 50);
        var tmpHandler = new ConcurrencyLimitingHandler(tmpMax)
        {
            InnerHandler = tmpInner,
        };
        var tmpClient = new HttpClient(tmpHandler);

        // Act — fire `tmpFanout` concurrent requests at once.
        var tmpTasks = new Task<HttpResponseMessage>[tmpFanout];
        for (int i = 0; i < tmpFanout; i++)
        {
            tmpTasks[i] = tmpClient.GetAsync("https://example.test/");
        }

        await Task.WhenAll(tmpTasks);

        // Assert — at no point did more than `tmpMax` requests run in
        // parallel through the inner handler.
        tmpInner.MaxConcurrentObserved.ShouldBeLessThanOrEqualTo(tmpMax);
        tmpInner.TotalCallCount.ShouldBe(tmpFanout);

        // Sanity — the test would be useless if it never actually ran
        // anything in parallel. With 100-way fanout + 50ms delay + cap=8,
        // we expect to see at least 2 concurrent (the cap is what bounds
        // it; a single-threaded run would be a regression).
        tmpInner.MaxConcurrentObserved.ShouldBeGreaterThan(1);
    }

    /// <summary>
    /// HttpMessageHandler that artificially holds each request for a
    /// fixed delay and tracks the peak concurrent in-flight count.
    /// </summary>
    private sealed class MaxConcurrentObservingHandler : HttpMessageHandler
    {
        private int m_Current;
        private int m_Max;
        private int m_Total;
        private readonly int m_PerRequestDelayMs;

        public int MaxConcurrentObserved => Volatile.Read(ref m_Max);
        public int TotalCallCount => Volatile.Read(ref m_Total);

        public MaxConcurrentObservingHandler(int perRequestDelayMs)
        {
            m_PerRequestDelayMs = perRequestDelayMs;
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var tmpNow = Interlocked.Increment(ref m_Current);
            // Atomic max — keep bumping if we just hit a new high.
            int tmpSeen;
            do
            {
                tmpSeen = Volatile.Read(ref m_Max);
                if (tmpNow <= tmpSeen) break;
            } while (Interlocked.CompareExchange(ref m_Max, tmpNow, tmpSeen) != tmpSeen);

            try
            {
                await Task.Delay(m_PerRequestDelayMs, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref m_Total);
                return new HttpResponseMessage(System.Net.HttpStatusCode.OK);
            }
            finally
            {
                Interlocked.Decrement(ref m_Current);
            }
        }
    }
}
