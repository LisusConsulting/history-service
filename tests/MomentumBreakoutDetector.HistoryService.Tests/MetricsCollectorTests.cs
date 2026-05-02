using MomentumBreakoutDetector.HistoryService.Observability;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Unit tests for the <see cref="MetricsCollector"/> primitive — counter
/// arithmetic, percentile math, in-flight probe aggregation. Hermetic
/// (no postgres, no network), runs in &lt; 50ms.
/// </summary>
public sealed class MetricsCollectorTests
{
    [Fact]
    public void Counters_StartAtZero_AndIncrementCorrectly()
    {
        var tmpC = new MetricsCollector();

        tmpC.RecordCacheHit(MetricKind.Bars);
        tmpC.RecordCacheHit(MetricKind.Bars);
        tmpC.RecordUpstreamFetch(MetricKind.Bars, 12.5);
        tmpC.RecordMissMarker(MetricKind.Bars);

        var tmpSnap = tmpC.Snapshot();
        var tmpBars = tmpSnap.Classes.First(c => c.Kind == MetricKind.Bars);
        tmpBars.CacheHits.ShouldBe(2L);
        tmpBars.UpstreamFetches.ShouldBe(1L);
        tmpBars.MissMarkers.ShouldBe(1L);

        // Other kinds untouched.
        var tmpNbbo = tmpSnap.Classes.First(c => c.Kind == MetricKind.Nbbo);
        tmpNbbo.CacheHits.ShouldBe(0L);
        tmpNbbo.UpstreamFetches.ShouldBe(0L);
    }

    [Fact]
    public void LatencyPercentiles_OnUniformDistribution_AreSane()
    {
        var tmpC = new MetricsCollector();
        // 100 samples evenly spaced 1..100 ms.
        for (int i = 1; i <= 100; i++)
        {
            tmpC.RecordUpstreamFetch(MetricKind.Bars, i);
        }
        var tmpSnap = tmpC.Snapshot();
        var tmpBars = tmpSnap.Classes.First(c => c.Kind == MetricKind.Bars);
        // p50 of 1..100 with nearest-rank ceiling: ceil(0.5*100)=50.
        tmpBars.LatencyP50Ms.ShouldBe(50d);
        // p95 = 95, p99 = 99.
        tmpBars.LatencyP95Ms.ShouldBe(95d);
        tmpBars.LatencyP99Ms.ShouldBe(99d);
    }

    [Fact]
    public void InFlightProbes_AreSummed_OverMultipleRegistrations()
    {
        var tmpC = new MetricsCollector();
        var tmpA = 3;
        var tmpB = 7;
        tmpC.RegisterInFlightProbe(MetricKind.Chains, () => tmpA);
        tmpC.RegisterInFlightProbe(MetricKind.Chains, () => tmpB);

        var tmpSnap = tmpC.Snapshot();
        tmpSnap.Classes.First(c => c.Kind == MetricKind.Chains)
            .InFlightCount.ShouldBe(10);
    }

    [Fact]
    public void Counters_AreThreadSafe_UnderConcurrentIncrement()
    {
        var tmpC = new MetricsCollector();
        const int kThreads = 8;
        const int kPerThread = 1000;

        Parallel.For(0, kThreads, _ =>
        {
            for (int i = 0; i < kPerThread; i++)
            {
                tmpC.RecordCacheHit(MetricKind.Bars);
                tmpC.RecordUpstreamFetch(MetricKind.Bars, 5);
            }
        });

        var tmpSnap = tmpC.Snapshot();
        var tmpBars = tmpSnap.Classes.First(c => c.Kind == MetricKind.Bars);
        tmpBars.CacheHits.ShouldBe((long)(kThreads * kPerThread));
        tmpBars.UpstreamFetches.ShouldBe((long)(kThreads * kPerThread));
    }

    [Fact]
    public void Snapshot_AsOfTimestamp_IsRecent()
    {
        var tmpC = new MetricsCollector();
        var tmpBefore = DateTime.UtcNow;
        var tmpSnap = tmpC.Snapshot();
        var tmpAfter = DateTime.UtcNow;

        tmpSnap.AsOfUtc.ShouldBeGreaterThanOrEqualTo(tmpBefore.AddSeconds(-1));
        tmpSnap.AsOfUtc.ShouldBeLessThanOrEqualTo(tmpAfter.AddSeconds(1));
    }
}
