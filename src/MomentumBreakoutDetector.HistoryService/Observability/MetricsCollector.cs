using System.Collections.Concurrent;

namespace MomentumBreakoutDetector.HistoryService.Observability;

/// <summary>
/// Per-data-class counter classification. Maps 1:1 with the proto
/// <c>DataClass</c> enum but is duplicated here to avoid pulling
/// the gRPC-generated namespace into providers/fetchers (which sit
/// below the gRPC layer). The HistoryServiceImpl bridges between
/// the two when shaping the GetCacheStats response.
/// </summary>
public enum MetricKind
{
    Bars = 0,
    Nbbo = 1,
    Chains = 2,
    Macro = 3,
}

/// <summary>
/// Snapshot of one data-class's counters. Ints are <see cref="long"/>
/// because counter wrap on a long-running process would be embarrassing.
/// Latency percentiles are computed lazily from a fixed-size ring
/// buffer of recent fetch durations.
/// </summary>
public sealed record ClassMetricsSnapshot(
    MetricKind Kind,
    long TotalRequests,
    long CacheHits,
    long UpstreamFetches,
    long MissMarkers,
    int InFlightCount,
    double LatencyP50Ms,
    double LatencyP95Ms,
    double LatencyP99Ms);

/// <summary>
/// Aggregate snapshot — one entry per <see cref="MetricKind"/> in a
/// stable order plus an as-of timestamp.
/// </summary>
public sealed record MetricsSnapshot(
    IReadOnlyList<ClassMetricsSnapshot> Classes,
    DateTime AsOfUtc);

/// <summary>
/// Process-wide counter + histogram store for the history-service.
///
/// Phase 1, micro-PR #8 — observability surface. Counters are atomic
/// (<see cref="Interlocked"/>) so providers / fetchers can call into
/// the collector without taking locks. Latency samples land in a fixed
/// 1024-slot ring buffer per kind; <see cref="Snapshot"/> sorts a copy
/// for percentile math (p50/p95/p99). The ring is small enough that
/// sorting on every snapshot read is cheap (microseconds), and big
/// enough that a backtest issuing thousands of fetches can still
/// produce a sensible window.
///
/// In-flight counts are NOT counters here — they're queried per-snapshot
/// from the registered fetchers (each fetcher exposes its own
/// SingleFlight count). Providers/fetchers register a per-kind
/// in-flight delegate at construction time via <see cref="RegisterInFlightProbe"/>.
///
/// Cache-hit accounting lives at the provider layer (the layer that
/// looks at the cache before the fetcher); upstream-fetch + miss-marker
/// + latency live at the fetcher layer (so the count reflects actual
/// wire calls, not coalesced waiters).
/// </summary>
public sealed class MetricsCollector
{
    private const int LatencyRingSize = 1024;

    private readonly ClassState[] m_State;

    public MetricsCollector()
    {
        m_State = new ClassState[Enum.GetValues<MetricKind>().Length];
        for (int i = 0; i < m_State.Length; i++)
        {
            m_State[i] = new ClassState((MetricKind)i, LatencyRingSize);
        }
    }

    public void RecordCacheHit(MetricKind inKind)
        => Interlocked.Increment(ref Get(inKind).CacheHits);

    /// <summary>
    /// Increment total-requests independently of cache-hit / fetch
    /// classification. Currently only the gRPC layer calls this;
    /// providers report cache-hit only on actually-served-from-cache
    /// reads, and fetchers report fetch on actual wire calls.
    /// </summary>
    public void RecordRequest(MetricKind inKind)
        => Interlocked.Increment(ref Get(inKind).TotalRequests);

    /// <summary>
    /// Record a successful upstream fetch and its observed latency.
    /// Increments <see cref="ClassMetricsSnapshot.UpstreamFetches"/>
    /// and pushes <paramref name="inLatencyMs"/> onto the per-kind
    /// ring buffer. Call this after the wire call returns regardless
    /// of whether the response was empty (empty = miss-marker, but
    /// the upstream fetch still happened).
    /// </summary>
    public void RecordUpstreamFetch(MetricKind inKind, double inLatencyMs)
    {
        var tmpState = Get(inKind);
        Interlocked.Increment(ref tmpState.UpstreamFetches);
        tmpState.RecordLatency(inLatencyMs);
    }

    public void RecordMissMarker(MetricKind inKind)
        => Interlocked.Increment(ref Get(inKind).MissMarkers);

    /// <summary>
    /// Register a function that returns the current in-flight count for
    /// <paramref name="inKind"/>. Multiple registrations are summed
    /// (e.g. if fetcher + provider both expose in-flight counts).
    /// </summary>
    public void RegisterInFlightProbe(MetricKind inKind, Func<int> inProbe)
        => Get(inKind).InFlightProbes.Add(inProbe);

    public MetricsSnapshot Snapshot()
    {
        var tmpClasses = new List<ClassMetricsSnapshot>(m_State.Length);
        foreach (var tmpState in m_State)
        {
            tmpClasses.Add(tmpState.Snapshot());
        }
        return new MetricsSnapshot(tmpClasses, DateTime.UtcNow);
    }

    private ClassState Get(MetricKind inKind) => m_State[(int)inKind];

    private sealed class ClassState
    {
        public readonly MetricKind Kind;
        public long TotalRequests;
        public long CacheHits;
        public long UpstreamFetches;
        public long MissMarkers;

        public readonly ConcurrentBag<Func<int>> InFlightProbes = new();

        // Ring-buffer of recent fetch durations (ms). Writes are
        // serialized by a SpinLock-style monotonic index (Interlocked
        // on m_RingNext), so a single Snapshot reading the buffer
        // copies under a brief lock to get a consistent view.
        private readonly double[] m_Ring;
        private long m_RingNext; // total writes; modulo gives slot.
        private readonly object m_RingLock = new();

        public ClassState(MetricKind inKind, int inRingSize)
        {
            Kind = inKind;
            m_Ring = new double[inRingSize];
        }

        public void RecordLatency(double inLatencyMs)
        {
            // Two-step write under a brief lock — preferred over a
            // lock-free design here because Snapshot needs a consistent
            // read of the same window. The lock is held for ~one array
            // store; uncontended ~10ns.
            lock (m_RingLock)
            {
                var tmpIdx = (int)(m_RingNext % m_Ring.Length);
                m_Ring[tmpIdx] = inLatencyMs;
                m_RingNext++;
            }
        }

        public ClassMetricsSnapshot Snapshot()
        {
            // Copy the live samples under the same lock to avoid a
            // half-written slot showing up in the percentile calc.
            double[] tmpCopy;
            int tmpCount;
            lock (m_RingLock)
            {
                tmpCount = (int)Math.Min(m_RingNext, m_Ring.Length);
                tmpCopy = new double[tmpCount];
                if (tmpCount > 0)
                {
                    if (m_RingNext <= m_Ring.Length)
                    {
                        Array.Copy(m_Ring, 0, tmpCopy, 0, tmpCount);
                    }
                    else
                    {
                        // Buffer has wrapped — copy in oldest-to-newest order.
                        var tmpStart = (int)(m_RingNext % m_Ring.Length);
                        Array.Copy(m_Ring, tmpStart, tmpCopy, 0, m_Ring.Length - tmpStart);
                        Array.Copy(m_Ring, 0, tmpCopy, m_Ring.Length - tmpStart, tmpStart);
                    }
                }
            }

            double tmpP50 = 0, tmpP95 = 0, tmpP99 = 0;
            if (tmpCount > 0)
            {
                Array.Sort(tmpCopy);
                tmpP50 = Percentile(tmpCopy, 0.50);
                tmpP95 = Percentile(tmpCopy, 0.95);
                tmpP99 = Percentile(tmpCopy, 0.99);
            }

            int tmpInFlight = 0;
            foreach (var tmpProbe in InFlightProbes)
            {
                try { tmpInFlight += tmpProbe(); }
                catch { /* probe failure must not break the snapshot */ }
            }

            return new ClassMetricsSnapshot(
                Kind: Kind,
                TotalRequests: Interlocked.Read(ref TotalRequests),
                CacheHits: Interlocked.Read(ref CacheHits),
                UpstreamFetches: Interlocked.Read(ref UpstreamFetches),
                MissMarkers: Interlocked.Read(ref MissMarkers),
                InFlightCount: tmpInFlight,
                LatencyP50Ms: tmpP50,
                LatencyP95Ms: tmpP95,
                LatencyP99Ms: tmpP99);
        }

        private static double Percentile(double[] inSorted, double inP)
        {
            if (inSorted.Length == 0) return 0;
            // Nearest-rank with clamp; for p99 on a small sample size
            // this just picks the max, which is the desired behaviour.
            var tmpRank = (int)Math.Ceiling(inP * inSorted.Length) - 1;
            if (tmpRank < 0) tmpRank = 0;
            if (tmpRank >= inSorted.Length) tmpRank = inSorted.Length - 1;
            return inSorted[tmpRank];
        }
    }
}
