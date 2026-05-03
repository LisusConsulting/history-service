using System.Collections.Concurrent;
using Dapper;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;
using DomainBar = MomentumBreakoutDetector.HistoryService.Domain.Bar;
using DomainBarTimeframe = MomentumBreakoutDetector.HistoryService.Domain.BarTimeframe;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Phase 1, micro-PR #7 — EnsureRangeCached warmup endpoint tests.
///
/// Two scenarios:
/// <list type="number">
///   <item>
///     <b>Concurrent warmup + point-fetches coalesce.</b> A slow stub
///     fetcher returns 100 bars after a 500ms delay. We start an
///     <c>EnsureRangeCachedAsync</c> warmup and 50 concurrent point-fetch
///     <c>GetBarsAsync</c> callers requesting the same (symbol, range,
///     timeframe). The SingleFlight coalescer in <see cref="PolygonBarFetcher"/>
///     folds them into a single upstream call. We assert: stub called
///     EXACTLY ONCE, all 50 point-fetches return the warmed bars.
///   </item>
///   <item>
///     <b>gRPC-level stream subscriber.</b> Drives
///     <see cref="HistoryServiceImpl.EnsureRangeCached"/> end-to-end with
///     a real Postgres + bars stub, captures the streamed
///     <c>EnsureRangeCachedProgress</c> events, asserts shape (Planning →
///     Fetching* → Completed for the BARS class).
///   </item>
/// </list>
/// </summary>
public sealed class EnsureRangeCachedTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer m_Pg = new PostgreSqlBuilder()
        .WithImage("timescale/timescaledb:latest-pg16")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    private string m_ConnectionString = null!;

    public async Task InitializeAsync()
    {
        await m_Pg.StartAsync();
        m_ConnectionString = m_Pg.GetConnectionString();
        await ApplySchemaAsync(m_ConnectionString);
    }

    public Task DisposeAsync() => m_Pg.DisposeAsync().AsTask();

    [Fact]
    public async Task ConcurrentWarmup_PointFetchCoalescesWithWarmup()
    {
        // Arrange — slow stub fetcher (500ms delay) returns 100 bars for
        // any range. Counts every invocation. SingleFlight wraps the
        // PolygonBarFetcher API surface, so identical-key requests during
        // the warmup's in-flight upstream call get folded.
        var tmpDay = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc);
        var tmpFromTs = tmpDay.AddHours(13).AddMinutes(30);  // 13:30 UTC
        var tmpToTs = tmpDay.AddHours(14).AddMinutes(09);    // 14:09 UTC inclusive (40 minutes)

        var tmpBars = new List<DomainBar>(40);
        for (var i = 0; i < 40; i++)
        {
            tmpBars.Add(new DomainBar(
                "TSLA",
                tmpFromTs.AddMinutes(i),
                250.10m + i, 250.50m + i, 250.00m + i, 250.30m + i,
                1000m + i, 250.20m + i));
        }

        var tmpStub = new SlowCountingStub(tmpBars, delayMs: 0);
        // Use an explicit gate instead of relying on Task.Delay timing —
        // slow-CI races otherwise let some callers slip past SingleFlight.
        // The gate keeps the first fetch in-flight while we queue 50
        // concurrent waiters; release once they're all resident.
        tmpStub.HoldOpen();
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnectionString,
            tmpStub,
            NullLogger<HistoricalBarsProvider>.Instance);

        // Act —
        //   1. Kick off warmup for the full range. Fetch enters SingleFlight
        //      and parks at the gate.
        //   2. Queue 50 concurrent point-fetches with the SAME (symbol,
        //      fromUtc, toUtc, timeframe) key. Each gap-detects + queues at
        //      the same SingleFlight slot.
        //   3. Release the gate; all 51 callers (warmup + 50) share the
        //      single resolved fetch.
        var tmpWarmupTask = tmpProvider.EnsureRangeCachedAsync(
            "TSLA", tmpFromTs, tmpToTs, DomainBarTimeframe.OneMinute,
            inProgress: null, inCt: CancellationToken.None);

        // Poll until the warmup has arrived at the fetcher boundary
        // (Arrivals==1) — guarantees the SingleFlight slot is open and
        // parked at the gate when point fetches start queueing.
        var tmpDeadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < tmpDeadline)
        {
            if (tmpStub.Arrivals >= 1) break;
            await Task.Delay(20).ConfigureAwait(false);
        }
        tmpStub.Arrivals.ShouldBe(1, "warmup must reach the fetcher before point-fetches start");

        var tmpPointFetchTasks = new Task<BarsReadResult>[50];
        for (var i = 0; i < 50; i++)
        {
            tmpPointFetchTasks[i] = tmpProvider.GetBarsAsync(
                "TSLA", tmpFromTs, tmpToTs, DomainBarTimeframe.OneMinute);
        }

        // Post 2026-05-02 concurrency-safety hardening: the
        // HistoricalBarsProvider now wraps each gap-range chunk in
        // GapLockExecutor at the provider level. Two concurrent
        // EnsureRangeCachedAsync / GetBarsAsync callers with the same
        // (symbol, timeframe, from, to) collapse on the BarGapKey BEFORE
        // they reach the fetcher's own SingleFlight. So Arrivals stops
        // climbing at 1 (the warmup); the 50 point-fetches join that
        // same gap-key slot and never enter the fetcher body.
        //
        // We assert the stronger property here: regardless of where
        // de-dup happens (fetcher vs provider), the upstream stub is
        // called EXACTLY ONCE. The release-gate path is now timing-
        // sensitive only via the warmup; we release immediately to
        // unblock every waiter via the SingleFlight chain.
        tmpStub.ReleaseGate();

        var tmpUpstreamCalls = await tmpWarmupTask.ConfigureAwait(false);
        var tmpResults = await Task.WhenAll(tmpPointFetchTasks).ConfigureAwait(false);

        // Assert —
        //   * Warmup performed 1 upstream call (single chunk for a
        //     40-minute range, well under the 30-day MaxFetchChunkDays
        //     ceiling).
        //   * The stub was called EXACTLY ONCE total: warmup hit it,
        //     all 50 point-fetches coalesced on the same in-flight
        //     SingleFlight task in PolygonBarFetcher and shared the result.
        //   * Every point-fetch returned the warmed bars.
        tmpUpstreamCalls.ShouldBe(1);
        tmpStub.CallCount.ShouldBe(1,
            $"warmup + 50 point-fetches with the same key should coalesce to ONE upstream call, but the stub recorded {tmpStub.CallCount}");

        tmpResults.Length.ShouldBe(50);
        foreach (var tmpResult in tmpResults)
        {
            tmpResult.Bars.Count.ShouldBe(40);
        }
    }

    [Fact]
    public async Task EnsureRangeCached_StreamsProgressEvents_ForBarsKind()
    {
        // Arrange — same shape as smoke test, but exercise the streaming
        // gRPC handler end-to-end. We capture the EnsureRangeCachedProgress
        // events the handler writes via a fake response stream.
        var tmpDay = new DateTime(2026, 4, 16, 0, 0, 0, DateTimeKind.Utc);
        var tmpFromTs = tmpDay.AddHours(13).AddMinutes(30);
        var tmpToTs = tmpDay.AddHours(14).AddMinutes(09);

        var tmpBars = new List<DomainBar>();
        for (var i = 0; i < 40; i++)
        {
            tmpBars.Add(new DomainBar(
                "TSLA", tmpFromTs.AddMinutes(i),
                250m + i, 251m + i, 249m + i, 250.5m + i, 1000m, 250.25m + i));
        }
        var tmpStub = new SlowCountingStub(tmpBars, delayMs: 0);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnectionString, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            tmpProvider,
            quotes: new ThrowingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null);

        var tmpRequest = new EnsureRangeCachedRequest
        {
            FromTs = Timestamp.FromDateTime(tmpFromTs),
            ToTs = Timestamp.FromDateTime(tmpToTs),
            BarTimeframe = BarTimeframe.Minute,
        };
        tmpRequest.Symbols.Add("TSLA");
        tmpRequest.DataClasses.Add(DataClass.Bars);

        var tmpStream = new CapturingStreamWriter<EnsureRangeCachedProgress>();
        var tmpCtx = new TestServerCallContext(CancellationToken.None);

        // Act
        await tmpImpl.EnsureRangeCached(tmpRequest, tmpStream, tmpCtx);

        // Assert — events captured: at minimum a Planning, ≥1 Fetching,
        // and a Completed for DATA_CLASS_BARS. Elapsed_ms monotonically
        // non-decreasing.
        tmpStream.Captured.Count.ShouldBeGreaterThanOrEqualTo(3);
        tmpStream.Captured.ShouldContain(p =>
            p.Status == WarmupStatus.Planning && p.DataClass == DataClass.Bars);
        tmpStream.Captured.ShouldContain(p =>
            p.Status == WarmupStatus.Fetching && p.DataClass == DataClass.Bars);
        tmpStream.Captured.ShouldContain(p =>
            p.Status == WarmupStatus.Completed && p.DataClass == DataClass.Bars);

        var tmpCompleted = tmpStream.Captured.First(p =>
            p.Status == WarmupStatus.Completed && p.DataClass == DataClass.Bars);
        tmpCompleted.UpstreamCalls.ShouldBe(1);

        // Diagnostic dump for the PR description / sample output. The
        // test runner captures Console output on failure; in pass we just
        // throw it on stderr so it lands in the build log.
        foreach (var tmpEvt in tmpStream.Captured)
        {
            Console.Error.WriteLine(
                $"[{tmpEvt.ElapsedMs,5}ms] {tmpEvt.DataClass}/{tmpEvt.Status} keys={tmpEvt.KeysComplete}/{tmpEvt.KeysTotal} upstream={tmpEvt.UpstreamCalls} :: {tmpEvt.Message}");
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /// <summary>
    /// Stub bar fetcher: counts calls + returns a fixed bar set after a
    /// configurable delay. Mirrors the real <see cref="PolygonBarFetcher"/>
    /// shape so we exercise the SingleFlight coalescer that wraps it.
    /// </summary>
    private sealed class SlowCountingStub : IPolygonBarFetcher
    {
        private readonly IReadOnlyList<DomainBar> m_Bars;
        private readonly int m_DelayMs;
        private int m_Calls;
        private int m_Arrivals;
        private readonly MomentumBreakoutDetector.HistoryService.Concurrency.SingleFlight<BarFetchKey, IReadOnlyList<DomainBar>> m_Coalescer = new();
        private TaskCompletionSource? m_Gate;

        public int CallCount => Volatile.Read(ref m_Calls);
        // Arrivals counts every FetchBarsAsync entry pre-SingleFlight —
        // lets tests poll for "all expected callers have arrived"
        // before releasing the gate.
        public int Arrivals => Volatile.Read(ref m_Arrivals);

        public SlowCountingStub(IReadOnlyList<DomainBar> inBars, int delayMs)
        {
            m_Bars = inBars;
            m_DelayMs = delayMs;
        }

        /// <summary>
        /// Hold the next fetch open until <see cref="ReleaseGate"/>.
        /// Eliminates slow-CI races where the fixed delayMs window isn't
        /// wide enough for all concurrent callers to enter SingleFlight
        /// before the first fetch resolves.
        /// </summary>
        public void HoldOpen()
            => m_Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void ReleaseGate() => m_Gate?.TrySetResult();

        public Task<IReadOnlyList<DomainBar>> FetchBarsAsync(
            string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            DomainBarTimeframe inTimeframe, CancellationToken inCt)
        {
            Interlocked.Increment(ref m_Arrivals);
            // Important: this stub stands in for PolygonBarFetcher, which
            // owns the SingleFlight coalescer. Replicating that here means
            // the test exercises the correct semantics — concurrent
            // identical-key callers fold onto a single FetchBarsAsync_Inner
            // invocation. Without this wrapper, the test would only prove
            // that HistoricalBarsProvider doesn't double-fetch on its own,
            // which is a weaker property.
            var tmpKey = new BarFetchKey(inSymbol, inFromUtc, inToUtc, inTimeframe);
            return m_Coalescer.ExecuteAsync(tmpKey, async () =>
            {
                Interlocked.Increment(ref m_Calls);
                if (m_Gate is not null) await m_Gate.Task.ConfigureAwait(false);
                if (m_DelayMs > 0)
                {
                    await Task.Delay(m_DelayMs, inCt).ConfigureAwait(false);
                }
                IReadOnlyList<DomainBar> tmpFiltered = m_Bars
                    .Where(b => b.Timestamp >= inFromUtc && b.Timestamp <= inToUtc)
                    .ToList();
                return tmpFiltered;
            });
        }

        /// <summary>Local re-declaration of <c>BarFetchKey</c> (the real
        /// one is internal to the production assembly). Same shape so the
        /// coalesce semantics match.</summary>
        private readonly record struct BarFetchKey(
            string Symbol, DateTime FromUtc, DateTime ToUtc, DomainBarTimeframe Timeframe);
    }

    private sealed class ThrowingQuotesProvider : IOptionQuotesProvider
    {
        public Task<OptionQuotesLookup> GetAtOrBeforeAsync(
            string inTicker, DateTime inTsUtc, CancellationToken inCt = default)
            => throw new NotSupportedException("Quotes path not exercised in this test.");
    }

    private sealed class CapturingStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Captured { get; } = new();
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            // Defensive copy via direct add — protobuf message refs are
            // mutable, but the handler emits new instances per event.
            Captured.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class TestServerCallContext : ServerCallContext
    {
        private readonly CancellationToken m_Ct;
        public TestServerCallContext(CancellationToken inCt) { m_Ct = inCt; }
        protected override string MethodCore => "/mbd.history.v1.HistoryService/EnsureRangeCached";
        protected override string HostCore => "localhost";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.UtcNow.AddSeconds(30);
        protected override Metadata RequestHeadersCore => new();
        protected override CancellationToken CancellationTokenCore => m_Ct;
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new("", new Dictionary<string, List<AuthProperty>>());
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }

    private static async Task ApplySchemaAsync(string inConnStr)
    {
        await using var tmpConn = new NpgsqlConnection(inConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            CREATE EXTENSION IF NOT EXISTS timescaledb;

            CREATE TABLE IF NOT EXISTS historical_bars (
              symbol VARCHAR(10) NOT NULL,
              timeframe VARCHAR(10) NOT NULL,
              timestamp TIMESTAMPTZ NOT NULL,
              open DECIMAL(18,4) NOT NULL,
              high DECIMAL(18,4) NOT NULL,
              low DECIMAL(18,4) NOT NULL,
              close DECIMAL(18,4) NOT NULL,
              volume DECIMAL(18,2) NOT NULL,
              vwap DECIMAL(18,4),
              trade_count INT
            );
            SELECT create_hypertable('historical_bars', 'timestamp', if_not_exists => TRUE);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bars_symbol_timeframe_timestamp
              ON historical_bars (symbol, timeframe, timestamp);

            CREATE TABLE IF NOT EXISTS historical_bars_misses (
              symbol      VARCHAR(10)  NOT NULL,
              timeframe   VARCHAR(10)  NOT NULL,
              range_from  TIMESTAMPTZ  NOT NULL,
              range_to    TIMESTAMPTZ  NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (symbol, timeframe, range_from, range_to)
            );
            """);
    }
}
