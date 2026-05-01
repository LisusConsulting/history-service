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
/// Phase 1, micro-PR #2 — single end-to-end test that proves the lift
/// works:
///   - Spins up a TimescaleDB postgres in a container.
///   - Applies the bars + miss-marker schema.
///   - Wires <see cref="HistoricalBarsProvider"/> with a stubbed
///     <see cref="IPolygonBarFetcher"/> that returns 5 known bars.
///   - Calls <see cref="HistoryServiceImpl.GetBars"/> directly (no
///     real gRPC channel — server-side method invocation only).
///   - First call: cold cache → fetches via stub → cache_hit = false,
///     5 bars returned.
///   - Second call: warm cache → cache_hit = true, same 5 bars.
///
/// Comprehensive integration coverage lands in micro-PR #8.
/// </summary>
public sealed class GetBarsSmokeTests : IAsyncLifetime
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
    public async Task GetBars_ColdCache_FetchesFromUpstream_ThenServesFromCacheOnSecondCall()
    {
        // Arrange — 5 known 1-min bars covering 13:30-13:34 UTC on 2026-04-15.
        var tmpDay = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc);
        var tmpFromTs = tmpDay.AddHours(13).AddMinutes(30); // 13:30 UTC
        var tmpToTs = tmpDay.AddHours(13).AddMinutes(34);   // 13:34 UTC inclusive

        var tmpStubBars = new List<DomainBar>
        {
            new("TSLA", tmpFromTs.AddMinutes(0), 250.10m, 250.50m, 250.00m, 250.30m, 1000m, 250.20m),
            new("TSLA", tmpFromTs.AddMinutes(1), 250.30m, 250.70m, 250.20m, 250.60m, 1100m, 250.45m),
            new("TSLA", tmpFromTs.AddMinutes(2), 250.60m, 250.90m, 250.50m, 250.80m, 1200m, 250.70m),
            new("TSLA", tmpFromTs.AddMinutes(3), 250.80m, 251.10m, 250.70m, 251.00m, 1300m, 250.90m),
            new("TSLA", tmpFromTs.AddMinutes(4), 251.00m, 251.30m, 250.90m, 251.20m, 1400m, 251.10m),
        };

        var tmpStubFetcher = new StubBarFetcher(tmpStubBars);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnectionString,
            tmpStubFetcher,
            NullLogger<HistoricalBarsProvider>.Instance);

        // HistoryServiceImpl ctor takes the quotes provider (PR #3) and
        // an optional macro provider (PR #5) alongside the bars provider.
        // We don't exercise GetNbbo or GetMacro here, so a non-null
        // quotes stub + null macro is the minimum needed to construct.
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            tmpProvider,
            quotes: new NullQuotesProvider(),
            macroProvider: null);

        var tmpRequest = new GetBarsRequest
        {
            Symbol = "TSLA",
            Timeframe = BarTimeframe.Minute,
            FromTs = Timestamp.FromDateTime(tmpFromTs),
            ToTs = Timestamp.FromDateTime(tmpToTs),
        };

        // Act — first call (cold cache).
        var tmpFirst = await tmpImpl.GetBars(tmpRequest, NewServerCallContext());

        // Assert — 5 bars, cache_hit=false, stub was called once.
        tmpFirst.Bars.Count.ShouldBe(5);
        tmpFirst.CacheHit.ShouldBeFalse();
        tmpStubFetcher.CallCount.ShouldBe(1);

        // Verify the proto fields round-tripped intact on the first bar.
        var tmpProtoFirst = tmpFirst.Bars[0];
        tmpProtoFirst.Symbol.ShouldBe("TSLA");
        tmpProtoFirst.Open.ShouldBe(250.10, 0.001);
        tmpProtoFirst.Close.ShouldBe(250.30, 0.001);
        tmpProtoFirst.Volume.ShouldBe(1000, 0.001);
        tmpProtoFirst.Vwap.ShouldBe(250.20, 0.001);
        tmpProtoFirst.Timestamp.ToDateTime().ShouldBe(tmpFromTs);

        // Act — second call (warm cache).
        var tmpSecond = await tmpImpl.GetBars(tmpRequest, NewServerCallContext());

        // Assert — same 5 bars, cache_hit=true, stub was NOT called again.
        tmpSecond.Bars.Count.ShouldBe(5);
        tmpSecond.CacheHit.ShouldBeTrue();
        tmpStubFetcher.CallCount.ShouldBe(1); // unchanged from first call
    }

    private static ServerCallContext NewServerCallContext()
        => new TestServerCallContext(CancellationToken.None);

    /// <summary>
    /// Minimal hand-rolled <see cref="ServerCallContext"/> for direct
    /// in-process invocation of a gRPC service method. We don't pull in
    /// Grpc.Core.Testing for one test — the surface we need
    /// (CancellationToken + Method) is small enough to inline.
    /// </summary>
    private sealed class TestServerCallContext : ServerCallContext
    {
        private readonly CancellationToken m_Ct;
        public TestServerCallContext(CancellationToken inCt) { m_Ct = inCt; }

        protected override string MethodCore => "/mbd.history.v1.HistoryService/GetBars";
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
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders)
            => Task.CompletedTask;
    }

    /// <summary>
    /// Apply the bars + miss-marker schema. We replicate the migration
    /// SQL inline rather than mounting the .sql files so the test
    /// project doesn't need a content-include item — keeps the lift
    /// hermetic.
    /// </summary>
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

    /// <summary>
    /// Minimal IOptionQuotesProvider stub. The bars test never invokes
    /// GetNbbo, but HistoryServiceImpl's ctor requires a non-null quotes
    /// provider (PR #3 — co-occurrent lift), so we satisfy that
    /// contract with a no-op that throws if anyone ever calls it.
    /// </summary>
    private sealed class NullQuotesProvider : IOptionQuotesProvider
    {
        public Task<OptionQuotesLookup> GetAtOrBeforeAsync(
            string inTicker, DateTime inTsUtc, CancellationToken inCt = default)
            => throw new NotSupportedException(
                "GetBarsSmokeTests should not exercise the NBBO path.");
    }

    /// <summary>
    /// In-memory <see cref="IPolygonBarFetcher"/> that returns a fixed
    /// bar set on every call within the requested range and tracks how
    /// many times it was invoked.
    /// </summary>
    private sealed class StubBarFetcher : IPolygonBarFetcher
    {
        private readonly IReadOnlyList<DomainBar> m_Bars;
        public int CallCount { get; private set; }

        public StubBarFetcher(IReadOnlyList<DomainBar> inBars)
        {
            m_Bars = inBars;
        }

        public Task<IReadOnlyList<DomainBar>> FetchBarsAsync(
            string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            DomainBarTimeframe inTimeframe, CancellationToken inCt)
        {
            CallCount++;
            // Filter to the requested range so the test mirrors what
            // real Polygon would do.
            var tmpFiltered = m_Bars
                .Where(b => b.Timestamp >= inFromUtc && b.Timestamp <= inToUtc)
                .ToList();
            return Task.FromResult<IReadOnlyList<DomainBar>>(tmpFiltered);
        }
    }
}
