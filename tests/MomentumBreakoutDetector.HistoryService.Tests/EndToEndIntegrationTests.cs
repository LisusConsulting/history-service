using Dapper;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Observability;
using MomentumBreakoutDetector.HistoryService.Providers;
using NSubstitute;
using Npgsql;
using Refit;
using Shouldly;
using Testcontainers.PostgreSql;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.Models.Options;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;
using Xunit;
using DomainBar = MomentumBreakoutDetector.HistoryService.Domain.Bar;
using DomainBarTimeframe = MomentumBreakoutDetector.HistoryService.Domain.BarTimeframe;
// gRPC GetBars proto vs Polygon SDK GetBars (stocks) request — both
// types are visible via wildcard imports above; alias the proto one so
// every reference in this file is unambiguous.
using GetBarsRequest = MomentumBreakoutDetector.HistoryService.Contracts.V1.GetBarsRequest;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Phase 1, micro-PR #8 — comprehensive end-to-end integration coverage
/// for the history-service. Boots a single Testcontainers Postgres and
/// drives all four kinds (Bars, NBBO, Chains, Macro) through a fully
/// wired <see cref="HistoryServiceImpl"/> with a shared
/// <see cref="MetricsCollector"/>.
///
/// Coverage matches the µPR #8 brief:
///   1. Cold-start flow — empty DB → all 4 kinds populate → metrics
///      reflect upstream count.
///   2. Warm-cache flow — re-run same fetches → 0 upstream calls,
///      cache_hits incremented.
///   3. Coalesce proof — 50 concurrent point fetches → 1 upstream call.
///   4. Error handling — 5xx fail loud, 404 → miss-marker + cached.
///   5. GetCacheStats accuracy — counters match observed sequence.
/// </summary>
[Trait("Category", "Integration")]
public sealed class EndToEndIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer m_Pg = new PostgreSqlBuilder()
        .WithImage("timescale/timescaledb:latest-pg16")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    private string m_ConnStr = null!;

    public async Task InitializeAsync()
    {
        await m_Pg.StartAsync();
        m_ConnStr = m_Pg.GetConnectionString();
        await ApplyAllSchemaAsync(m_ConnStr);
    }

    public Task DisposeAsync() => m_Pg.DisposeAsync().AsTask();

    // ─────────────────────────────────────────────────────────────────
    // 1. Cold-start: all 4 kinds populate, metrics reflect upstream.
    // ─────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ColdStart_AllFourKinds_Populate_AndMetricsReflectUpstreamCount()
    {
        var tmpMetrics = new MetricsCollector();
        var tmpHarness = BuildHarness(tmpMetrics);

        var tmpFromTs = new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(4);

        // ---- Bars: cold-start triggers fetch.
        var tmpBarsResp = await tmpHarness.Service.GetBars(new GetBarsRequest
        {
            Symbol = "TSLA",
            Timeframe = BarTimeframe.Minute,
            FromTs = Timestamp.FromDateTime(tmpFromTs),
            ToTs = Timestamp.FromDateTime(tmpToTs),
        }, NewServerCallContext());

        tmpBarsResp.Bars.Count.ShouldBe(5);
        tmpBarsResp.CacheHit.ShouldBeFalse();

        // ---- NBBO: cold-start triggers fetch.
        var tmpQuoteTs = new DateTime(2026, 4, 15, 14, 0, 0, DateTimeKind.Utc);
        var tmpNbboResp = await tmpHarness.Service.GetNbbo(new GetNbboRequest
        {
            Ticker = "O:TSLA260418C00250000",
            Ts = Timestamp.FromDateTime(tmpQuoteTs),
        }, NewServerCallContext());

        tmpNbboResp.Quote.ShouldNotBeNull();
        tmpNbboResp.CacheHit.ShouldBeFalse();

        // ---- Chains: cold-start triggers a multi-page sweep (1 page here).
        var tmpAsOf = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc);
        var tmpChainResp = await tmpHarness.Service.GetOptionChain(new GetOptionChainRequest
        {
            UnderlyingTicker = "TSLA",
            AsOfDate = Timestamp.FromDateTime(tmpAsOf),
        }, NewServerCallContext());

        tmpChainResp.Contracts.Count.ShouldBe(2);
        tmpChainResp.CacheHit.ShouldBeFalse();

        // ---- Macro: cold-start triggers FRED fetch.
        var tmpFromDate = new DateOnly(2024, 4, 29);
        var tmpToDate = new DateOnly(2024, 5, 3);
        var tmpMacroResp = await tmpHarness.Service.GetMacro(new GetMacroRequest
        {
            SeriesId = "T10Y2Y",
            FromDate = Timestamp.FromDateTime(tmpFromDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc)),
            ToDate = Timestamp.FromDateTime(tmpToDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc)),
        }, NewServerCallContext());

        tmpMacroResp.Observations.Count.ShouldBeGreaterThanOrEqualTo(4);

        // ---- Snapshot via GetCacheStats — counters reflect 4 upstream
        //      fetches (one per kind; chains is one page = one wire call).
        var tmpStats = await tmpHarness.Service.GetCacheStats(
            new GetCacheStatsRequest(), NewServerCallContext());

        tmpStats.ClassStats.Count.ShouldBe(4);
        StatFor(tmpStats, DataClass.Bars).UpstreamFetches.ShouldBe(1L);
        StatFor(tmpStats, DataClass.Nbbo).UpstreamFetches.ShouldBe(1L);
        StatFor(tmpStats, DataClass.Chains).UpstreamFetches.ShouldBe(1L);
        StatFor(tmpStats, DataClass.Macro).UpstreamFetches.ShouldBe(1L);

        // No cache hits yet — every read was a cold fetch.
        StatFor(tmpStats, DataClass.Bars).CacheHits.ShouldBe(0L);
    }

    // ─────────────────────────────────────────────────────────────────
    // 2. Warm-cache: re-run = zero upstream, metrics show cache hits.
    // ─────────────────────────────────────────────────────────────────
    [Fact]
    public async Task WarmCache_SecondRun_ZeroUpstream_AndCacheHitsIncrement()
    {
        var tmpMetrics = new MetricsCollector();
        var tmpHarness = BuildHarness(tmpMetrics);

        var tmpFromTs = new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(4);

        var tmpReq = new GetBarsRequest
        {
            Symbol = "TSLA",
            Timeframe = BarTimeframe.Minute,
            FromTs = Timestamp.FromDateTime(tmpFromTs),
            ToTs = Timestamp.FromDateTime(tmpToTs),
        };

        // Cold call.
        await tmpHarness.Service.GetBars(tmpReq, NewServerCallContext());
        tmpHarness.BarFetcher.CallCount.ShouldBe(1);

        // Five warm calls — all served from cache.
        for (int i = 0; i < 5; i++)
        {
            var tmpResp = await tmpHarness.Service.GetBars(tmpReq, NewServerCallContext());
            tmpResp.CacheHit.ShouldBeTrue();
        }
        tmpHarness.BarFetcher.CallCount.ShouldBe(1, "warm path must not re-fetch");

        var tmpStats = await tmpHarness.Service.GetCacheStats(
            new GetCacheStatsRequest { DataClass = DataClass.Bars },
            NewServerCallContext());

        tmpStats.ClassStats.Count.ShouldBe(1);
        tmpStats.ClassStats[0].UpstreamFetches.ShouldBe(1L);
        tmpStats.ClassStats[0].CacheHits.ShouldBe(5L);
    }

    // ─────────────────────────────────────────────────────────────────
    // 3. Coalesce proof: N concurrent identical requests → 1 upstream.
    //    SingleFlight is fetcher-level so the proof is at the bar
    //    fetcher boundary — but we drive it via gRPC so the test
    //    mirrors a 50-thread cold-start backtest.
    // ─────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ConcurrentPointFetches_ColdStart_CoalesceTo_OneUpstreamCall()
    {
        var tmpMetrics = new MetricsCollector();
        var tmpHarness = BuildHarness(tmpMetrics);
        // Hold the first fetch open with an explicit gate so the test
        // doesn't depend on Task.Delay timing relative to the SingleFlight
        // window — slow-CI races otherwise let some callers slip through.
        tmpHarness.BarFetcher.HoldOpen();

        var tmpFromTs = new DateTime(2026, 4, 16, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(4);

        var tmpReq = new GetBarsRequest
        {
            Symbol = "TSLA",
            Timeframe = BarTimeframe.Minute,
            FromTs = Timestamp.FromDateTime(tmpFromTs),
            ToTs = Timestamp.FromDateTime(tmpToTs),
        };

        // Fire 50 concurrent GetBars; the gate keeps the first fetch in-
        // flight so all 50 callers reach SingleFlight before any return.
        var tmpTasks = Enumerable.Range(0, 50)
            .Select(_ => tmpHarness.Service.GetBars(tmpReq, NewServerCallContext()))
            .ToArray();

        // Give all 50 tasks a moment to enter the SingleFlight slot, then
        // release the gate so the coalesced fetch resolves.
        await Task.Delay(200);
        tmpHarness.BarFetcher.ReleaseGate();

        await Task.WhenAll(tmpTasks);

        tmpTasks.All(t => t.Result.Bars.Count == 5).ShouldBeTrue();

        // The fetcher's CallCount counts wire calls; SingleFlight
        // collapses 50 concurrent callers down to 1 — but the provider's
        // EnsureRangeCachedAsync runs its own per-call gap detection
        // BEFORE invoking the fetcher, so multiple concurrent gap probes
        // can each elect to call the fetcher. SingleFlight's job is to
        // fold those into one. The proof: stub call count is 1.
        tmpHarness.BarFetcher.CallCount.ShouldBe(1,
            "50 concurrent identical fetches must coalesce to 1 upstream call");

        var tmpStats = await tmpHarness.Service.GetCacheStats(
            new GetCacheStatsRequest { DataClass = DataClass.Bars },
            NewServerCallContext());
        tmpStats.ClassStats[0].UpstreamFetches.ShouldBe(1L);
    }

    // ─────────────────────────────────────────────────────────────────
    // 4a. Error handling: 5xx fails loud (no miss-marker, no cache).
    // ─────────────────────────────────────────────────────────────────
    [Fact]
    public async Task UpstreamFiveHundred_FailsLoud_AndDoesNotPoisonCache()
    {
        var tmpMetrics = new MetricsCollector();
        var tmpHarness = BuildHarness(tmpMetrics, fetchOutcome: FetchOutcome.ServerError);

        var tmpFromTs = new DateTime(2026, 4, 17, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(4);

        await Should.ThrowAsync<HttpRequestException>(async () =>
            await tmpHarness.Service.GetBars(new GetBarsRequest
            {
                Symbol = "TSLA",
                Timeframe = BarTimeframe.Minute,
                FromTs = Timestamp.FromDateTime(tmpFromTs),
                ToTs = Timestamp.FromDateTime(tmpToTs),
            }, NewServerCallContext()));

        // Failed wire call is still an upstream fetch — but counter is
        // recorded inside the success path of the fetcher, after the
        // pipeline returns. A 5xx throws before that point, so we don't
        // count it here. What matters is that the cache wasn't poisoned.
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        var tmpRowCount = await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_bars WHERE symbol = 'TSLA'");
        tmpRowCount.ShouldBe(0L);
        var tmpMissCount = await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_bars_misses WHERE symbol = 'TSLA'");
        tmpMissCount.ShouldBe(0L,
            "5xx must NOT write a miss-marker — that would poison the cache");
    }

    // ─────────────────────────────────────────────────────────────────
    // 4b. Error handling: 404 → miss-marker + cached on subsequent reads.
    // ─────────────────────────────────────────────────────────────────
    [Fact]
    public async Task UpstreamFourOhFour_WritesMissMarker_AndIsCachedOnSecondRead()
    {
        var tmpMetrics = new MetricsCollector();
        var tmpHarness = BuildHarness(tmpMetrics, fetchOutcome: FetchOutcome.NotFound);

        var tmpFromTs = new DateTime(2026, 4, 18, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(4);

        var tmpReq = new GetBarsRequest
        {
            Symbol = "TSLA",
            Timeframe = BarTimeframe.Minute,
            FromTs = Timestamp.FromDateTime(tmpFromTs),
            ToTs = Timestamp.FromDateTime(tmpToTs),
        };

        // First call: 404 → empty result, miss-marker written.
        var tmpFirst = await tmpHarness.Service.GetBars(tmpReq, NewServerCallContext());
        tmpFirst.Bars.Count.ShouldBe(0);

        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        var tmpMissCount = await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_bars_misses WHERE symbol = 'TSLA' AND timeframe = '1min'");
        tmpMissCount.ShouldBe(1L);

        // Second call: marker covers range → no Polygon fetch.
        var tmpSecond = await tmpHarness.Service.GetBars(tmpReq, NewServerCallContext());
        tmpSecond.Bars.Count.ShouldBe(0);
        tmpHarness.BarFetcher.CallCount.ShouldBe(1, "miss-marker must short-circuit re-fetch");

        var tmpStats = await tmpHarness.Service.GetCacheStats(
            new GetCacheStatsRequest { DataClass = DataClass.Bars },
            NewServerCallContext());
        tmpStats.ClassStats[0].MissMarkers.ShouldBe(1L,
            "miss-marker counter must increment when a marker is written");
    }

    // ─────────────────────────────────────────────────────────────────
    // 5. GetCacheStats accuracy: known sequence → exact counters.
    // ─────────────────────────────────────────────────────────────────
    [Fact]
    public async Task GetCacheStats_AfterKnownSequence_MatchesExpectedCounters()
    {
        var tmpMetrics = new MetricsCollector();
        var tmpHarness = BuildHarness(tmpMetrics);

        var tmpFromTs = new DateTime(2026, 4, 19, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(4);
        var tmpReq = new GetBarsRequest
        {
            Symbol = "TSLA",
            Timeframe = BarTimeframe.Minute,
            FromTs = Timestamp.FromDateTime(tmpFromTs),
            ToTs = Timestamp.FromDateTime(tmpToTs),
        };

        // Sequence: 1 cold + 3 warm. Expected: upstream=1, hits=3.
        await tmpHarness.Service.GetBars(tmpReq, NewServerCallContext());
        await tmpHarness.Service.GetBars(tmpReq, NewServerCallContext());
        await tmpHarness.Service.GetBars(tmpReq, NewServerCallContext());
        await tmpHarness.Service.GetBars(tmpReq, NewServerCallContext());

        var tmpStats = await tmpHarness.Service.GetCacheStats(
            new GetCacheStatsRequest(), NewServerCallContext());

        StatFor(tmpStats, DataClass.Bars).UpstreamFetches.ShouldBe(1L);
        StatFor(tmpStats, DataClass.Bars).CacheHits.ShouldBe(3L);
        StatFor(tmpStats, DataClass.Bars).TotalRequests.ShouldBe(4L);
        StatFor(tmpStats, DataClass.Bars).LatencyP50Ms.ShouldBeGreaterThanOrEqualTo(0d);
        StatFor(tmpStats, DataClass.Bars).InFlightCount.ShouldBe(0,
            "no fetches in flight after the sequence completes");

        // AsOf timestamp is set; sanity-check it's recent.
        var tmpAsOf = tmpStats.AsOf.ToDateTime();
        (DateTime.UtcNow - tmpAsOf).TotalSeconds.ShouldBeLessThan(60);
    }

    // ─────────────────────────────────────────────────────────────────
    // Harness — wires all 4 providers + their stubbed fetchers together.
    // ─────────────────────────────────────────────────────────────────
    private TestHarness BuildHarness(
        MetricsCollector inMetrics,
        bool slowFetch = false,
        FetchOutcome fetchOutcome = FetchOutcome.Ok)
    {
        // ---- Bars ----
        // The configurable stub stands in for PolygonBarFetcher (which is
        // where the production metric lands). To keep the integration
        // test's metrics behaviour faithful to production, the stub also
        // records UpstreamFetches + miss-marker on its wire-call analogue.
        var tmpBarFetcher = new ConfigurableBarFetcher(slowFetch, fetchOutcome, inMetrics);
        var tmpBarsProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpBarFetcher, NullLogger<HistoricalBarsProvider>.Instance, inMetrics);

        // ---- Quotes / NBBO ----
        var tmpStubOptions = Substitute.For<IOptionsService>();
        tmpStubOptions
            .GetQuotesAsync(Arg.Any<GetQuotesRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult<TreyThomasCodes.Polygon.Models.Common.PolygonResponse<List<TreyThomasCodes.Polygon.Models.Options.OptionQuote>>?>(
                new TreyThomasCodes.Polygon.Models.Common.PolygonResponse<List<TreyThomasCodes.Polygon.Models.Options.OptionQuote>>
                {
                    Status = "OK",
                    Results = new List<TreyThomasCodes.Polygon.Models.Options.OptionQuote>
                    {
                        new()
                        {
                            BidPrice = 1.20m,
                            AskPrice = 1.25m,
                            BidSize = 10,
                            AskSize = 12,
                            BidExchange = 1,
                            AskExchange = 2,
                            // SIP timestamp is nanoseconds since epoch.
                            SipTimestamp = (long)(new DateTime(2026, 4, 15, 14, 0, 0, DateTimeKind.Utc)
                                - DateTime.UnixEpoch).TotalMilliseconds * 1_000_000L,
                        },
                    },
                }));
        var tmpNbboFetcher = new PolygonNbboFetcher(
            tmpStubOptions, NullLogger<PolygonNbboFetcher>.Instance, inMetrics);
        var tmpNbboMem = new NbboMemoryCache();
        var tmpQuotesProvider = new OptionQuotesProvider(
            tmpNbboMem, tmpNbboFetcher,
            Options.Create(new HistoryServiceOptions { ConnectionString = m_ConnStr }),
            NullLogger<OptionQuotesProvider>.Instance, inMetrics);

        // ---- Chains ----
        tmpStubOptions
            .GetListContractsRawAsync(Arg.Any<GetListContractsRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(MakeChainOkResponse(new PolygonResponse<List<OptionsContract>>
            {
                Status = "OK",
                Results = new List<OptionsContract>
                {
                    new()
                    {
                        Ticker = "O:TSLA260418C00250000",
                        UnderlyingTicker = "TSLA",
                        ContractType = "call",
                        ExerciseStyle = "american",
                        ExpirationDate = "2026-04-18",
                        StrikePrice = 250m,
                        SharesPerContract = 100,
                        PrimaryExchange = "BATO",
                    },
                    new()
                    {
                        Ticker = "O:TSLA260418P00240000",
                        UnderlyingTicker = "TSLA",
                        ContractType = "put",
                        ExerciseStyle = "american",
                        ExpirationDate = "2026-04-18",
                        StrikePrice = 240m,
                        SharesPerContract = 100,
                        PrimaryExchange = "BATO",
                    },
                },
                NextUrl = null,
            })));
        var tmpChainFetcher = new PolygonChainFetcher(
            tmpStubOptions, NullLogger<PolygonChainFetcher>.Instance, inMetrics);
        var tmpChainProvider = new OptionChainProvider(
            Options.Create(new HistoryServiceOptions { ConnectionString = m_ConnStr }),
            NullLogger<OptionChainProvider>.Instance, tmpChainFetcher, inMetrics);

        // ---- Macro ----
        var tmpFred = new StubFredFetcher(inMetrics);
        tmpFred.Responses["T10Y2Y"] = new List<FredObservationRow>
        {
            new("T10Y2Y", new DateOnly(2024, 4, 29), -0.34m),
            new("T10Y2Y", new DateOnly(2024, 4, 30), -0.32m),
            new("T10Y2Y", new DateOnly(2024, 5, 1), -0.30m),
            new("T10Y2Y", new DateOnly(2024, 5, 2), -0.29m),
            new("T10Y2Y", new DateOnly(2024, 5, 3), -0.28m),
        };
        var tmpMacroProvider = new MacroDataProvider(
            m_ConnStr, NullLogger<MacroDataProvider>.Instance, tmpFred, inMetrics);

        var tmpService = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            tmpBarsProvider,
            tmpQuotesProvider,
            tmpMacroProvider,
            tmpChainProvider,
            inMetrics);

        return new TestHarness(tmpService, tmpBarFetcher, tmpFred);
    }

    private static ApiResponse<PolygonResponse<List<OptionsContract>>> MakeChainOkResponse(
        PolygonResponse<List<OptionsContract>> inBody)
    {
        var tmpHttp = new HttpResponseMessage(System.Net.HttpStatusCode.OK)
        {
            RequestMessage = new HttpRequestMessage(
                HttpMethod.Get,
                new Uri("https://api.polygon.io/v3/reference/options/contracts")),
        };
        return new ApiResponse<PolygonResponse<List<OptionsContract>>>(
            tmpHttp, inBody, new RefitSettings(), error: null);
    }

    private sealed record TestHarness(
        HistoryServiceImpl Service,
        ConfigurableBarFetcher BarFetcher,
        StubFredFetcher Fred);

    private static ClassStats StatFor(GetCacheStatsResponse inResp, DataClass inClass)
        => inResp.ClassStats.First(c => c.DataClass == inClass);

    // ─────────────────────────────────────────────────────────────────
    // Stubs — bar fetcher with configurable outcome / latency.
    // ─────────────────────────────────────────────────────────────────
    private enum FetchOutcome { Ok, NotFound, ServerError }

    private sealed class ConfigurableBarFetcher : IPolygonBarFetcher
    {
        private readonly bool m_SlowFetch;
        private readonly FetchOutcome m_Outcome;
        private readonly MetricsCollector? m_Metrics;
        // SingleFlight collapses 50 concurrent identical requests into
        // one — the production PolygonBarFetcher does this internally.
        // The stub mirrors that behaviour so the coalesce-proof test
        // sees the same single-call result a real backtest would.
        private readonly MomentumBreakoutDetector.HistoryService.Concurrency.SingleFlight<
            (string, DateTime, DateTime, DomainBarTimeframe), IReadOnlyList<DomainBar>> m_Coalescer = new();
        public int CallCount;

        // Optional gate — when set, the fetch awaits the gate before
        // returning. Lets the coalesce-proof test ensure every concurrent
        // caller has arrived in SingleFlight before the first fetch
        // completes (avoids slow-CI races where the 100ms Task.Delay
        // window isn't wide enough to fold all 50 callers).
        private TaskCompletionSource? m_Gate;

        public ConfigurableBarFetcher(bool slowFetch, FetchOutcome outcome, MetricsCollector? metrics = null)
        {
            m_SlowFetch = slowFetch;
            m_Outcome = outcome;
            m_Metrics = metrics;
        }

        /// <summary>
        /// Block the next fetch until <see cref="ReleaseGate"/> is called.
        /// Use to deterministically hold SingleFlight open while the test
        /// queues N concurrent waiters.
        /// </summary>
        public void HoldOpen()
        {
            m_Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void ReleaseGate() => m_Gate?.TrySetResult();

        public Task<IReadOnlyList<DomainBar>> FetchBarsAsync(
            string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            DomainBarTimeframe inTimeframe, CancellationToken inCt)
        {
            var tmpKey = (inSymbol, inFromUtc, inToUtc, inTimeframe);
            return m_Coalescer.ExecuteAsync(tmpKey,
                () => DoFetchAsync(inSymbol, inFromUtc, inToUtc, inTimeframe, inCt));
        }

        private async Task<IReadOnlyList<DomainBar>> DoFetchAsync(
            string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            DomainBarTimeframe inTimeframe, CancellationToken inCt)
        {
            var tmpStart = System.Diagnostics.Stopwatch.GetTimestamp();
            Interlocked.Increment(ref CallCount);
            if (m_Gate is not null) await m_Gate.Task;
            if (m_SlowFetch) await Task.Delay(100, inCt);

            switch (m_Outcome)
            {
                case FetchOutcome.NotFound:
                    m_Metrics?.RecordUpstreamFetch(MetricKind.Bars,
                        ElapsedMs(tmpStart));
                    return Array.Empty<DomainBar>();
                case FetchOutcome.ServerError:
                    throw new HttpRequestException("simulated 5xx");
                case FetchOutcome.Ok:
                default:
                    var tmpBars = new List<DomainBar>();
                    for (int i = 0; i < 5; i++)
                    {
                        var ts = inFromUtc.AddMinutes(i);
                        if (ts > inToUtc) break;
                        tmpBars.Add(new DomainBar("TSLA", ts,
                            250m + i, 251m + i, 249m + i, 250.5m + i, 1000m, 250.25m + i));
                    }
                    m_Metrics?.RecordUpstreamFetch(MetricKind.Bars,
                        ElapsedMs(tmpStart));
                    return tmpBars;
            }
        }

        private static double ElapsedMs(long inStart)
            => (System.Diagnostics.Stopwatch.GetTimestamp() - inStart)
               * 1000d / System.Diagnostics.Stopwatch.Frequency;
    }

    private sealed class StubFredFetcher : IFredFetcher
    {
        private readonly MetricsCollector? m_Metrics;
        public Dictionary<string, List<FredObservationRow>> Responses { get; } = new();
        public int CallCount;

        public StubFredFetcher(MetricsCollector? metrics = null)
        {
            m_Metrics = metrics;
        }

        public Task<IReadOnlyList<FredObservationRow>> FetchSeriesAsync(
            string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            // Mirror production FredFetcher's metric instrumentation so
            // assertions in the integration tests reflect a faithful
            // request → upstream count.
            m_Metrics?.RecordUpstreamFetch(MetricKind.Macro, 1.0);
            return Task.FromResult<IReadOnlyList<FredObservationRow>>(
                Responses.TryGetValue(seriesId, out var rows)
                    ? rows
                    : Array.Empty<FredObservationRow>());
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Test ServerCallContext — same minimal harness as smoke tests.
    // ─────────────────────────────────────────────────────────────────
    private static ServerCallContext NewServerCallContext()
        => new TestServerCallContext(CancellationToken.None);

    private sealed class TestServerCallContext : ServerCallContext
    {
        private readonly CancellationToken m_Ct;
        public TestServerCallContext(CancellationToken inCt) { m_Ct = inCt; }

        protected override string MethodCore => "/mbd.history.v1.HistoryService/Test";
        protected override string HostCore => "localhost";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.UtcNow.AddSeconds(60);
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

    // ─────────────────────────────────────────────────────────────────
    // Schema — applies all four kinds' DDL for the integration suite.
    // ─────────────────────────────────────────────────────────────────
    private static async Task ApplyAllSchemaAsync(string inConnStr)
    {
        await using var tmpConn = new NpgsqlConnection(inConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            CREATE EXTENSION IF NOT EXISTS timescaledb;

            -- Bars
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

            -- NBBO
            CREATE TABLE IF NOT EXISTS historical_options_quotes (
              ticker        VARCHAR(50)  NOT NULL,
              ts            TIMESTAMPTZ  NOT NULL,
              as_of_ts      TIMESTAMPTZ,
              bid_price     NUMERIC(18,4),
              ask_price     NUMERIC(18,4),
              bid_size      INT,
              ask_size      INT,
              bid_exchange  INT,
              ask_exchange  INT,
              fetched_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (ticker, ts)
            );
            CREATE TABLE IF NOT EXISTS historical_options_quotes_misses (
              ticker      VARCHAR(50)  NOT NULL,
              ts          TIMESTAMPTZ  NOT NULL,
              reason      TEXT,
              recorded_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (ticker, ts)
            );

            -- Chains
            CREATE TABLE IF NOT EXISTS historical_options_contracts (
              as_of_date DATE NOT NULL,
              ticker VARCHAR(50) NOT NULL,
              underlying_ticker VARCHAR(10) NOT NULL,
              contract_type VARCHAR(10),
              exercise_style VARCHAR(20),
              expiration_date DATE,
              strike_price DECIMAL(18,4),
              shares_per_contract INT,
              primary_exchange VARCHAR(10)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_options_date_ticker
              ON historical_options_contracts (as_of_date, ticker);
            CREATE TABLE IF NOT EXISTS historical_options_chains_misses (
              symbol      VARCHAR(10)  NOT NULL,
              as_of_date  DATE         NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (symbol, as_of_date)
            );

            -- Macro
            CREATE TABLE IF NOT EXISTS macro_data (
              series_id VARCHAR(20) NOT NULL,
              observation_date DATE NOT NULL,
              value DECIMAL(18,6)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_series_date
              ON macro_data (series_id, observation_date);
            CREATE TABLE IF NOT EXISTS macro_data_misses (
              series_id        VARCHAR(20)  NOT NULL,
              observation_date DATE         NOT NULL,
              reason           TEXT,
              fetched_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (series_id, observation_date)
            );
            """);
    }
}
