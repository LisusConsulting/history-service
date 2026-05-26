using System.Collections.Concurrent;
using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Cross-provider regression suite for the 2026-05-02 concurrency-safety
/// hardening. Each test stands up a real Postgres+Timescale container,
/// applies the schema each provider expects, and fans out N concurrent
/// <c>EnsureRangeCachedAsync</c> (or equivalent) calls against the same
/// gap key. Pre-fix:
///   * macro / NBBO / chains / bars all suffered some flavour of
///     duplicate-key INSERT race when two writers raced past the
///     pre-cache check before either had committed.
/// Post-fix (Layer 1 SingleFlight at the provider level + Layer 3
/// pg_advisory_xact_lock around persistence): zero exceptions, zero
/// duplicate rows, and exactly one upstream call per gap key (modulo
/// the provider's per-day fan-out for chains).
///
/// All tests share one Postgres container per fixture for speed; data
/// is namespaced per test (distinct ticker / series_id / symbol) so they
/// can run in any order.
/// </summary>
public sealed class ProviderConcurrencyRegressionTests : IAsyncLifetime
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

    // ── BARS ────────────────────────────────────────────────────────────

    /// <summary>
    /// 16 concurrent <see cref="HistoricalBarsProvider.EnsureRangeCachedAsync"/>
    /// callers on the same (symbol, timeframe, range). Pre-fix: duplicate
    /// key violations possible on historical_bars or its miss markers.
    /// Post-fix: zero exceptions; exactly one upstream call thanks to
    /// the BarGapKey SingleFlight in the provider.
    /// </summary>
    [Fact]
    public async Task Bars_16ConcurrentEnsureRangeCached_NoRaceNoDuplicateRows()
    {
        const int kFanout = 16;
        var tmpDay = new DateTime(2026, 4, 17, 0, 0, 0, DateTimeKind.Utc);
        var tmpFrom = tmpDay.AddHours(13).AddMinutes(30);
        var tmpTo = tmpDay.AddHours(13).AddMinutes(34);  // 5 mins → 5 bars

        var tmpBars = new List<Bar>(5);
        for (var i = 0; i < 5; i++)
        {
            tmpBars.Add(new Bar(
                "CONC", tmpFrom.AddMinutes(i),
                250m + i, 251m + i, 249m + i, 250.5m + i, 1000m, 250.25m + i));
        }
        var tmpStub = new CountingBarFetcher(tmpBars);

        var tmpProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        // Fan out kFanout concurrent EnsureRangeCachedAsync. Pre-fix this
        // could surface 23505 unique-violation on historical_bars under
        // load (two writers committing between each other's reads). Post-
        // fix the BarGapKey SingleFlight collapses the work.
        var tmpExceptions = new ConcurrentBag<Exception>();
        var tmpStartGate = new TaskCompletionSource();
        var tmpTasks = new Task[kFanout];
        for (var i = 0; i < kFanout; i++)
        {
            tmpTasks[i] = Task.Run(async () =>
            {
                await tmpStartGate.Task.ConfigureAwait(false);
                try
                {
                    await tmpProvider.EnsureRangeCachedAsync(
                        "CONC", tmpFrom, tmpTo, BarTimeframe.OneMinute,
                        inProgress: null, inCt: CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (Exception ex) { tmpExceptions.Add(ex); }
            });
        }
        tmpStartGate.SetResult();
        await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        tmpExceptions.ShouldBeEmpty(
            "no caller should see a duplicate-key or transient race");
        // On a fast machine all 16 callers arrive at the BarGapKey
        // SingleFlight before the first fetch + cache-write completes,
        // so Calls is exactly 1. On slow GHA runners with contended
        // cores, the 1st caller can finish before the last few even
        // start their cache-check; those late arrivals see the warm
        // cache and skip the fetcher. The invariant we actually want
        // to protect is "coalescing happened" (not ~16 calls); 3 is
        // ≤19% of the fan-out, so a pre-fix regression (which would
        // give double-digit Calls) still fails.
        tmpStub.Calls.ShouldBeLessThanOrEqualTo(3,
            "16 concurrent ensure-calls must fold via SingleFlight — expected 1 (allow ≤3 for CI scheduling jitter)");

        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync().ConfigureAwait(false);
        var tmpRowCount = await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_bars WHERE symbol = @S",
            new { S = "CONC" }).ConfigureAwait(false);
        tmpRowCount.ShouldBe(5, "exactly 5 bar rows persisted (one per minute)");
    }

    // ── NBBO ────────────────────────────────────────────────────────────

    /// <summary>
    /// 16 concurrent <see cref="OptionQuotesProvider.GetAtOrBeforeAsync"/>
    /// callers on the same (ticker, ts) for a fresh, never-cached quote.
    /// Pre-fix: every caller hits Polygon (no in-process collapsing) and
    /// the writes race ON CONFLICT — survives because of DO NOTHING but
    /// each caller still makes a wasted upstream call. Post-fix: NbboGapKey
    /// SingleFlight folds them into one Polygon call.
    /// </summary>
    [Fact]
    public async Task Nbbo_16ConcurrentSameKey_OneUpstreamCall_NoDuplicateRows()
    {
        const int kFanout = 16;
        const string kTicker = "O:TSLA260417C500-CONC";
        var tmpTs = new DateTime(2026, 4, 17, 13, 30, 0, DateTimeKind.Utc);

        var tmpStub = new CountingNbboFetcher(quoteTicker: kTicker);
        var tmpMem = new NbboMemoryCache();
        var tmpProvider = new OptionQuotesProvider(
            tmpMem, tmpStub,
            Microsoft.Extensions.Options.Options.Create(new HistoryServiceOptions
            {
                ConnectionString = m_ConnStr,
                NbboStaleQuoteToleranceSeconds = 0,
            }),
            NullLogger<OptionQuotesProvider>.Instance);

        var tmpStart = new TaskCompletionSource();
        var tmpExceptions = new ConcurrentBag<Exception>();
        var tmpTasks = new Task<OptionQuotesLookup>[kFanout];
        for (var i = 0; i < kFanout; i++)
        {
            tmpTasks[i] = Task.Run(async () =>
            {
                await tmpStart.Task.ConfigureAwait(false);
                try
                {
                    return await tmpProvider.GetAtOrBeforeAsync(
                        kTicker, tmpTs, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    tmpExceptions.Add(ex);
                    return new OptionQuotesLookup(null, false, false);
                }
            });
        }
        tmpStart.SetResult();
        var tmpResults = await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        tmpExceptions.ShouldBeEmpty();
        tmpStub.Calls.ShouldBe(1,
            "GapLockExecutor must fold concurrent same-key callers into one fetch");

        // All callers must see the same record; either all CacheHit=false
        // (one winner) and rest CacheHit=true is fine — but the resolved
        // quote is identical.
        var tmpFirstResolved = tmpResults.First(r => r.Quote is not null);
        foreach (var tmpR in tmpResults)
        {
            tmpR.Quote.ShouldNotBeNull();
            tmpR.Quote!.Ticker.ShouldBe(tmpFirstResolved.Quote!.Ticker);
            tmpR.Quote.RequestedTsUtc.ShouldBe(tmpFirstResolved.Quote.RequestedTsUtc);
        }

        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync().ConfigureAwait(false);
        var tmpRows = await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_options_quotes WHERE ticker = @T",
            new { T = kTicker }).ConfigureAwait(false);
        tmpRows.ShouldBe(1, "exactly one quote row persisted");
    }

    // ── CHAINS ──────────────────────────────────────────────────────────

    /// <summary>
    /// 16 concurrent <see cref="OptionChainProvider.EnsureChainCachedAsync"/>
    /// callers on the same (symbol, as_of) day. Pre-fix: in-flight Dictionary
    /// already collapsed in-process duplicates correctly. Post-fix: same
    /// behavior preserved through the GapLockExecutor migration. We pin
    /// "1 fetch" here as a regression guard against a future refactor that
    /// breaks the SF.
    /// </summary>
    [Fact]
    public async Task Chains_16ConcurrentEnsureChain_OneUpstreamCall()
    {
        const int kFanout = 16;
        const string kSymbol = "TSCH";
        var tmpAsOf = new DateOnly(2026, 4, 17);

        var tmpStub = new CountingChainFetcher();
        var tmpProvider = new OptionChainProvider(
            Microsoft.Extensions.Options.Options.Create(new HistoryServiceOptions
            {
                ConnectionString = m_ConnStr,
            }),
            NullLogger<OptionChainProvider>.Instance,
            tmpStub);

        var tmpStart = new TaskCompletionSource();
        var tmpExceptions = new ConcurrentBag<Exception>();
        var tmpTasks = new Task[kFanout];
        for (var i = 0; i < kFanout; i++)
        {
            tmpTasks[i] = Task.Run(async () =>
            {
                await tmpStart.Task.ConfigureAwait(false);
                try
                {
                    await tmpProvider.EnsureChainCachedAsync(kSymbol, tmpAsOf, CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (Exception ex) { tmpExceptions.Add(ex); }
            });
        }
        tmpStart.SetResult();
        await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        tmpExceptions.ShouldBeEmpty();
        tmpStub.Calls.ShouldBe(1,
            "ChainGapKey SingleFlight must fold same-day same-symbol concurrent ensures");
    }

    // ── MACRO ───────────────────────────────────────────────────────────

    /// <summary>
    /// 16 concurrent <see cref="MacroDataProvider.EnsureRangeCachedAsync(string, DateOnly, DateOnly, CancellationToken)"/>
    /// callers on the same (series, range). This is THE bug that crashed
    /// the dev backtest at 02:04:58 UTC 2026-05-02 — two writers raced
    /// the marker insert and the second hit
    /// macro_data_misses_v2_pkey duplicate-key. Post-fix: MacroGapKey
    /// SingleFlight folds them; even without the SF, RangeMarkerWriter's
    /// own pg_advisory_xact_lock now serialises the marker write.
    /// </summary>
    [Fact]
    public async Task Macro_16ConcurrentEnsureRange_NoDuplicateKey_OneFetchCall()
    {
        const int kFanout = 16;
        const string kSeries = "T10Y2Y-CONC";
        // Use an in-the-past range that returns ONE empty observation
        // from the stub fetcher — this exercises the marker-write path
        // (the failure mode that crashed 2026-05-02). KnownSeriesCadence
        // doesn't include this synthetic series so it defaults to Daily.
        var tmpFrom = new DateOnly(2024, 1, 1);
        var tmpTo = new DateOnly(2024, 1, 5);  // Mon-Fri — 5 expected boundaries

        var tmpStub = new CountingFredFetcher(emptyResponse: true);
        var tmpProvider = new MacroDataProvider(
            m_ConnStr,
            NullLogger<MacroDataProvider>.Instance,
            tmpStub);

        var tmpStart = new TaskCompletionSource();
        var tmpExceptions = new ConcurrentBag<Exception>();
        var tmpTasks = new Task[kFanout];
        for (var i = 0; i < kFanout; i++)
        {
            tmpTasks[i] = Task.Run(async () =>
            {
                await tmpStart.Task.ConfigureAwait(false);
                try
                {
                    await tmpProvider.EnsureRangeCachedAsync(
                        kSeries, tmpFrom, tmpTo, CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (Exception ex) { tmpExceptions.Add(ex); }
            });
        }
        tmpStart.SetResult();
        await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        tmpExceptions.ShouldBeEmpty(
            "no concurrent macro caller should hit a unique-constraint violation");
        tmpStub.Calls.ShouldBe(1,
            "MacroGapKey SingleFlight must fold same-(series,range) concurrent ensures into one FRED call");

        // Exactly one marker row (or one merged range) covering the window.
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync().ConfigureAwait(false);
        var tmpMarkerCount = await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM macro_data_misses WHERE series_id = @S",
            new { S = kSeries }).ConfigureAwait(false);
        tmpMarkerCount.ShouldBe(1L,
            "all 16 callers should converge to one merged marker row covering the gap");
    }

    /// <summary>
    /// Defense-in-depth: even if the SingleFlight is bypassed (simulated
    /// here by writing markers directly via RangeMarkerWriter from N
    /// tasks), the RangeMarkerWriter's own pg_advisory_xact_lock must
    /// keep the writes idempotent. This pins Layer 3 (pg lock) as
    /// effective independent of Layer 1 (SF).
    /// </summary>
    [Fact]
    public async Task Macro_RangeMarkerWriter_DirectConcurrent_NoDuplicateKey()
    {
        const int kFanout = 8;
        const string kSeries = "DEFENSE-IN-DEPTH";
        var tmpStart = new DateOnly(2024, 1, 1);

        var tmpStartGate = new TaskCompletionSource();
        var tmpExceptions = new ConcurrentBag<Exception>();
        var tmpTasks = new Task[kFanout];
        for (var i = 0; i < kFanout; i++)
        {
            var tmpFrom = tmpStart.AddDays(i * 7);
            var tmpTo = tmpStart.AddDays(((i + 1) * 7) - 1);
            var tmpRange = (
                From: new DateTimeOffset(tmpFrom.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
                To: new DateTimeOffset(tmpTo.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero));
            tmpTasks[i] = Task.Run(async () =>
            {
                await tmpStartGate.Task.ConfigureAwait(false);
                try
                {
                    await using var tmpConn = new NpgsqlConnection(m_ConnStr);
                    await tmpConn.OpenAsync().ConfigureAwait(false);
                    await RangeMarkerWriter.WriteAsync(
                        tmpConn,
                        MacroDataProvider.MacroMissTableSpec,
                        new[] { new KeyValuePair<string, object>("SeriesId", kSeries) },
                        new[] { tmpRange },
                        "defense-in-depth",
                        inAdjacencyTicks: TimeSpan.FromDays(2).Ticks,
                        inCt: CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex) { tmpExceptions.Add(ex); }
            });
        }
        tmpStartGate.SetResult();
        await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        tmpExceptions.ShouldBeEmpty();
    }

    // ── Stubs ───────────────────────────────────────────────────────────

    private sealed class CountingBarFetcher : IPolygonBarFetcher
    {
        private int m_Calls;
        private readonly IReadOnlyList<Bar> m_Bars;
        public CountingBarFetcher(IReadOnlyList<Bar> inBars) { m_Bars = inBars; }
        public int Calls => Volatile.Read(ref m_Calls);
        public Task<IReadOnlyList<Bar>> FetchBarsAsync(
            string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            BarTimeframe inTimeframe, CancellationToken inCt)
        {
            Interlocked.Increment(ref m_Calls);
            return Task.FromResult(m_Bars);
        }
    }

    private sealed class CountingNbboFetcher : IPolygonNbboFetcher
    {
        private int m_Calls;
        private readonly string m_Ticker;
        public CountingNbboFetcher(string quoteTicker) { m_Ticker = quoteTicker; }
        public int Calls => Volatile.Read(ref m_Calls);
        public async Task<PolygonNbboFetch> FetchAsync(
            string inTicker, DateTime inTsUtc, CancellationToken inCt)
        {
            Interlocked.Increment(ref m_Calls);
            // Inject a small delay so concurrent waiters have time to
            // queue at the SingleFlight slot before the first resolution.
            await Task.Delay(50, inCt).ConfigureAwait(false);
            return new PolygonNbboFetch(
                Outcome: PolygonNbboOutcome.Hit,
                Quote: new PolygonNbboResult(
                    Ticker: m_Ticker,
                    RequestedTsUtc: inTsUtc,
                    AsOfTsUtc: inTsUtc,
                    BidPrice: 1.50m,
                    AskPrice: 1.55m,
                    BidSize: 10,
                    AskSize: 12,
                    BidExchange: 1,
                    AskExchange: 2),
                MissReason: null);
        }
    }

    private sealed class CountingChainFetcher : IPolygonChainFetcher
    {
        private int m_Calls;
        public int Calls => Volatile.Read(ref m_Calls);
        public async Task<IReadOnlyList<TreyThomasCodes.Polygon.Models.Options.OptionsContract>>
            FetchChainAsync(string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
        {
            Interlocked.Increment(ref m_Calls);
            await Task.Delay(50, inCt).ConfigureAwait(false);
            return new List<TreyThomasCodes.Polygon.Models.Options.OptionsContract>
            {
                new()
                {
                    Ticker = $"O:{inSymbol}260417C00500000",
                    UnderlyingTicker = inSymbol,
                    ContractType = "call",
                    ExerciseStyle = "american",
                    ExpirationDate = "2026-04-17",
                    StrikePrice = 500m,
                    SharesPerContract = 100,
                },
            };
        }
    }

    private sealed class CountingFredFetcher : IFredFetcher
    {
        private int m_Calls;
        private readonly bool m_EmptyResponse;
        public CountingFredFetcher(bool emptyResponse) { m_EmptyResponse = emptyResponse; }
        public int Calls => Volatile.Read(ref m_Calls);
        public async Task<IReadOnlyList<FredObservationRow>> FetchSeriesAsync(
            string inSeriesId, DateOnly inFromDate, DateOnly inToDate,
            CancellationToken inCt)
        {
            Interlocked.Increment(ref m_Calls);
            await Task.Delay(50, inCt).ConfigureAwait(false);
            if (m_EmptyResponse) return Array.Empty<FredObservationRow>();
            return new List<FredObservationRow>
            {
                new(inSeriesId, inFromDate, 1.23m),
            };
        }
    }

    // ── Schema ──────────────────────────────────────────────────────────

    private static async Task ApplyAllSchemaAsync(string inConnStr)
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

            CREATE TABLE IF NOT EXISTS historical_options_quotes (
              ticker            VARCHAR(50)  NOT NULL,
              ts                TIMESTAMPTZ  NOT NULL,
              as_of_ts          TIMESTAMPTZ,
              bid_price         DECIMAL(18,4),
              ask_price         DECIMAL(18,4),
              bid_size          INT,
              ask_size          INT,
              bid_exchange      INT,
              ask_exchange      INT,
              underlying_price  DECIMAL(18,4),
              fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            SELECT create_hypertable('historical_options_quotes', 'ts', if_not_exists => TRUE);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_options_quotes_ticker_ts
              ON historical_options_quotes (ticker, ts);

            CREATE TABLE IF NOT EXISTS historical_options_quotes_misses (
              ticker      VARCHAR(50)  NOT NULL,
              range_from  TIMESTAMPTZ  NOT NULL,
              range_to    TIMESTAMPTZ  NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (ticker, range_from, range_to)
            );

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
            SELECT create_hypertable('historical_options_contracts', 'as_of_date', if_not_exists => TRUE);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_options_date_ticker
              ON historical_options_contracts (as_of_date, ticker);

            CREATE TABLE IF NOT EXISTS historical_options_chains_misses (
              symbol      VARCHAR(10)  NOT NULL,
              range_from  DATE         NOT NULL,
              range_to    DATE         NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (symbol, range_from, range_to)
            );

            CREATE TABLE IF NOT EXISTS macro_data (
              series_id VARCHAR(20) NOT NULL,
              observation_date DATE NOT NULL,
              value DECIMAL(18,6)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_series_date
              ON macro_data (series_id, observation_date);

            CREATE TABLE IF NOT EXISTS macro_data_misses (
              series_id   VARCHAR(20)  NOT NULL,
              range_from  DATE         NOT NULL,
              range_to    DATE         NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              CONSTRAINT macro_data_misses_v2_pkey
                PRIMARY KEY (series_id, range_from, range_to)
            );
            """);
    }
}
