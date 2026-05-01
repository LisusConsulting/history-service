using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using NSubstitute;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Phase 1, micro-PR #3 smoke test. Stands up a real postgres
/// (Testcontainers), creates the historical_options_quotes(_misses)
/// schema, stubs the Polygon fetcher, and proves the provider's
/// cache + write-through contract:
///
///   - First call: provider hits the stub fetcher, writes through to
///     postgres, returns CacheHit=false.
///   - Second call: provider hits the in-memory cache, returns
///     CacheHit=true with no additional fetcher invocations.
///
/// Comprehensive coverage (miss-markers, fuzzy-window, transient,
/// SingleFlight) lands in micro-PR #8.
/// </summary>
public sealed class OptionQuotesProviderSmokeTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _pg = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("mbd_history_test")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    public async Task InitializeAsync()
    {
        await _pg.StartAsync();
        await using var conn = new NpgsqlConnection(_pg.GetConnectionString());
        await conn.OpenAsync();
        // Same shape as MBD's historical_options_quotes(_misses), trimmed
        // to the columns the provider reads + writes. No timescale extension
        // required for the unit-level cache contract.
        await conn.ExecuteAsync("""
            CREATE TABLE historical_options_quotes (
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
            CREATE TABLE historical_options_quotes_misses (
              ticker      VARCHAR(50)  NOT NULL,
              ts          TIMESTAMPTZ  NOT NULL,
              reason      TEXT,
              recorded_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (ticker, ts)
            );
            """);
    }

    public Task DisposeAsync() => _pg.DisposeAsync().AsTask();

    [Fact]
    public async Task FirstCallFetches_SecondCallHitsCache()
    {
        var ticker = "O:TSLA241220C00250000";
        var ts = new DateTime(2024, 12, 18, 15, 30, 0, DateTimeKind.Utc);
        var asOf = ts.AddSeconds(-12);

        // Stub fetcher: returns one canned hit.
        var fetcher = Substitute.For<IPolygonNbboFetcher>();
        fetcher
            .FetchAsync(Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
            .Returns(new PolygonNbboFetch(
                PolygonNbboOutcome.Hit,
                new PolygonNbboResult(
                    Ticker: ticker,
                    RequestedTsUtc: ts,
                    AsOfTsUtc: asOf,
                    BidPrice: 12.34m,
                    AskPrice: 12.40m,
                    BidSize: 5,
                    AskSize: 7,
                    BidExchange: 12,
                    AskExchange: 13),
                MissReason: null));

        var memCache = new NbboMemoryCache();
        var opts = Options.Create(new HistoryServiceOptions
        {
            ConnectionString = _pg.GetConnectionString(),
            // Disable the fuzzy at-or-before window so the test asserts
            // strict (ticker, ts) cache behaviour — keeps the assertion
            // surface narrow for a smoke test.
            NbboStaleQuoteToleranceSeconds = 0,
        });
        var provider = new OptionQuotesProvider(
            memCache, fetcher, opts, NullLogger<OptionQuotesProvider>.Instance);

        // 1st call — cache miss, fetcher invoked, write-through.
        var first = await provider.GetAtOrBeforeAsync(ticker, ts);
        first.Quote.ShouldNotBeNull();
        first.Quote!.BidPrice.ShouldBe(12.34m);
        first.Quote.AskPrice.ShouldBe(12.40m);
        first.Quote.BidSize.ShouldBe(5);
        first.Quote.AskSize.ShouldBe(7);
        first.CacheHit.ShouldBeFalse();
        first.IsMissMarker.ShouldBeFalse();

        // Cache row landed in postgres.
        await using var conn = new NpgsqlConnection(_pg.GetConnectionString());
        await conn.OpenAsync();
        var rowCount = await conn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_options_quotes WHERE ticker = @t",
            new { t = ticker });
        rowCount.ShouldBe(1);

        // 2nd call — cache hit (in-memory). Fetcher must not be called again.
        var second = await provider.GetAtOrBeforeAsync(ticker, ts);
        second.Quote.ShouldNotBeNull();
        second.Quote!.BidPrice.ShouldBe(12.34m);
        second.CacheHit.ShouldBeTrue();
        second.IsMissMarker.ShouldBeFalse();

        await fetcher.Received(1)
            .FetchAsync(Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>());
    }
}
