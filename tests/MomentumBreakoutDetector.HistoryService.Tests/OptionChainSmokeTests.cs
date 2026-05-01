using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Smoke test for micro-PR #4 — provider integration:
///   Cold-start flow: empty cache → stubbed Polygon JSON → upsert →
///   warm read returns the upserted contract list with cache_hit=true
///   on the second call.
///
/// Provides:
///   - A throwaway TimescaleDB container (Testcontainers).
///   - A stubbed HttpClient that returns a hand-crafted Polygon
///     /v3/reference/options/contracts response shape.
///
/// Migration #003 + #005 (chains table + miss-markers table) is applied
/// to the container before the test runs.
/// </summary>
public class OptionChainSmokeTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        // The production image is timescaledb/timescaledb:latest-pg16; the
        // chains table is a hypertable. For the smoke test we don't need
        // hypertable-specific behavior — a plain postgres container with
        // the table created as a normal table is enough to exercise the
        // upsert + cache + miss-marker code paths. The migration calls
        // create_hypertable() guarded by if_not_exists; we rewrite that to
        // a no-op below.
        .WithImage("postgres:16-alpine")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        // Apply just the bits this test needs: contracts table + chains
        // miss-markers. Skip create_hypertable (Timescale-only).
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await using var tmpCmd = tmpConn.CreateCommand();
        tmpCmd.CommandText = """
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
        """;
        await tmpCmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task ColdStart_ThenWarm_ReturnsContracts_AndShortCircuitsSecondCall()
    {
        // ---- Stub Polygon response ----
        var tmpStubBody = """
        {
          "results": [
            {
              "ticker": "O:TSLA240105C00250000",
              "underlying_ticker": "TSLA",
              "contract_type": "call",
              "exercise_style": "american",
              "expiration_date": "2024-01-05",
              "strike_price": 250.0,
              "shares_per_contract": 100,
              "primary_exchange": "BATO"
            },
            {
              "ticker": "O:TSLA240105P00240000",
              "underlying_ticker": "TSLA",
              "contract_type": "put",
              "exercise_style": "american",
              "expiration_date": "2024-01-05",
              "strike_price": 240.0,
              "shares_per_contract": 100,
              "primary_exchange": "BATO"
            }
          ],
          "next_url": null,
          "status": "OK"
        }
        """;
        var tmpHandler = new CountingStubHandler(tmpStubBody);
        var tmpHttp = new HttpClient(tmpHandler);

        var tmpPolygonOpts = Options.Create(new PolygonOptions
        {
            ApiKey = "stub-key",
            BaseUrl = "https://stub.polygon.local",
        });

        var tmpFetcher = new PolygonChainFetcher(
            tmpHttp, tmpPolygonOpts, NullLogger<PolygonChainFetcher>.Instance);

        var tmpHistoryOpts = Options.Create(new HistoryServiceOptions
        {
            ConnectionString = _postgres.GetConnectionString(),
        });
        var tmpProvider = new OptionChainProvider(
            tmpHistoryOpts, NullLogger<OptionChainProvider>.Instance, tmpFetcher);

        var tmpAsOf = new DateOnly(2024, 1, 4);

        // ---- 1. Cold start — cache empty, fetch hits stub once, upsert ----
        var tmpCold = await tmpProvider.GetChainAsync("TSLA", tmpAsOf, CancellationToken.None);

        tmpCold.Contracts.Count.ShouldBe(2);
        tmpCold.IsMissMarker.ShouldBeFalse();
        tmpCold.CacheHit.ShouldBeFalse();
        tmpHandler.CallCount.ShouldBe(1);

        // Provider's stable ORDER BY: strike, expiration, ticker.
        // Strike 240 (put) < strike 250 (call), so put comes first.
        tmpCold.Contracts[0].Ticker.ShouldBe("O:TSLA240105P00240000");
        tmpCold.Contracts[0].ContractType.ShouldBe("put");
        tmpCold.Contracts[0].StrikePrice.ShouldBe(240.0m);
        tmpCold.Contracts[1].Ticker.ShouldBe("O:TSLA240105C00250000");
        tmpCold.Contracts[1].ContractType.ShouldBe("call");

        // ---- 2. Warm — second call short-circuits, no Polygon hit ----
        var tmpWarm = await tmpProvider.GetChainAsync("TSLA", tmpAsOf, CancellationToken.None);

        tmpWarm.Contracts.Count.ShouldBe(2);
        tmpWarm.CacheHit.ShouldBeTrue();
        tmpWarm.IsMissMarker.ShouldBeFalse();
        tmpHandler.CallCount.ShouldBe(1);  // unchanged
    }

    /// <summary>
    /// Returns a fixed JSON body to every request, counts calls.
    /// </summary>
    private sealed class CountingStubHandler : HttpMessageHandler
    {
        private readonly string _body;
        public int CallCount;

        public CountingStubHandler(string body) => _body = body;

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref CallCount);
            var resp = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(_body, Encoding.UTF8, "application/json"),
            };
            return Task.FromResult(resp);
        }
    }
}
