using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using NSubstitute;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.Models.Options;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Smoke test for the option-chain provider. Phase E: post the SDK
/// refactor (PolygonChainFetcher now consumes IOptionsService instead of
/// raw HttpClient), this test stubs the SDK service rather than the
/// HTTP message handler. Same end-to-end contract:
///
///   Cold-start flow: empty cache → stub returns 2 contracts → upsert →
///   warm read returns the upserted contract list with cache_hit=true on
///   the second call (and the SDK stub is NOT invoked again).
/// </summary>
public class OptionChainSmokeTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

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
          range_from  DATE         NOT NULL,
          range_to    DATE         NOT NULL,
          reason      TEXT,
          fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
          PRIMARY KEY (symbol, range_from, range_to)
        );
        """;
        await tmpCmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task ColdStart_ThenWarm_ReturnsContracts_AndShortCircuitsSecondCall()
    {
        // ---- Stub IOptionsService — returns 2 contracts on first call ----
        var tmpStubService = Substitute.For<IOptionsService>();
        var tmpResults = new List<OptionsContract>
        {
            new()
            {
                Ticker = "O:TSLA240105C00250000",
                UnderlyingTicker = "TSLA",
                ContractType = "call",
                ExerciseStyle = "american",
                ExpirationDate = "2024-01-05",
                StrikePrice = 250.0m,
                SharesPerContract = 100,
                PrimaryExchange = "BATO",
            },
            new()
            {
                Ticker = "O:TSLA240105P00240000",
                UnderlyingTicker = "TSLA",
                ContractType = "put",
                ExerciseStyle = "american",
                ExpirationDate = "2024-01-05",
                StrikePrice = 240.0m,
                SharesPerContract = 100,
                PrimaryExchange = "BATO",
            },
        };

        var tmpEnvelope = new PolygonResponse<List<OptionsContract>>
        {
            Results = tmpResults,
            NextUrl = null,
            Status = "OK",
        };

        // Wrap in a Refit ApiResponse — the SDK Raw variant returns this
        // wrapper. Use the in-source HttpResponseMessage helper so the
        // status code is 200.
        tmpStubService
            .GetListContractsRawAsync(Arg.Any<GetListContractsRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(MakeOkResponse(tmpEnvelope)));

        var tmpFetcher = new PolygonChainFetcher(
            tmpStubService, NullLogger<PolygonChainFetcher>.Instance);

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
        await tmpStubService.Received(1)
            .GetListContractsRawAsync(Arg.Any<GetListContractsRequest>(), Arg.Any<CancellationToken>());

        // Provider's stable ORDER BY: strike, expiration, ticker.
        // Strike 240 (put) < strike 250 (call), so put comes first.
        tmpCold.Contracts[0].Ticker.ShouldBe("O:TSLA240105P00240000");
        tmpCold.Contracts[0].ContractType.ShouldBe("put");
        tmpCold.Contracts[0].StrikePrice.ShouldBe(240.0m);
        tmpCold.Contracts[1].Ticker.ShouldBe("O:TSLA240105C00250000");
        tmpCold.Contracts[1].ContractType.ShouldBe("call");

        // ---- 2. Warm — second call short-circuits, stub NOT invoked again ----
        var tmpWarm = await tmpProvider.GetChainAsync("TSLA", tmpAsOf, CancellationToken.None);

        tmpWarm.Contracts.Count.ShouldBe(2);
        tmpWarm.CacheHit.ShouldBeTrue();
        tmpWarm.IsMissMarker.ShouldBeFalse();
        await tmpStubService.Received(1)
            .GetListContractsRawAsync(Arg.Any<GetListContractsRequest>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Build a Refit <see cref="Refit.ApiResponse{T}"/> wrapping a 200/OK
    /// HTTP response with the given parsed body. Refit's ApiResponse ctor
    /// is internal in some versions but its constructor signature
    /// (HttpResponseMessage, T?, RefitSettings, ApiException?) is the
    /// supported way to fabricate one — and 0.10.0's Refit 10.1.6 exposes
    /// it publicly.
    /// </summary>
    private static Refit.ApiResponse<PolygonResponse<List<OptionsContract>>> MakeOkResponse(
        PolygonResponse<List<OptionsContract>> inBody)
    {
        var tmpHttp = new HttpResponseMessage(System.Net.HttpStatusCode.OK)
        {
            RequestMessage = new HttpRequestMessage(
                HttpMethod.Get,
                new Uri("https://api.polygon.io/v3/reference/options/contracts")),
        };
        return new Refit.ApiResponse<PolygonResponse<List<OptionsContract>>>(
            tmpHttp, inBody, new Refit.RefitSettings(), error: null);
    }
}
