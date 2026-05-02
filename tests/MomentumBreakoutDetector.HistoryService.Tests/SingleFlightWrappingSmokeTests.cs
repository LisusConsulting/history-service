using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using NSubstitute;
using Refit;
using Shouldly;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.Models.Options;
using TreyThomasCodes.Polygon.Models.Stocks;
using TreyThomasCodes.Polygon.RestClient.Services;
using Xunit;
using OptionsGetQuotesRequest = TreyThomasCodes.Polygon.RestClient.Requests.Options.GetQuotesRequest;
using OptionsGetListContractsRequest = TreyThomasCodes.Polygon.RestClient.Requests.Options.GetListContractsRequest;
using StocksGetBarsRequest = TreyThomasCodes.Polygon.RestClient.Requests.Stocks.GetBarsRequest;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Phase 1, micro-PR #6 — wrapping smoke tests. Proves the SingleFlight
/// coalescer is correctly wired into each of the 4 fetchers: 50 concurrent
/// callers asking for the same key result in exactly ONE upstream
/// invocation, and all 50 callers get the same payload.
///
/// One test per fetcher; each stubs the underlying SDK service (or
/// IHttpClientFactory in FRED's case) with a delayed response so the
/// coalescer has a chance to gather waiters before the first call
/// completes.
/// </summary>
public class SingleFlightWrappingSmokeTests
{
    private const int FANOUT = 50;
    private const int DELAY_MS = 100;

    [Fact]
    public async Task PolygonBarFetcher_50ConcurrentSameKey_OneUpstreamCall()
    {
        // Arrange — stub IStocksService with a 100ms delay; return a body
        // shaped to map to 5 Bar rows for [13:30Z..13:34Z].
        var tmpDay = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc);
        var tmpFromTs = tmpDay.AddHours(13).AddMinutes(30);
        var tmpToTs = tmpDay.AddHours(13).AddMinutes(34);

        var tmpBody = MakeStockBarsBody(tmpFromTs, count: 5);
        var tmpStocks = Substitute.For<IStocksService>();
        var tmpCalls = 0;
        tmpStocks
            .GetBarsRawAsync(Arg.Any<StocksGetBarsRequest>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                Interlocked.Increment(ref tmpCalls);
                await Task.Delay(DELAY_MS).ConfigureAwait(false);
                return MakeOkResponse(tmpBody, "https://api.polygon.io/v2/aggs");
            });

        var tmpFetcher = new PolygonBarFetcher(tmpStocks, NullLogger<PolygonBarFetcher>.Instance);

        // Act — fan out 50 concurrent requests for the same key.
        var tmpTasks = new Task<IReadOnlyList<Bar>>[FANOUT];
        for (var i = 0; i < FANOUT; i++)
        {
            tmpTasks[i] = tmpFetcher.FetchBarsAsync(
                "TSLA", tmpFromTs, tmpToTs, BarTimeframe.OneMinute, CancellationToken.None);
        }
        var tmpResults = await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        // Assert — ONE upstream call, all callers got 5 bars.
        tmpCalls.ShouldBe(1);
        tmpResults.Length.ShouldBe(FANOUT);
        foreach (var tmpRes in tmpResults)
        {
            tmpRes.Count.ShouldBe(5);
        }
        tmpFetcher.InFlightCount.ShouldBe(0);
    }

    [Fact]
    public async Task PolygonNbboFetcher_50ConcurrentSameKey_OneUpstreamCall()
    {
        // Arrange — stub IOptionsService.GetQuotesAsync with a single quote.
        var tmpTicker = "O:TSLA241220C00250000";
        var tmpTs = new DateTime(2024, 12, 18, 15, 30, 0, DateTimeKind.Utc);

        var tmpBody = new PolygonResponse<List<OptionQuote>>
        {
            Status = "OK",
            Results = new List<OptionQuote>
            {
                new OptionQuote
                {
                    BidPrice = 5.10m,
                    AskPrice = 5.20m,
                    BidSize = 10,
                    AskSize = 12,
                    BidExchange = 1,
                    AskExchange = 2,
                    SipTimestamp = (long)(tmpTs.AddSeconds(-3)
                        - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds * 1_000_000L,
                }
            }
        };

        var tmpOptions = Substitute.For<IOptionsService>();
        var tmpCalls = 0;
        tmpOptions
            .GetQuotesAsync(Arg.Any<OptionsGetQuotesRequest>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                Interlocked.Increment(ref tmpCalls);
                await Task.Delay(DELAY_MS).ConfigureAwait(false);
                return tmpBody;
            });

        var tmpFetcher = new PolygonNbboFetcher(tmpOptions, NullLogger<PolygonNbboFetcher>.Instance);

        // Act
        var tmpTasks = new Task<PolygonNbboFetch>[FANOUT];
        for (var i = 0; i < FANOUT; i++)
        {
            tmpTasks[i] = tmpFetcher.FetchAsync(tmpTicker, tmpTs);
        }
        var tmpResults = await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        // Assert
        tmpCalls.ShouldBe(1);
        tmpResults.Length.ShouldBe(FANOUT);
        foreach (var tmpRes in tmpResults)
        {
            tmpRes.Outcome.ShouldBe(PolygonNbboOutcome.Hit);
            tmpRes.Quote!.BidPrice.ShouldBe(5.10m);
            tmpRes.Quote.AskPrice.ShouldBe(5.20m);
        }
        tmpFetcher.InFlightCount.ShouldBe(0);
    }

    [Fact]
    public async Task PolygonChainFetcher_50ConcurrentSameKey_OneUpstreamSweep()
    {
        // Arrange — single-page chain, no cursor → exactly one
        // GetListContractsRawAsync per coalesced sweep.
        var tmpAsOf = new DateOnly(2024, 1, 5);
        var tmpBody = new PolygonResponse<List<OptionsContract>>
        {
            Status = "OK",
            Results = new List<OptionsContract>
            {
                new OptionsContract
                {
                    Ticker = "O:TSLA240105C00250000",
                    UnderlyingTicker = "TSLA",
                    ContractType = "call",
                    StrikePrice = 250m,
                    ExpirationDate = "2024-01-05",
                },
                new OptionsContract
                {
                    Ticker = "O:TSLA240105P00240000",
                    UnderlyingTicker = "TSLA",
                    ContractType = "put",
                    StrikePrice = 240m,
                    ExpirationDate = "2024-01-05",
                }
            },
            // No NextUrl → loop terminates after one page.
        };

        var tmpOptions = Substitute.For<IOptionsService>();
        var tmpCalls = 0;
        tmpOptions
            .GetListContractsRawAsync(Arg.Any<OptionsGetListContractsRequest>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                Interlocked.Increment(ref tmpCalls);
                await Task.Delay(DELAY_MS).ConfigureAwait(false);
                return MakeOkResponse(tmpBody, "https://api.polygon.io/v3/reference/options/contracts");
            });

        var tmpFetcher = new PolygonChainFetcher(tmpOptions, NullLogger<PolygonChainFetcher>.Instance);

        // Act
        var tmpTasks = new Task<IReadOnlyList<OptionsContract>>[FANOUT];
        for (var i = 0; i < FANOUT; i++)
        {
            tmpTasks[i] = tmpFetcher.FetchChainAsync("TSLA", tmpAsOf, CancellationToken.None);
        }
        var tmpResults = await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        // Assert
        tmpCalls.ShouldBe(1);
        tmpResults.Length.ShouldBe(FANOUT);
        foreach (var tmpRes in tmpResults)
        {
            tmpRes.Count.ShouldBe(2);
        }
        tmpFetcher.InFlightCount.ShouldBe(0);
    }

    [Fact]
    public async Task FredFetcher_50ConcurrentSameKey_OneUpstreamCall()
    {
        // Arrange — stub IHttpClientFactory with a counting handler.
        var tmpFrom = new DateOnly(2024, 1, 1);
        var tmpTo = new DateOnly(2024, 1, 5);
        var tmpJson = """
            {
              "observations": [
                { "date": "2024-01-02", "value": "4.25" },
                { "date": "2024-01-03", "value": "4.27" }
              ]
            }
            """;

        var tmpHandler = new CountingDelayHandler(tmpJson, DELAY_MS);
        var tmpFactory = Substitute.For<IHttpClientFactory>();
        tmpFactory.CreateClient(Arg.Any<string>())
            .Returns(_ => new HttpClient(tmpHandler));

        var tmpFetcher = new FredFetcher(
            NullLogger<FredFetcher>.Instance,
            httpClientFactory: tmpFactory,
            apiKey: "test-key");

        // Act
        var tmpTasks = new Task<IReadOnlyList<FredObservationRow>>[FANOUT];
        for (var i = 0; i < FANOUT; i++)
        {
            tmpTasks[i] = tmpFetcher.FetchSeriesAsync("DGS10", tmpFrom, tmpTo, CancellationToken.None);
        }
        var tmpResults = await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        // Assert — exactly one HTTP call to FRED, all 50 callers got 2 rows.
        tmpHandler.CallCount.ShouldBe(1);
        tmpResults.Length.ShouldBe(FANOUT);
        foreach (var tmpRes in tmpResults)
        {
            tmpRes.Count.ShouldBe(2);
        }
        tmpFetcher.InFlightCount.ShouldBe(0);
    }

    // --- Helpers ----------------------------------------------------------

    private static PolygonResponse<List<StockBar>> MakeStockBarsBody(DateTime startUtc, int count)
    {
        var tmpResults = new List<StockBar>(count);
        var tmpEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        for (var i = 0; i < count; i++)
        {
            var tmpTs = startUtc.AddMinutes(i);
            var tmpUnixMs = (ulong)(tmpTs - tmpEpoch).TotalMilliseconds;
            tmpResults.Add(new StockBar
            {
                Ticker = "TSLA",
                Timestamp = tmpUnixMs,
                Open = 100m + i,
                High = 101m + i,
                Low = 99m + i,
                Close = 100.5m + i,
                Volume = 1000UL + (ulong)i,
                VolumeWeightedAveragePrice = 100.25m + i,
            });
        }
        return new PolygonResponse<List<StockBar>>
        {
            Status = "OK",
            Results = tmpResults,
        };
    }

    private static ApiResponse<PolygonResponse<T>> MakeOkResponse<T>(
        PolygonResponse<T> inBody, string inRequestUri)
    {
        var tmpHttp = new HttpResponseMessage(HttpStatusCode.OK)
        {
            RequestMessage = new HttpRequestMessage(HttpMethod.Get, new Uri(inRequestUri)),
        };
        return new ApiResponse<PolygonResponse<T>>(tmpHttp, inBody, new RefitSettings(), error: null);
    }

    /// <summary>
    /// HttpMessageHandler for FRED test: returns a canned JSON body after
    /// a configurable delay; counts every invocation so we can assert
    /// exactly one upstream call across N coalesced waiters.
    /// </summary>
    private sealed class CountingDelayHandler : HttpMessageHandler
    {
        private readonly string m_Json;
        private readonly int m_DelayMs;
        private int m_Calls;

        public int CallCount => Volatile.Read(ref m_Calls);

        public CountingDelayHandler(string json, int delayMs)
        {
            m_Json = json;
            m_DelayMs = delayMs;
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref m_Calls);
            await Task.Delay(m_DelayMs, cancellationToken).ConfigureAwait(false);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(m_Json, System.Text.Encoding.UTF8, "application/json"),
            };
        }
    }
}
