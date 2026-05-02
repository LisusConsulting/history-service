using System.Net;
using Alpaca.Markets;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Unit tests for <see cref="AlpacaBarFetcher"/> (Phase 2c — stock-bars
/// vendor swap from Polygon to Alpaca).
///
/// The Alpaca SDK is mocked via NSubstitute so the test runs offline +
/// deterministically. We exercise three contracts that the production
/// path depends on:
///   1. Range request returns the expected count and shape.
///   2. Pagination across NextPageToken concatenates all pages.
///   3. Auth failure (401/403) surfaces as a warn log + empty list,
///      never a crash. Mirrors PolygonBarFetcher's fail-quiet contract.
/// </summary>
public sealed class AlpacaBarFetcherTests
{
    [Fact]
    public async Task FetchBarsAsync_SinglePage_ReturnsMappedBars()
    {
        // Arrange — 3 bars on 2026-04-15, 13:30..13:32 UTC.
        var tmpFromTs = new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(2);

        var tmpBars = new[]
        {
            FakeBar("TSLA", tmpFromTs.AddMinutes(0), 250.10m, 250.50m, 250.00m, 250.30m, 1000m, 250.20m),
            FakeBar("TSLA", tmpFromTs.AddMinutes(1), 250.30m, 250.70m, 250.20m, 250.60m, 1100m, 250.45m),
            FakeBar("TSLA", tmpFromTs.AddMinutes(2), 250.60m, 250.90m, 250.50m, 250.80m, 1200m, 250.70m),
        };

        var tmpPage = FakePage(tmpBars, nextPageToken: null);
        var tmpClient = Substitute.For<IHistoricalBarsClient<HistoricalBarsRequest>>();
        tmpClient
            .ListHistoricalBarsAsync(Arg.Any<HistoricalBarsRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => tmpPage);

        var tmpFetcher = new AlpacaBarFetcher(
            tmpClient, MarketDataFeed.Sip,
            NullLogger<AlpacaBarFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchBarsAsync(
            "TSLA", tmpFromTs, tmpToTs, BarTimeframe.OneMinute, CancellationToken.None);

        // Assert
        tmpResult.Count.ShouldBe(3);
        tmpResult[0].Symbol.ShouldBe("TSLA");
        tmpResult[0].Timestamp.ShouldBe(tmpFromTs);
        tmpResult[0].Open.ShouldBe(250.10m);
        tmpResult[0].Close.ShouldBe(250.30m);
        tmpResult[0].Volume.ShouldBe(1000m);
        tmpResult[0].VWAP.ShouldBe(250.20m);
        tmpResult[2].Timestamp.ShouldBe(tmpFromTs.AddMinutes(2));
        await tmpClient.Received(1).ListHistoricalBarsAsync(
            Arg.Any<HistoricalBarsRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchBarsAsync_PostFiltersOutOfRangeBars()
    {
        // Arrange — Alpaca returns 4 bars but only 2 fall inside the
        // requested [from, to] window. The fetcher must clip the rest.
        var tmpFromTs = new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(1);

        var tmpBars = new[]
        {
            FakeBar("TSLA", tmpFromTs.AddMinutes(-1), 1m, 1m, 1m, 1m, 1m, 1m), // before
            FakeBar("TSLA", tmpFromTs.AddMinutes(0), 250.10m, 250.50m, 250.00m, 250.30m, 1000m, 250.20m),
            FakeBar("TSLA", tmpFromTs.AddMinutes(1), 250.30m, 250.70m, 250.20m, 250.60m, 1100m, 250.45m),
            FakeBar("TSLA", tmpFromTs.AddMinutes(2), 9m, 9m, 9m, 9m, 9m, 9m),  // after
        };

        var tmpPage = FakePage(tmpBars, nextPageToken: null);
        var tmpClient = Substitute.For<IHistoricalBarsClient<HistoricalBarsRequest>>();
        tmpClient
            .ListHistoricalBarsAsync(Arg.Any<HistoricalBarsRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => tmpPage);

        var tmpFetcher = new AlpacaBarFetcher(
            tmpClient, MarketDataFeed.Sip,
            NullLogger<AlpacaBarFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchBarsAsync(
            "TSLA", tmpFromTs, tmpToTs, BarTimeframe.OneMinute, CancellationToken.None);

        // Assert
        tmpResult.Count.ShouldBe(2);
        tmpResult[0].Timestamp.ShouldBe(tmpFromTs);
        tmpResult[1].Timestamp.ShouldBe(tmpFromTs.AddMinutes(1));
    }

    [Fact]
    public async Task FetchBarsAsync_PaginatesAcrossNextPageToken()
    {
        // Arrange — two pages, 2 bars each, total 4. The fetcher must
        // page until NextPageToken is null/empty.
        var tmpFromTs = new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(3);

        var tmpPage1Bars = new[]
        {
            FakeBar("TSLA", tmpFromTs.AddMinutes(0), 250.10m, 250.50m, 250.00m, 250.30m, 1000m, 250.20m),
            FakeBar("TSLA", tmpFromTs.AddMinutes(1), 250.30m, 250.70m, 250.20m, 250.60m, 1100m, 250.45m),
        };
        var tmpPage2Bars = new[]
        {
            FakeBar("TSLA", tmpFromTs.AddMinutes(2), 250.60m, 250.90m, 250.50m, 250.80m, 1200m, 250.70m),
            FakeBar("TSLA", tmpFromTs.AddMinutes(3), 250.80m, 251.10m, 250.70m, 251.00m, 1300m, 250.90m),
        };

        var tmpPage1 = FakePage(tmpPage1Bars, nextPageToken: "page-2-token");
        var tmpPage2 = FakePage(tmpPage2Bars, nextPageToken: null);
        var tmpCallIdx = 0;
        var tmpClient = Substitute.For<IHistoricalBarsClient<HistoricalBarsRequest>>();
        tmpClient
            .ListHistoricalBarsAsync(Arg.Any<HistoricalBarsRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => tmpCallIdx++ == 0 ? tmpPage1 : tmpPage2);

        var tmpFetcher = new AlpacaBarFetcher(
            tmpClient, MarketDataFeed.Sip,
            NullLogger<AlpacaBarFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchBarsAsync(
            "TSLA", tmpFromTs, tmpToTs, BarTimeframe.OneMinute, CancellationToken.None);

        // Assert
        tmpResult.Count.ShouldBe(4);
        tmpResult[0].Timestamp.ShouldBe(tmpFromTs);
        tmpResult[3].Timestamp.ShouldBe(tmpFromTs.AddMinutes(3));
        await tmpClient.Received(2).ListHistoricalBarsAsync(
            Arg.Any<HistoricalBarsRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchBarsAsync_AuthFailure_FailsQuietWithEmptyList()
    {
        // Arrange — Alpaca SDK throws RestClientErrorException with 401
        // when the API key is invalid. The fetcher must return an empty
        // list (caller writes a miss marker) and emit a warn log, NOT
        // crash. Mirrors PolygonBarFetcher's behaviour so the upstream
        // miss-marker pipeline keeps working unchanged.
        var tmpFromTs = new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(1);

        var tmpClient = Substitute.For<IHistoricalBarsClient<HistoricalBarsRequest>>();
        // RestClientErrorException's HttpStatusCode is read-only and the
        // SDK only sets it via internal constructors. We surface the
        // status as an HttpRequestException with StatusCode set — the
        // fetcher's status extraction recognises both shapes (see
        // AlpacaBarFetcher.TryExtractStatusCode).
        var tmpAuthEx = new HttpRequestException(
            "invalid api key", inner: null, statusCode: HttpStatusCode.Unauthorized);
        tmpClient
            .ListHistoricalBarsAsync(Arg.Any<HistoricalBarsRequest>(), Arg.Any<CancellationToken>())
            .Throws(tmpAuthEx);

        var tmpFetcher = new AlpacaBarFetcher(
            tmpClient, MarketDataFeed.Sip,
            NullLogger<AlpacaBarFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchBarsAsync(
            "TSLA", tmpFromTs, tmpToTs, BarTimeframe.OneMinute, CancellationToken.None);

        // Assert
        tmpResult.ShouldBeEmpty();
    }

    [Fact]
    public async Task FetchBarsAsync_RateLimit_FailsQuietWithEmptyList()
    {
        // Arrange — 429 fail-quiets too (caller writes a miss marker;
        // subsequent runs hit the marker and skip re-fetch).
        var tmpFromTs = new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(1);

        var tmpClient = Substitute.For<IHistoricalBarsClient<HistoricalBarsRequest>>();
        tmpClient
            .ListHistoricalBarsAsync(Arg.Any<HistoricalBarsRequest>(), Arg.Any<CancellationToken>())
            .Throws(new HttpRequestException(
                "rate limited", inner: null, statusCode: HttpStatusCode.TooManyRequests));

        var tmpFetcher = new AlpacaBarFetcher(
            tmpClient, MarketDataFeed.Sip,
            NullLogger<AlpacaBarFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchBarsAsync(
            "TSLA", tmpFromTs, tmpToTs, BarTimeframe.OneMinute, CancellationToken.None);

        // Assert
        tmpResult.ShouldBeEmpty();
    }

    [Fact]
    public async Task FetchBarsAsync_ServerError_FailsLoud()
    {
        // Arrange — 5xx propagates so the engine surfaces it as
        // BacktestFailed instead of silently mis-modeling.
        var tmpFromTs = new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc);
        var tmpToTs = tmpFromTs.AddMinutes(1);

        var tmpClient = Substitute.For<IHistoricalBarsClient<HistoricalBarsRequest>>();
        tmpClient
            .ListHistoricalBarsAsync(Arg.Any<HistoricalBarsRequest>(), Arg.Any<CancellationToken>())
            .Throws(new HttpRequestException(
                "server error", inner: null, statusCode: HttpStatusCode.InternalServerError));

        var tmpFetcher = new AlpacaBarFetcher(
            tmpClient, MarketDataFeed.Sip,
            NullLogger<AlpacaBarFetcher>.Instance);

        // Act + Assert — fail loud (5xx propagates; the engine will
        // surface BacktestFailed instead of mis-modeling).
        await Should.ThrowAsync<HttpRequestException>(() =>
            tmpFetcher.FetchBarsAsync(
                "TSLA", tmpFromTs, tmpToTs, BarTimeframe.OneMinute, CancellationToken.None));
    }

    [Fact]
    public void ParseFeed_DefaultsToSip_OnNullOrUnknown()
    {
        AlpacaBarFetcher.ParseFeed(null).ShouldBe(MarketDataFeed.Sip);
        AlpacaBarFetcher.ParseFeed("").ShouldBe(MarketDataFeed.Sip);
        AlpacaBarFetcher.ParseFeed("nonsense").ShouldBe(MarketDataFeed.Sip);
        AlpacaBarFetcher.ParseFeed("sip").ShouldBe(MarketDataFeed.Sip);
        AlpacaBarFetcher.ParseFeed("SIP").ShouldBe(MarketDataFeed.Sip);
        AlpacaBarFetcher.ParseFeed("iex").ShouldBe(MarketDataFeed.Iex);
        AlpacaBarFetcher.ParseFeed("IEX").ShouldBe(MarketDataFeed.Iex);
        AlpacaBarFetcher.ParseFeed("otc").ShouldBe(MarketDataFeed.Otc);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test helpers — fake IBar / IPage<IBar> via NSubstitute. We don't
    // pull in Alpaca's internal classes; the SDK exposes only the
    // interface so each call returns a substituted instance with the
    // properties we care about wired up.
    // ─────────────────────────────────────────────────────────────────────

    private static IBar FakeBar(
        string inSymbol, DateTime inTimeUtc,
        decimal inOpen, decimal inHigh, decimal inLow, decimal inClose,
        decimal inVolume, decimal inVwap)
    {
        var tmpBar = Substitute.For<IBar>();
        tmpBar.Symbol.Returns(inSymbol);
        tmpBar.TimeUtc.Returns(inTimeUtc);
        tmpBar.Open.Returns(inOpen);
        tmpBar.High.Returns(inHigh);
        tmpBar.Low.Returns(inLow);
        tmpBar.Close.Returns(inClose);
        tmpBar.Volume.Returns(inVolume);
        tmpBar.Vwap.Returns(inVwap);
        return tmpBar;
    }

    private static IPage<IBar> FakePage(IReadOnlyList<IBar> inItems, string? nextPageToken)
    {
        var tmpPage = Substitute.For<IPage<IBar>>();
        tmpPage.Items.Returns(inItems);
        tmpPage.NextPageToken.Returns(nextPageToken);
        tmpPage.Symbol.Returns(inItems.FirstOrDefault()?.Symbol ?? "");
        return tmpPage;
    }
}
