using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using NSubstitute;
using Shouldly;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.Models.Options;
using TreyThomasCodes.Polygon.RestClient.Services;
using Xunit;
using OptionsGetQuotesRequest = TreyThomasCodes.Polygon.RestClient.Requests.Options.GetQuotesRequest;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Freshness-gate tests for <see cref="PolygonNbboFetcher"/>.
///
/// /v3/quotes with <c>timestamp.lte=&lt;bucket&gt;&amp;order=desc&amp;limit=1</c>
/// returns the most-recent NBBO at-or-before the bucket — for a contract
/// that hasn't ticked yet in the requested session, that means the
/// prior-session close NBBO. Without a freshness gate, those quotes
/// were persisted under fresh-bucket keys (e.g. SPY 240103 contracts
/// stamped under 2024-01-02 09:30 ET bucket but with sip from
/// 2023-12-29 16:14 ET — the prior trading day's close). See
/// <c>002-create-historical-options-quotes.sql</c> migration comment +
/// the 2026-05-02 Lisus screenshot that surfaced this.
/// </summary>
public class PolygonNbboFetcherFreshnessTests
{
    private const long EPOCH_MS_PER_NS = 1_000_000L;
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static long ToSipNs(DateTime utc)
        => (long)(utc - UnixEpoch).TotalMilliseconds * EPOCH_MS_PER_NS;

    private static PolygonResponse<List<OptionQuote>> MakeQuoteBody(DateTime sipUtc)
        => new()
        {
            Status = "OK",
            Results = new List<OptionQuote>
            {
                new()
                {
                    BidPrice = 1.10m,
                    AskPrice = 1.20m,
                    BidSize = 5,
                    AskSize = 6,
                    BidExchange = 1,
                    AskExchange = 2,
                    SipTimestamp = ToSipNs(sipUtc),
                }
            }
        };

    [Fact]
    public async Task QuoteFromPriorSession_ReturnsMissWithStaleQuoteReason()
    {
        // Arrange — bucket = Tuesday 2024-01-02 14:30:00Z (09:30 ET open),
        // sip = Friday 2023-12-29 21:14:19Z (16:14 ET prior-day close).
        // This is the exact shape from Lisus's 2026-05-02 screenshot.
        var tmpTicker = "O:SPY240103P00450000";
        var tmpBucket = new DateTime(2024, 1, 2, 14, 30, 0, DateTimeKind.Utc);
        var tmpStaleSip = new DateTime(2023, 12, 29, 21, 14, 19, DateTimeKind.Utc);

        var tmpOptions = Substitute.For<IOptionsService>();
        tmpOptions
            .GetQuotesAsync(Arg.Any<OptionsGetQuotesRequest>(), Arg.Any<CancellationToken>())
            .Returns(MakeQuoteBody(tmpStaleSip));

        var tmpFetcher = new PolygonNbboFetcher(
            tmpOptions, NullLogger<PolygonNbboFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchAsync(tmpTicker, tmpBucket, CancellationToken.None);

        // Assert — Miss, not Hit. The provider records a miss-marker so we
        // never persist this stale quote under the bucket key.
        tmpResult.Outcome.ShouldBe(PolygonNbboOutcome.Miss);
        tmpResult.Quote.ShouldBeNull();
        tmpResult.MissReason.ShouldBe("stale-quote");
    }

    [Fact]
    public async Task QuoteOlderThanMaxAge_ReturnsMissWithStaleQuoteReason()
    {
        // Arrange — bucket and sip on the same calendar day, but sip is
        // 6 minutes old (default max age 300s = 5 min).
        var tmpTicker = "O:TSLA240115C00250000";
        var tmpBucket = new DateTime(2024, 1, 15, 15, 30, 0, DateTimeKind.Utc);
        var tmpStaleSip = tmpBucket.AddSeconds(-360);

        var tmpOptions = Substitute.For<IOptionsService>();
        tmpOptions
            .GetQuotesAsync(Arg.Any<OptionsGetQuotesRequest>(), Arg.Any<CancellationToken>())
            .Returns(MakeQuoteBody(tmpStaleSip));

        var tmpFetcher = new PolygonNbboFetcher(
            tmpOptions, NullLogger<PolygonNbboFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchAsync(tmpTicker, tmpBucket, CancellationToken.None);

        // Assert
        tmpResult.Outcome.ShouldBe(PolygonNbboOutcome.Miss);
        tmpResult.MissReason.ShouldBe("stale-quote");
    }

    [Fact]
    public async Task QuoteWithinMaxAge_ReturnsHit()
    {
        // Arrange — sip is 3 minutes old, comfortably within the default
        // 5-min freshness window. This is the normal happy path: a
        // moderately-illiquid contract last quoted a few minutes ago,
        // which the read-side fuzzy at-or-before window also accepts.
        var tmpTicker = "O:TSLA240115C00250000";
        var tmpBucket = new DateTime(2024, 1, 15, 15, 30, 0, DateTimeKind.Utc);
        var tmpFreshSip = tmpBucket.AddSeconds(-180);

        var tmpOptions = Substitute.For<IOptionsService>();
        tmpOptions
            .GetQuotesAsync(Arg.Any<OptionsGetQuotesRequest>(), Arg.Any<CancellationToken>())
            .Returns(MakeQuoteBody(tmpFreshSip));

        var tmpFetcher = new PolygonNbboFetcher(
            tmpOptions, NullLogger<PolygonNbboFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchAsync(tmpTicker, tmpBucket, CancellationToken.None);

        // Assert
        tmpResult.Outcome.ShouldBe(PolygonNbboOutcome.Hit);
        tmpResult.Quote.ShouldNotBeNull();
        tmpResult.Quote!.BidPrice.ShouldBe(1.10m);
        tmpResult.Quote.AskPrice.ShouldBe(1.20m);
        tmpResult.Quote.AsOfTsUtc.ShouldBe(tmpFreshSip);
    }

    [Fact]
    public async Task QuoteAtBucketBoundary_ReturnsHit()
    {
        // Arrange — sip exactly at bucket ts (age = 0).
        var tmpTicker = "O:TSLA240115C00250000";
        var tmpBucket = new DateTime(2024, 1, 15, 15, 30, 0, DateTimeKind.Utc);

        var tmpOptions = Substitute.For<IOptionsService>();
        tmpOptions
            .GetQuotesAsync(Arg.Any<OptionsGetQuotesRequest>(), Arg.Any<CancellationToken>())
            .Returns(MakeQuoteBody(tmpBucket));

        var tmpFetcher = new PolygonNbboFetcher(
            tmpOptions, NullLogger<PolygonNbboFetcher>.Instance);

        // Act
        var tmpResult = await tmpFetcher.FetchAsync(tmpTicker, tmpBucket, CancellationToken.None);

        // Assert
        tmpResult.Outcome.ShouldBe(PolygonNbboOutcome.Hit);
    }

    [Fact]
    public async Task CustomMaxAge_AppliesGate()
    {
        // Arrange — override max age to 30s. A 60s-old sip should now be
        // rejected even though it would be accepted at the default 300s.
        var tmpTicker = "O:TSLA240115C00250000";
        var tmpBucket = new DateTime(2024, 1, 15, 15, 30, 0, DateTimeKind.Utc);
        var tmpSip = tmpBucket.AddSeconds(-60);

        var tmpOptions = Substitute.For<IOptionsService>();
        tmpOptions
            .GetQuotesAsync(Arg.Any<OptionsGetQuotesRequest>(), Arg.Any<CancellationToken>())
            .Returns(MakeQuoteBody(tmpSip));

        var tmpFetcher = new PolygonNbboFetcher(
            tmpOptions, NullLogger<PolygonNbboFetcher>.Instance,
            inMaxQuoteAgeSeconds: 30);

        // Act
        var tmpResult = await tmpFetcher.FetchAsync(tmpTicker, tmpBucket, CancellationToken.None);

        // Assert
        tmpResult.Outcome.ShouldBe(PolygonNbboOutcome.Miss);
        tmpResult.MissReason.ShouldBe("stale-quote");
    }
}
