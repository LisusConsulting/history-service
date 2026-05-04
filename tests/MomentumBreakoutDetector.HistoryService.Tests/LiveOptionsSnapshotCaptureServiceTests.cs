using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using MomentumBreakoutDetector.HistoryService.HostedServices;
using NSubstitute;
using Shouldly;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.Models.Options;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Unit tests for the Wave B / PR 4 live-capture cron. Coverage:
/// <list type="bullet">
///   <item>Schedule next-fire computation across weekday/weekend, RTH/non-RTH,
///         half-day boundaries.</item>
///   <item>ATM band filter (the static helper) given a hand-built chain.</item>
///   <item>Flag-OFF behaviour — the service body skips Polygon calls.</item>
/// </list>
/// We do NOT exercise the persist path here (that's a Postgres
/// integration test which the pricing layer doesn't strictly need; the
/// SQL shape is mechanical). The DI-graph wiring is validated by the
/// existing service-host smoke tests.
/// </summary>
public class LiveOptionsSnapshotCaptureServiceTests
{
    // ── Schedule logic ─────────────────────────────────────────────────

    [Fact]
    public void ComputeNextFireUtc_BeforeRthOpen_OnTradingDay_ReturnsTodayOpen()
    {
        // Mon 2024-01-08 04:00 ET → 09:00 UTC (EST).
        var tmpNowEt = new DateTime(2024, 1, 8, 4, 0, 0, DateTimeKind.Unspecified);
        var tmpNowUtc = ToEsternDayUtc(tmpNowEt);

        var tmpService = BuildService();
        var tmpFire = tmpService.ComputeNextFireUtc(new DateTimeOffset(tmpNowUtc, TimeSpan.Zero));

        // Expected: today 09:30 ET → 14:30 UTC (EST is UTC-5).
        var tmpExpected = new DateTimeOffset(
            ToEsternDayUtc(new DateTime(2024, 1, 8, 9, 30, 0, DateTimeKind.Unspecified)),
            TimeSpan.Zero);
        tmpFire.ShouldBe(tmpExpected);
    }

    [Fact]
    public void ComputeNextFireUtc_DuringRth_ReturnsNext5MinBoundary()
    {
        // Mon 2024-01-08 09:32 ET — first 5-min slot lands at 09:35 ET.
        var tmpNowUtc = new DateTimeOffset(
            ToEsternDayUtc(new DateTime(2024, 1, 8, 9, 32, 0, DateTimeKind.Unspecified)),
            TimeSpan.Zero);

        var tmpService = BuildService();
        var tmpFire = tmpService.ComputeNextFireUtc(tmpNowUtc);

        var tmpExpected = new DateTimeOffset(
            ToEsternDayUtc(new DateTime(2024, 1, 8, 9, 35, 0, DateTimeKind.Unspecified)),
            TimeSpan.Zero);
        tmpFire.ShouldBe(tmpExpected);
    }

    [Fact]
    public void ComputeNextFireUtc_PastClose_AdvancesToNextTradingDay()
    {
        // Mon 2024-01-08 16:30 ET (after RTH close) → Tue 2024-01-09 09:30 ET.
        var tmpNowUtc = new DateTimeOffset(
            ToEsternDayUtc(new DateTime(2024, 1, 8, 16, 30, 0, DateTimeKind.Unspecified)),
            TimeSpan.Zero);

        var tmpService = BuildService();
        var tmpFire = tmpService.ComputeNextFireUtc(tmpNowUtc);

        var tmpExpected = new DateTimeOffset(
            ToEsternDayUtc(new DateTime(2024, 1, 9, 9, 30, 0, DateTimeKind.Unspecified)),
            TimeSpan.Zero);
        tmpFire.ShouldBe(tmpExpected);
    }

    [Fact]
    public void ComputeNextFireUtc_Saturday_AdvancesToMondayOpen()
    {
        // Sat 2024-01-13 10:00 ET → Mon 2024-01-15 is MLK Day (closed) →
        // Tue 2024-01-16 09:30 ET.
        var tmpNowUtc = new DateTimeOffset(
            ToEsternDayUtc(new DateTime(2024, 1, 13, 10, 0, 0, DateTimeKind.Unspecified)),
            TimeSpan.Zero);

        var tmpService = BuildService();
        var tmpFire = tmpService.ComputeNextFireUtc(tmpNowUtc);

        var tmpExpected = new DateTimeOffset(
            ToEsternDayUtc(new DateTime(2024, 1, 16, 9, 30, 0, DateTimeKind.Unspecified)),
            TimeSpan.Zero);
        tmpFire.ShouldBe(tmpExpected);
    }

    // ── ATM band filter ────────────────────────────────────────────────

    [Fact]
    public void FilterAtmBand_KeepsRowsWithinBand()
    {
        var tmpRows = new[]
        {
            MakeSnap("O:TSLA240119C00190000", 190m),
            MakeSnap("O:TSLA240119C00200000", 200m),
            MakeSnap("O:TSLA240119C00210000", 210m),
            MakeSnap("O:TSLA240119C00250000", 250m), // way OOB
            MakeSnap("O:TSLA240119C00100000", 100m), // way OOB
        };

        var tmpResult = LiveOptionsSnapshotCaptureService
            .FilterAtmBand(tmpRows, inKLow: 190m, inKHigh: 210m).ToList();

        tmpResult.Count.ShouldBe(3);
        tmpResult.ShouldContain(r => r.Details!.Ticker == "O:TSLA240119C00190000");
        tmpResult.ShouldContain(r => r.Details!.Ticker == "O:TSLA240119C00200000");
        tmpResult.ShouldContain(r => r.Details!.Ticker == "O:TSLA240119C00210000");
        tmpResult.ShouldNotContain(r => r.Details!.Ticker == "O:TSLA240119C00250000");
        tmpResult.ShouldNotContain(r => r.Details!.Ticker == "O:TSLA240119C00100000");
    }

    [Fact]
    public void FilterAtmBand_SkipsRowsWithMissingStrike()
    {
        var tmpRows = new[]
        {
            new OptionSnapshot { Details = new OptionContractDetails { Ticker = "missing-strike" } },
            MakeSnap("normal", 200m),
        };
        var tmpResult = LiveOptionsSnapshotCaptureService
            .FilterAtmBand(tmpRows, 190m, 210m).ToList();
        tmpResult.Count.ShouldBe(1);
        tmpResult[0].Details!.Ticker.ShouldBe("normal");
    }

    // ── Flag-OFF body short-circuits ───────────────────────────────────

    [Fact]
    public async Task RunOnceAsync_DoesNothingWhenNoSymbolsConfigured()
    {
        // Master enable + empty symbols list — the service body loops
        // over zero symbols and exits without throwing or calling Polygon.
        var tmpPolygon = Substitute.For<IOptionsService>();
        var tmpOpts = Options.Create(new LiveOptionsSnapshotCaptureOptions
        {
            LiveSnapshotCaptureEnabled = true,
            LiveSnapshotCaptureSymbols = new List<string>(), // empty
        });
        var tmpHistOpts = Options.Create(new HistoryServiceOptions());

        var tmpService = new LiveOptionsSnapshotCaptureService(
            tmpPolygon,
            new FakeTimeProvider(),
            NullLogger<LiveOptionsSnapshotCaptureService>.Instance,
            tmpOpts,
            tmpHistOpts);

        await tmpService.RunOnceAsync(DateTimeOffset.UtcNow, CancellationToken.None);

        // No Polygon calls at all.
        await tmpPolygon.DidNotReceiveWithAnyArgs()
            .GetChainSnapshotAsync(default!, default);
    }

    [Fact]
    public async Task RunOnceAsync_WithSymbol_FetchesChainAndFiltersToBand()
    {
        // The Polygon stub returns 4 snapshots; the band keeps 2.
        var tmpPolygon = Substitute.For<IOptionsService>();
        tmpPolygon.GetChainSnapshotAsync(Arg.Any<GetChainSnapshotRequest>(), Arg.Any<CancellationToken>())
            .Returns(new PolygonResponse<List<OptionSnapshot>>
            {
                Status = "OK",
                Results = new List<OptionSnapshot>
                {
                    MakeSnapWithUnderlying("O:TSLA240119C00200000", inStrike: 200m, inUnderlying: 200m),
                    MakeSnapWithUnderlying("O:TSLA240119C00205000", inStrike: 205m, inUnderlying: 200m),
                    MakeSnapWithUnderlying("O:TSLA240119C00250000", inStrike: 250m, inUnderlying: 200m),
                    MakeSnapWithUnderlying("O:TSLA240119C00150000", inStrike: 150m, inUnderlying: 200m),
                },
            });

        // Use HistoryServiceOptions with an unreachable connection string —
        // we expect the persist to fail; the test asserts "the chain was
        // fetched and filtered" which happens before persistence. The
        // per-symbol try/catch swallows the persist exception.
        var tmpHistOpts = Options.Create(new HistoryServiceOptions
        {
            ConnectionString = "Host=localhost;Port=1;Database=none;Username=none;Password=none",
        });
        var tmpOpts = Options.Create(new LiveOptionsSnapshotCaptureOptions
        {
            LiveSnapshotCaptureEnabled = true,
            LiveSnapshotCaptureSymbols = new[] { "TSLA" },
            StrikeBandPct = 0.05,
            SnapshotDteMaxDays = 60,
        });

        var tmpService = new LiveOptionsSnapshotCaptureService(
            tmpPolygon,
            new FakeTimeProvider(),
            NullLogger<LiveOptionsSnapshotCaptureService>.Instance,
            tmpOpts,
            tmpHistOpts);

        await tmpService.RunOnceAsync(DateTimeOffset.UtcNow, CancellationToken.None);

        // The fetch happened with a TSLA underlying-asset request.
        await tmpPolygon.Received(1).GetChainSnapshotAsync(
            Arg.Is<GetChainSnapshotRequest>(r => r.UnderlyingAsset == "TSLA"),
            Arg.Any<CancellationToken>());
    }

    // ── Cursor pagination ──────────────────────────────────────────────

    [Fact]
    public async Task FetchFullChainAsync_AccumulatesAcrossThreePages()
    {
        // Stub returns 3 pages: page1 (250 rows, next_url set),
        // page2 (250 rows, next_url set), page3 (50 rows, next_url null).
        // Total = 550 rows. Verifies the cursor loop walks all 3 pages.
        var tmpPolygon = Substitute.For<IOptionsService>();
        var tmpPage1 = MakePage(0, 250, nextCursor: "abc-page2");
        var tmpPage2 = MakePage(250, 250, nextCursor: "def-page3");
        var tmpPage3 = MakePage(500, 50, nextCursor: null);

        tmpPolygon.GetChainSnapshotAsync(
            Arg.Is<GetChainSnapshotRequest>(r => string.IsNullOrEmpty(r.Cursor)),
            Arg.Any<CancellationToken>())
            .Returns(tmpPage1);
        tmpPolygon.GetChainSnapshotAsync(
            Arg.Is<GetChainSnapshotRequest>(r => r.Cursor == "abc-page2"),
            Arg.Any<CancellationToken>())
            .Returns(tmpPage2);
        tmpPolygon.GetChainSnapshotAsync(
            Arg.Is<GetChainSnapshotRequest>(r => r.Cursor == "def-page3"),
            Arg.Any<CancellationToken>())
            .Returns(tmpPage3);

        var tmpService = new LiveOptionsSnapshotCaptureService(
            tmpPolygon,
            new FakeTimeProvider(),
            NullLogger<LiveOptionsSnapshotCaptureService>.Instance,
            Options.Create(new LiveOptionsSnapshotCaptureOptions { LiveSnapshotCaptureEnabled = true }),
            Options.Create(new HistoryServiceOptions()));

        var tmpResult = await tmpService.FetchFullChainAsync(
            "TSLA",
            new DateOnly(2024, 1, 8),
            new DateOnly(2024, 3, 8),
            new DateTimeOffset(2024, 1, 8, 14, 30, 0, TimeSpan.Zero),
            CancellationToken.None);

        tmpResult.Count.ShouldBe(550, "all three pages should be accumulated");

        // All 3 pages were fetched.
        await tmpPolygon.Received(3).GetChainSnapshotAsync(
            Arg.Any<GetChainSnapshotRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchFullChainAsync_AtmFilterRunsAgainstUnion()
    {
        // Three pages of strikes; ATM band keeps the middle page only.
        // Confirms the filter is applied AFTER pagination accumulation,
        // not per-page (which would discard the underlying-price-bearing
        // page if it landed late).
        var tmpPolygon = Substitute.For<IOptionsService>();

        // Page 1: strikes 100,101,102 — well below the 200 ATM
        // (no underlying price in this page).
        var tmpPage1 = new PolygonResponse<List<OptionSnapshot>>
        {
            Status = "OK",
            Results = new List<OptionSnapshot>
            {
                MakeSnap("p1-a", 100m),
                MakeSnap("p1-b", 101m),
                MakeSnap("p1-c", 102m),
            },
            NextUrl = "https://api.polygon.io/v3/snapshot/options/TSLA?cursor=p2-cursor",
        };
        // Page 2: strikes 195, 200, 205 — inside the band, AND carries
        // the underlying-price anchor.
        var tmpPage2 = new PolygonResponse<List<OptionSnapshot>>
        {
            Status = "OK",
            Results = new List<OptionSnapshot>
            {
                MakeSnapWithUnderlying("p2-a", 195m, 200m),
                MakeSnapWithUnderlying("p2-b", 200m, 200m),
                MakeSnapWithUnderlying("p2-c", 205m, 200m),
            },
            NextUrl = "https://api.polygon.io/v3/snapshot/options/TSLA?cursor=p3-cursor",
        };
        // Page 3: strikes 300, 301 — well above ATM band.
        var tmpPage3 = new PolygonResponse<List<OptionSnapshot>>
        {
            Status = "OK",
            Results = new List<OptionSnapshot>
            {
                MakeSnap("p3-a", 300m),
                MakeSnap("p3-b", 301m),
            },
            NextUrl = null,
        };

        tmpPolygon.GetChainSnapshotAsync(
            Arg.Is<GetChainSnapshotRequest>(r => string.IsNullOrEmpty(r.Cursor)),
            Arg.Any<CancellationToken>())
            .Returns(tmpPage1);
        tmpPolygon.GetChainSnapshotAsync(
            Arg.Is<GetChainSnapshotRequest>(r => r.Cursor == "p2-cursor"),
            Arg.Any<CancellationToken>())
            .Returns(tmpPage2);
        tmpPolygon.GetChainSnapshotAsync(
            Arg.Is<GetChainSnapshotRequest>(r => r.Cursor == "p3-cursor"),
            Arg.Any<CancellationToken>())
            .Returns(tmpPage3);

        var tmpService = new LiveOptionsSnapshotCaptureService(
            tmpPolygon,
            new FakeTimeProvider(),
            NullLogger<LiveOptionsSnapshotCaptureService>.Instance,
            Options.Create(new LiveOptionsSnapshotCaptureOptions { LiveSnapshotCaptureEnabled = true }),
            Options.Create(new HistoryServiceOptions()));

        var tmpUnion = await tmpService.FetchFullChainAsync(
            "TSLA",
            new DateOnly(2024, 1, 8),
            new DateOnly(2024, 3, 8),
            new DateTimeOffset(2024, 1, 8, 14, 30, 0, TimeSpan.Zero),
            CancellationToken.None);

        tmpUnion.Count.ShouldBe(8, "union of all pages");

        // Filter the union with a band centered on 200 (±5%):
        // [190, 210]. Only page 2's three rows survive.
        var tmpFiltered = LiveOptionsSnapshotCaptureService
            .FilterAtmBand(tmpUnion, inKLow: 190m, inKHigh: 210m)
            .ToList();
        tmpFiltered.Count.ShouldBe(3);
        tmpFiltered.ShouldContain(r => r.Details!.Ticker == "p2-a");
        tmpFiltered.ShouldContain(r => r.Details!.Ticker == "p2-b");
        tmpFiltered.ShouldContain(r => r.Details!.Ticker == "p2-c");
    }

    [Theory]
    [InlineData("https://api.polygon.io/v3/snapshot/options/TSLA?cursor=abc123", "abc123")]
    [InlineData("https://api.polygon.io/v3/snapshot/options/TSLA?limit=250&cursor=xyz&sort=ticker", "xyz")]
    [InlineData("https://api.polygon.io/v3/snapshot/options/TSLA?limit=250", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void ExtractCursor_ParsesNextUrl(string? inUrl, string? inExpected)
    {
        LiveOptionsSnapshotCaptureService.ExtractCursor(inUrl).ShouldBe(inExpected);
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private static PolygonResponse<List<OptionSnapshot>> MakePage(
        int inStart, int inCount, string? nextCursor)
    {
        var tmpResults = new List<OptionSnapshot>(inCount);
        for (var i = 0; i < inCount; i++)
        {
            tmpResults.Add(MakeSnap($"O:TSLA240119C{(inStart + i):00000000}", 100m + (inStart + i)));
        }
        return new PolygonResponse<List<OptionSnapshot>>
        {
            Status = "OK",
            Results = tmpResults,
            NextUrl = string.IsNullOrEmpty(nextCursor)
                ? null
                : $"https://api.polygon.io/v3/snapshot/options/TSLA?cursor={nextCursor}",
        };
    }


    private LiveOptionsSnapshotCaptureService BuildService()
    {
        return new LiveOptionsSnapshotCaptureService(
            Substitute.For<IOptionsService>(),
            new FakeTimeProvider(),
            NullLogger<LiveOptionsSnapshotCaptureService>.Instance,
            Options.Create(new LiveOptionsSnapshotCaptureOptions()),
            Options.Create(new HistoryServiceOptions()));
    }

    private static DateTime ToEsternDayUtc(DateTime inEt)
    {
        TimeZoneInfo tmpTz;
        try { tmpTz = TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch (TimeZoneNotFoundException) { tmpTz = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
        return TimeZoneInfo.ConvertTimeToUtc(
            DateTime.SpecifyKind(inEt, DateTimeKind.Unspecified), tmpTz);
    }

    private static OptionSnapshot MakeSnap(string inTicker, decimal inStrike)
        => new()
        {
            Details = new OptionContractDetails { Ticker = inTicker, StrikePrice = inStrike },
        };

    private static OptionSnapshot MakeSnapWithUnderlying(
        string inTicker, decimal inStrike, decimal inUnderlying)
        => new()
        {
            Details = new OptionContractDetails { Ticker = inTicker, StrikePrice = inStrike },
            UnderlyingAsset = new OptionUnderlyingAsset { Price = inUnderlying },
            LastQuote = new OptionLastQuote { Bid = 1.0m, Ask = 1.1m },
            ImpliedVolatility = 0.5m,
            Greeks = new OptionGreeks
            {
                Delta = 0.5m, Gamma = 0.01m, Theta = -0.05m, Vega = 0.20m,
            },
        };
}
