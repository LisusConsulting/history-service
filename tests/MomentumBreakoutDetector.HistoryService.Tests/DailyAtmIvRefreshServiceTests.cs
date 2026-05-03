using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using MomentumBreakoutDetector.HistoryService.HostedServices;
using MomentumBreakoutDetector.HistoryService.Providers;
using NSubstitute;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Wave C / PR 6 — schedule + per-cycle behaviour tests for
/// <see cref="DailyAtmIvRefreshService"/>. Mirrors the test shape of
/// <see cref="DailyOptionsFlowRefreshServiceTests"/> (the cron is the
/// same template applied to a different upstream computer).
/// </summary>
public class DailyAtmIvRefreshServiceTests
{
    private static DailyAtmIvRefreshService BuildService(
        IDailyAtmIvAggregator inAggregator,
        IDailyAtmIvProvider inProvider,
        TimeProvider inTimeProvider,
        DailyAtmIvRefreshOptions? inOpts = null)
    {
        return new DailyAtmIvRefreshService(
            inAggregator,
            inProvider,
            inTimeProvider,
            NullLogger<DailyAtmIvRefreshService>.Instance,
            Options.Create(inOpts ?? new DailyAtmIvRefreshOptions
            {
                Symbols = new List<string> { "TSLA" },
                FireHourEt = 8,
                FireMinuteEt = 0,
            }));
    }

    [Fact]
    public void ComputeNextFireUtc_TuesdayBefore8Et_FiresToday8Et()
    {
        // Tuesday 2026-01-06 06:00 ET = 11:00 UTC (EST = UTC-5).
        var tmpNow = new DateTimeOffset(2026, 1, 6, 11, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyAtmIvAggregator>(),
            Substitute.For<IDailyAtmIvProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        // 08:00 ET on the same day = 13:00 UTC (EST).
        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 6, 13, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void ComputeNextFireUtc_TuesdayAfter8Et_FiresWednesday8Et()
    {
        var tmpNow = new DateTimeOffset(2026, 1, 6, 19, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyAtmIvAggregator>(),
            Substitute.For<IDailyAtmIvProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 7, 13, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void ComputeNextFireUtc_FridayAfter8Et_SkipsToMonday()
    {
        var tmpNow = new DateTimeOffset(2026, 1, 9, 19, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyAtmIvAggregator>(),
            Substitute.For<IDailyAtmIvProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        // Mon 2026-01-12 08:00 ET = 13:00 UTC.
        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 12, 13, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void ComputeNextFireUtc_SaturdayMorning_SkipsToMonday()
    {
        var tmpNow = new DateTimeOffset(2026, 1, 10, 11, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyAtmIvAggregator>(),
            Substitute.For<IDailyAtmIvProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 12, 13, 0, 0, TimeSpan.Zero));
    }

    [Theory]
    [InlineData(2026, 1, 5,  2026, 1, 2)]   // Mon → Fri
    [InlineData(2026, 1, 6,  2026, 1, 5)]   // Tue → Mon
    [InlineData(2026, 1, 20, 2026, 1, 16)]  // Tue 1/20 → Fri 1/16 (MLK Day = Mon 1/19)
    [InlineData(2026, 4, 6,  2026, 4, 2)]   // Mon 4/6 → Thu 4/2 (Good Friday 4/3)
    public void PreviousTradingDay_VariousAnchors_WalksToPriorTradingDay(
        int inFromY, int inFromM, int inFromD,
        int inExpY, int inExpM, int inExpD)
    {
        var tmpFrom = new DateOnly(inFromY, inFromM, inFromD);
        var tmpExpected = new DateOnly(inExpY, inExpM, inExpD);

        DailyAtmIvRefreshService.PreviousTradingDay(tmpFrom).ShouldBe(tmpExpected);
    }

    [Fact]
    public async Task RunOnceAsync_AggregatorReturnsRow_ProviderReceivesOneUpsert()
    {
        var tmpNow = new DateTimeOffset(2026, 1, 6, 14, 0, 0, TimeSpan.Zero);
        var tmpExpectedRow = new DailyAtmIvRow(
            UnderlyingTicker: "TSLA",
            TradeDate: new DateOnly(2026, 1, 5),
            AtmIv: 0.4567m,
            ContractCount: 42);

        var tmpAgg = Substitute.For<IDailyAtmIvAggregator>();
        tmpAgg.AggregateAsync("TSLA", new DateOnly(2026, 1, 5), Arg.Any<CancellationToken>())
            .Returns(tmpExpectedRow);

        var tmpProvider = Substitute.For<IDailyAtmIvProvider>();
        var tmpSvc = BuildService(tmpAgg, tmpProvider, new FakeTimeProvider(tmpNow));

        await tmpSvc.RunOnceAsync(CancellationToken.None);

        await tmpAgg.Received(1).AggregateAsync(
            "TSLA", new DateOnly(2026, 1, 5), Arg.Any<CancellationToken>());
        await tmpProvider.Received(1).UpsertAsync(
            Arg.Is<IReadOnlyList<DailyAtmIvRow>>(r =>
                r.Count == 1
                && r[0].TradeDate == new DateOnly(2026, 1, 5)
                && r[0].AtmIv == 0.4567m
                && r[0].ContractCount == 42),
            Arg.Any<CancellationToken>());
        await tmpProvider.DidNotReceive().RecordMissAsync(
            Arg.Any<string>(), Arg.Any<DateOnly>(), Arg.Any<DateOnly>(),
            Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunOnceAsync_AggregatorReturnsNull_ProviderReceivesMissMarker()
    {
        var tmpNow = new DateTimeOffset(2026, 1, 6, 14, 0, 0, TimeSpan.Zero);

        var tmpAgg = Substitute.For<IDailyAtmIvAggregator>();
        tmpAgg.AggregateAsync("TSLA", new DateOnly(2026, 1, 5), Arg.Any<CancellationToken>())
            .Returns((DailyAtmIvRow?)null);

        var tmpProvider = Substitute.For<IDailyAtmIvProvider>();
        var tmpSvc = BuildService(tmpAgg, tmpProvider, new FakeTimeProvider(tmpNow));

        await tmpSvc.RunOnceAsync(CancellationToken.None);

        await tmpProvider.Received(1).RecordMissAsync(
            "TSLA", new DateOnly(2026, 1, 5), new DateOnly(2026, 1, 5),
            "no-snapshot-rows", Arg.Any<CancellationToken>());
        await tmpProvider.DidNotReceive().UpsertAsync(
            Arg.Any<IReadOnlyList<DailyAtmIvRow>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunOnceAsync_MultipleSymbols_FailureInOneDoesNotStopOthers()
    {
        var tmpNow = new DateTimeOffset(2026, 1, 6, 14, 0, 0, TimeSpan.Zero);

        var tmpAgg = Substitute.For<IDailyAtmIvAggregator>();
        tmpAgg.AggregateAsync("TSLA", Arg.Any<DateOnly>(), Arg.Any<CancellationToken>())
            .Returns<DailyAtmIvRow?>(_ => throw new InvalidOperationException("TSLA blew up"));
        tmpAgg.AggregateAsync("AAPL", new DateOnly(2026, 1, 5), Arg.Any<CancellationToken>())
            .Returns(new DailyAtmIvRow("AAPL", new DateOnly(2026, 1, 5), 0.32m, 18));

        var tmpProvider = Substitute.For<IDailyAtmIvProvider>();
        var tmpSvc = BuildService(
            tmpAgg, tmpProvider, new FakeTimeProvider(tmpNow),
            new DailyAtmIvRefreshOptions
            {
                Symbols = new List<string> { "TSLA", "AAPL" },
                FireHourEt = 8,
                FireMinuteEt = 0,
            });

        await tmpSvc.RunOnceAsync(CancellationToken.None);

        // AAPL still got UPSERTed despite TSLA throwing.
        await tmpProvider.Received(1).UpsertAsync(
            Arg.Is<IReadOnlyList<DailyAtmIvRow>>(r =>
                r.Count == 1 && r[0].UnderlyingTicker == "AAPL"),
            Arg.Any<CancellationToken>());
    }
}
