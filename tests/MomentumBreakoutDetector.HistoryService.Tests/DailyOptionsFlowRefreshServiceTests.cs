using Microsoft.Extensions.DependencyInjection;
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
/// PR 3 — schedule + per-cycle behaviour tests for
/// <see cref="DailyOptionsFlowRefreshService"/>. Uses
/// <see cref="FakeTimeProvider"/> so the cron's "wall clock" can be
/// driven through known weekday/weekend boundaries deterministically.
///
/// <para>
/// What's exercised here:
/// <list type="bullet">
///   <item>The <c>ComputeNextFireUtc</c> schedule pure function:
///         Tue–Fri at 08:00 ET fires the next weekday at 08:00 ET; Fri
///         after 08:00 → next is Mon (skip Sat/Sun); current time on a
///         Sat/Sun → next is Mon; before-08:00 ET on a weekday → today
///         at 08:00 ET.</item>
///   <item>The <c>PreviousTradingDay</c> calendar walk: Mon → Fri,
///         Tue → Mon (or earlier if Mon is a holiday), Wed → Tue.</item>
///   <item>One <c>RunOnceAsync</c> happy-path: computer returns a row →
///         provider receives a single UPSERT for the previous trading
///         day; computer returns null → provider receives RecordMissAsync
///         instead.</item>
/// </list>
/// </para>
/// </summary>
public class DailyOptionsFlowRefreshServiceTests
{
    private static DailyOptionsFlowRefreshService BuildService(
        IDailyOptionsFlowComputer inComputer,
        IDailyOptionsFlowProvider inProvider,
        TimeProvider inTimeProvider,
        DailyOptionsFlowRefreshOptions? inOpts = null)
    {
        // 2026-05-15: cron now warms IOptionChainProvider via a scoped
        // resolution before computing. Tests don't exercise the warmup
        // step; the cron catches resolution failures gracefully so an
        // empty scope-factory (no IOptionChainProvider registered) just
        // logs a warning and proceeds to the compute step under test.
        return new DailyOptionsFlowRefreshService(
            inComputer,
            inProvider,
            new EmptyScopeFactory(),
            inTimeProvider,
            NullLogger<DailyOptionsFlowRefreshService>.Instance,
            Options.Create(inOpts ?? new DailyOptionsFlowRefreshOptions
            {
                Symbols = new List<string> { "TSLA" },
                FireHourEt = 8,
                FireMinuteEt = 0,
            }));
    }

    /// <summary>
    /// Minimal IServiceScopeFactory for the cron's chain-warmup step.
    /// Returns a scope whose provider has no IOptionChainProvider
    /// registered — GetRequiredService throws, the cron's try/catch
    /// logs a warning, and the test path under examination (the
    /// compute step) runs as before.
    /// </summary>
    private sealed class EmptyScopeFactory : IServiceScopeFactory
    {
        public IServiceScope CreateScope() => new EmptyScope();
        private sealed class EmptyScope : IServiceScope
        {
            public IServiceProvider ServiceProvider { get; } = new EmptyProvider();
            public void Dispose() { }
            private sealed class EmptyProvider : IServiceProvider
            {
                public object? GetService(Type serviceType) => null;
            }
        }
    }

    [Fact]
    public void ComputeNextFireUtc_TuesdayBefore8Et_FiresToday8Et()
    {
        // Tuesday 2026-01-06 06:00 ET = 11:00 UTC (EST = UTC-5).
        var tmpNow = new DateTimeOffset(2026, 1, 6, 11, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyOptionsFlowComputer>(),
            Substitute.For<IDailyOptionsFlowProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        // 08:00 ET on the same day = 13:00 UTC (EST).
        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 6, 13, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void ComputeNextFireUtc_TuesdayAfter8Et_FiresWednesday8Et()
    {
        // Tuesday 2026-01-06 14:00 ET = 19:00 UTC (already past 08:00 ET).
        var tmpNow = new DateTimeOffset(2026, 1, 6, 19, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyOptionsFlowComputer>(),
            Substitute.For<IDailyOptionsFlowProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        // Wed 2026-01-07 08:00 ET = 13:00 UTC.
        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 7, 13, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void ComputeNextFireUtc_FridayAfter8Et_SkipsToMonday()
    {
        // Friday 2026-01-09 14:00 ET = 19:00 UTC.
        var tmpNow = new DateTimeOffset(2026, 1, 9, 19, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyOptionsFlowComputer>(),
            Substitute.For<IDailyOptionsFlowProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        // Mon 2026-01-12 08:00 ET = 13:00 UTC. (Sat 01-10, Sun 01-11 skipped.)
        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 12, 13, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void ComputeNextFireUtc_SaturdayMorning_SkipsToMonday()
    {
        // Saturday 2026-01-10 06:00 ET = 11:00 UTC. Even though it's
        // before 08:00 ET, Sat is not a fire day.
        var tmpNow = new DateTimeOffset(2026, 1, 10, 11, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyOptionsFlowComputer>(),
            Substitute.For<IDailyOptionsFlowProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 12, 13, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void ComputeNextFireUtc_SundayEvening_FiresMonday()
    {
        // Sunday 2026-01-11 22:00 ET = Mon 03:00 UTC.
        var tmpNow = new DateTimeOffset(2026, 1, 12, 3, 0, 0, TimeSpan.Zero);
        var tmpSvc = BuildService(
            Substitute.For<IDailyOptionsFlowComputer>(),
            Substitute.For<IDailyOptionsFlowProvider>(),
            new FakeTimeProvider(tmpNow));

        var tmpNext = tmpSvc.ComputeNextFireUtc(tmpNow);

        // Mon 2026-01-12 08:00 ET = 13:00 UTC.
        tmpNext.ShouldBe(new DateTimeOffset(2026, 1, 12, 13, 0, 0, TimeSpan.Zero));
    }

    [Theory]
    [InlineData(2026, 1, 5,  2026, 1, 2)]   // Mon → Fri (prev trading day)
    [InlineData(2026, 1, 6,  2026, 1, 5)]   // Tue → Mon
    [InlineData(2026, 1, 7,  2026, 1, 6)]   // Wed → Tue
    [InlineData(2026, 1, 20, 2026, 1, 16)]  // Tue 1/20 → Fri 1/16 (Mon 1/19 = MLK Day)
    [InlineData(2026, 4, 6,  2026, 4, 2)]   // Mon 4/6 → Thu 4/2 (Fri 4/3 = Good Friday)
    public void PreviousTradingDay_VariousAnchors_WalksToPriorTradingDay(
        int inFromY, int inFromM, int inFromD,
        int inExpY, int inExpM, int inExpD)
    {
        var tmpFrom = new DateOnly(inFromY, inFromM, inFromD);
        var tmpExpected = new DateOnly(inExpY, inExpM, inExpD);

        DailyOptionsFlowRefreshService.PreviousTradingDay(tmpFrom)
            .ShouldBe(tmpExpected);
    }

    [Fact]
    public async Task RunOnceAsync_ComputerReturnsRow_ProviderReceivesOneUpsert()
    {
        // Tuesday 2026-01-06 09:00 ET — cron just fired. Previous
        // trading day = Mon 2026-01-05.
        var tmpNow = new DateTimeOffset(2026, 1, 6, 14, 0, 0, TimeSpan.Zero);
        var tmpExpectedRow = new DailyOptionsFlowRow(
            UnderlyingTicker: "TSLA",
            TradeDate: new DateOnly(2026, 1, 5),
            CallVolume: 600L, PutVolume: 550L,
            CallOi: 0L, PutOi: 0L,
            PutCallRatio: 0.9167m, FlowScore: 0.0583m,
            ContractCount: 5);

        var tmpComputer = Substitute.For<IDailyOptionsFlowComputer>();
        tmpComputer.ComputeAsync(
            "TSLA", new DateOnly(2026, 1, 5),
            Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(tmpExpectedRow);

        var tmpProvider = Substitute.For<IDailyOptionsFlowProvider>();
        var tmpSvc = BuildService(tmpComputer, tmpProvider, new FakeTimeProvider(tmpNow));

        await tmpSvc.RunOnceAsync(CancellationToken.None);

        await tmpComputer.Received(1).ComputeAsync(
            "TSLA", new DateOnly(2026, 1, 5),
            Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        await tmpProvider.Received(1).UpsertAsync(
            Arg.Is<IReadOnlyList<DailyOptionsFlowRow>>(r =>
                r.Count == 1 && r[0].TradeDate == new DateOnly(2026, 1, 5) && r[0].CallVolume == 600L),
            Arg.Any<CancellationToken>());
        await tmpProvider.DidNotReceive().RecordMissAsync(
            Arg.Any<string>(), Arg.Any<DateOnly>(), Arg.Any<DateOnly>(),
            Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunOnceAsync_ComputerReturnsNull_ProviderReceivesMissMarker()
    {
        var tmpNow = new DateTimeOffset(2026, 1, 6, 14, 0, 0, TimeSpan.Zero);

        var tmpComputer = Substitute.For<IDailyOptionsFlowComputer>();
        tmpComputer.ComputeAsync(
            "TSLA", new DateOnly(2026, 1, 5),
            Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns((DailyOptionsFlowRow?)null);

        var tmpProvider = Substitute.For<IDailyOptionsFlowProvider>();
        var tmpSvc = BuildService(tmpComputer, tmpProvider, new FakeTimeProvider(tmpNow));

        await tmpSvc.RunOnceAsync(CancellationToken.None);

        await tmpProvider.Received(1).RecordMissAsync(
            "TSLA", new DateOnly(2026, 1, 5), new DateOnly(2026, 1, 5),
            "no-chain-rows-cached", Arg.Any<CancellationToken>());
        await tmpProvider.DidNotReceive().UpsertAsync(
            Arg.Any<IReadOnlyList<DailyOptionsFlowRow>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunOnceAsync_MultipleSymbols_FailureInOneDoesNotStopOthers()
    {
        // Verify per-symbol fail-quiet: TSLA throws, AAPL still runs.
        var tmpNow = new DateTimeOffset(2026, 1, 6, 14, 0, 0, TimeSpan.Zero);

        var tmpComputer = Substitute.For<IDailyOptionsFlowComputer>();
        tmpComputer.ComputeAsync(
            "TSLA", Arg.Any<DateOnly>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns<DailyOptionsFlowRow?>(_ => throw new InvalidOperationException("TSLA blew up"));
        tmpComputer.ComputeAsync(
            "AAPL", new DateOnly(2026, 1, 5),
            Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new DailyOptionsFlowRow(
                "AAPL", new DateOnly(2026, 1, 5),
                100L, 80L, 0L, 0L,
                0.8m, 0.14m, 4));

        var tmpProvider = Substitute.For<IDailyOptionsFlowProvider>();
        var tmpSvc = BuildService(
            tmpComputer, tmpProvider, new FakeTimeProvider(tmpNow),
            new DailyOptionsFlowRefreshOptions
            {
                Symbols = new List<string> { "TSLA", "AAPL" },
                FireHourEt = 8,
                FireMinuteEt = 0,
            });

        // Should NOT throw — per-symbol catch wraps each iteration.
        await tmpSvc.RunOnceAsync(CancellationToken.None);

        // AAPL still got UPSERTed despite TSLA's failure.
        await tmpProvider.Received(1).UpsertAsync(
            Arg.Is<IReadOnlyList<DailyOptionsFlowRow>>(r =>
                r.Count == 1 && r[0].UnderlyingTicker == "AAPL"),
            Arg.Any<CancellationToken>());
    }
}
