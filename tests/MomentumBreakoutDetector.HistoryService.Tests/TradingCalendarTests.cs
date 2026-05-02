using MomentumBreakoutDetector.HistoryService.Domain;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Unit tests for the shared <see cref="TradingCalendar"/> utility.
///
/// <para>
/// Coverage:
/// <list type="bullet">
///   <item>Holiday-list correctness across 2022 → 2026 (sample of named
///         dates, plus boundary cases).</item>
///   <item>Half-day list correctness (Black Friday, Christmas Eve etc.).</item>
///   <item><see cref="TradingCalendar.GetSessionMinutes"/> shape per
///         session × normal/half-day combination.</item>
///   <item>EnumerateTradingDays / GetTradingDays semantics.</item>
/// </list>
/// </para>
/// </summary>
public sealed class TradingCalendarTests
{
    // ── IsTradingDay / IsHalfDay ────────────────────────────────────────

    [Theory]
    [InlineData(2025, 11, 26, true)]  // Wednesday before Thanksgiving — open
    [InlineData(2025, 11, 27, false)] // Thanksgiving Day — closed
    [InlineData(2025, 11, 28, true)]  // Day after Thanksgiving — half-day open
    [InlineData(2025, 12, 24, true)]  // Christmas Eve — half-day open
    [InlineData(2025, 12, 25, false)] // Christmas Day — closed
    [InlineData(2026, 1, 1, false)]   // New Year's Day — closed
    [InlineData(2026, 1, 19, false)]  // MLK Day — closed
    [InlineData(2026, 4, 3, false)]   // Good Friday — closed
    [InlineData(2026, 7, 3, false)]   // Independence Day observed (Jul 4 = Sat)
    [InlineData(2025, 1, 9, false)]   // Carter mourning day — closed
    public void IsTradingDay_KnownHolidaysClosed(int y, int m, int d, bool expected)
    {
        TradingCalendar.IsTradingDay(new DateOnly(y, m, d)).ShouldBe(expected);
    }

    [Theory]
    [InlineData(2025, 11, 8, false)]  // Saturday
    [InlineData(2025, 11, 9, false)]  // Sunday
    [InlineData(2025, 11, 10, true)]  // Monday — open
    [InlineData(2025, 11, 14, true)]  // Friday — open
    public void IsTradingDay_WeekendsClosed_WeekdaysOpen(int y, int m, int d, bool expected)
    {
        TradingCalendar.IsTradingDay(new DateOnly(y, m, d)).ShouldBe(expected);
    }

    [Theory]
    [InlineData(2025, 11, 28, true)]  // Day after Thanksgiving
    [InlineData(2025, 12, 24, true)]  // Christmas Eve
    [InlineData(2025, 12, 23, false)] // Day before Christmas Eve — full day
    [InlineData(2026, 11, 27, true)]  // Day after Thanksgiving 2026
    public void IsHalfDay_RecognisedDates(int y, int m, int d, bool expected)
    {
        TradingCalendar.IsHalfDay(new DateOnly(y, m, d)).ShouldBe(expected);
    }

    // ── EnumerateTradingDays / GetTradingDays ───────────────────────────

    [Fact]
    public void EnumerateTradingDays_SkipsHolidaysAndWeekends()
    {
        // Thanksgiving week 2025: Mon-Wed open, Thu closed (Thanksgiving),
        // Fri half-day (still open).
        var tmpDays = TradingCalendar.EnumerateTradingDays(
            new DateOnly(2025, 11, 24), new DateOnly(2025, 11, 30)).ToList();
        tmpDays.ShouldBe(new[]
        {
            new DateOnly(2025, 11, 24),
            new DateOnly(2025, 11, 25),
            new DateOnly(2025, 11, 26),
            new DateOnly(2025, 11, 28), // half-day still trading
        });
    }

    [Fact]
    public void GetTradingDays_AliasesEnumerate()
    {
        var tmpFrom = new DateOnly(2026, 4, 1);
        var tmpTo = new DateOnly(2026, 4, 10);
        var tmpListA = TradingCalendar.EnumerateTradingDays(tmpFrom, tmpTo).ToList();
        var tmpListB = TradingCalendar.GetTradingDays(tmpFrom, tmpTo);
        tmpListB.ShouldBe(tmpListA);
    }

    [Fact]
    public void EnumerateTradingDays_GoodFriday2026_Skipped()
    {
        // 2026-04-03 is Good Friday. The full week 2026-03-30..2026-04-03
        // should yield Mon-Thu only.
        var tmpDays = TradingCalendar.EnumerateTradingDays(
            new DateOnly(2026, 3, 30), new DateOnly(2026, 4, 3)).ToList();
        tmpDays.ShouldBe(new[]
        {
            new DateOnly(2026, 3, 30),
            new DateOnly(2026, 3, 31),
            new DateOnly(2026, 4, 1),
            new DateOnly(2026, 4, 2),
        });
    }

    // ── GetSessionMinutes ───────────────────────────────────────────────

    [Fact]
    public void GetSessionMinutes_RthFullDay_Yields390Minutes()
    {
        // Wednesday, well clear of any holiday. RTH = 09:30..16:00 ET = 390min.
        var tmpDate = new DateOnly(2026, 4, 15);
        var tmpMinutes = TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.Rth).ToList();
        tmpMinutes.Count.ShouldBe(390);
        tmpMinutes.First().Kind.ShouldBe(DateTimeKind.Utc);
        // Apr 15 2026 falls in EDT (UTC-4). 09:30 ET = 13:30 UTC.
        tmpMinutes.First().ShouldBe(new DateTime(2026, 4, 15, 13, 30, 0, DateTimeKind.Utc));
        // Last minute is 15:59 ET = 19:59 UTC (the bar opening at 15:59,
        // closing at 16:00).
        tmpMinutes.Last().ShouldBe(new DateTime(2026, 4, 15, 19, 59, 0, DateTimeKind.Utc));
    }

    [Fact]
    public void GetSessionMinutes_RthHalfDay_Yields210Minutes()
    {
        // 2025-11-28 (Day after Thanksgiving) — half-day. RTH = 09:30..13:00 ET = 210min.
        var tmpDate = new DateOnly(2025, 11, 28);
        var tmpMinutes = TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.Rth).ToList();
        tmpMinutes.Count.ShouldBe(210);
        // Nov falls in EST (UTC-5). 09:30 ET = 14:30 UTC.
        tmpMinutes.First().ShouldBe(new DateTime(2025, 11, 28, 14, 30, 0, DateTimeKind.Utc));
        // Last minute is 12:59 ET = 17:59 UTC.
        tmpMinutes.Last().ShouldBe(new DateTime(2025, 11, 28, 17, 59, 0, DateTimeKind.Utc));
    }

    [Fact]
    public void GetSessionMinutes_RthOnHoliday_YieldsEmpty()
    {
        var tmpDate = new DateOnly(2025, 12, 25); // Christmas
        var tmpMinutes = TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.Rth).ToList();
        tmpMinutes.ShouldBeEmpty();
    }

    [Fact]
    public void GetSessionMinutes_PreMarket_Yields330Minutes()
    {
        // PreMarket = 04:00..09:30 ET = 5h30 = 330min.
        var tmpDate = new DateOnly(2026, 4, 15);
        var tmpMinutes = TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.PreMarket).ToList();
        tmpMinutes.Count.ShouldBe(330);
    }

    [Fact]
    public void GetSessionMinutes_AfterHours_Yields240Minutes()
    {
        // AfterHours = 16:00..20:00 ET = 4h = 240min.
        var tmpDate = new DateOnly(2026, 4, 15);
        var tmpMinutes = TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.AfterHours).ToList();
        tmpMinutes.Count.ShouldBe(240);
    }

    [Fact]
    public void GetSessionMinutes_AfterHoursHalfDay_Starts1300Et()
    {
        // Half-day after-hours starts at 13:00 ET (the early close), runs
        // to 20:00 ET = 7h = 420min.
        var tmpDate = new DateOnly(2025, 11, 28);
        var tmpMinutes = TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.AfterHours).ToList();
        tmpMinutes.Count.ShouldBe(420);
        // 13:00 EST = 18:00 UTC.
        tmpMinutes.First().ShouldBe(new DateTime(2025, 11, 28, 18, 0, 0, DateTimeKind.Utc));
    }

    [Fact]
    public void GetSessionMinutes_ExtendedHours_Yields960Minutes()
    {
        // ExtendedHours = 04:00..20:00 ET = 16h = 960min.
        var tmpDate = new DateOnly(2026, 4, 15);
        var tmpMinutes = TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.ExtendedHours).ToList();
        tmpMinutes.Count.ShouldBe(960);
    }

    [Fact]
    public void GetSessionMinutes_EnumerableYieldsUtcKind()
    {
        var tmpDate = new DateOnly(2026, 4, 15);
        foreach (var tmpTs in TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.Rth))
        {
            tmpTs.Kind.ShouldBe(DateTimeKind.Utc);
        }
    }

    // ── Cache-correspondence sanity check ───────────────────────────────

    /// <summary>
    /// Sanity: the calendar's expected RTH minute set for a known full
    /// trading day in mid-April 2026 contains exactly the 390 expected
    /// timestamps with no duplicates and no off-by-one. This catches
    /// regressions where a refactor accidentally yields 389 or 391.
    /// </summary>
    [Fact]
    public void GetSessionMinutes_NoDuplicates_NoOffByOne()
    {
        var tmpDate = new DateOnly(2026, 4, 15);
        var tmpMinutes = TradingCalendar.GetSessionMinutes(tmpDate, TradingSession.Rth).ToList();
        tmpMinutes.Distinct().Count().ShouldBe(tmpMinutes.Count);
        // Exclusivity at the close: 16:00 ET is NOT a yielded minute (it's
        // the close, not a bar-open). 19:59 UTC (= 15:59 ET) is the last.
        tmpMinutes.ShouldNotContain(new DateTime(2026, 4, 15, 20, 0, 0, DateTimeKind.Utc));
    }
}
