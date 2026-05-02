namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// US equity-options trading calendar — minimal hardcoded holiday list
/// covering the seed window (2025-11-02 → 2026-05-02 plus padding).
/// </summary>
/// <remarks>
/// Verified against the NYSE 2025/2026 holiday calendar (nyse.com/markets/hours-calendars).
/// Early-close half-days (Black Friday, Christmas Eve, Independence Day eve)
/// are NOT skipped — the seeder still pulls 9:30→13:00 RTH for those, and
/// the cache layer's miss-marker semantics handle the missing 13:00→16:00
/// window naturally on first access.
///
/// To extend the window past 2026-05-02, add entries to <see cref="s_Holidays"/>
/// from the NYSE source. Half-days are just an early close, not a holiday,
/// so they belong in <see cref="s_HalfDays"/> only if the caller wants to
/// short-circuit the afternoon.
/// </remarks>
public static class TradingCalendar
{
    // Full closures only. Half-days (early close at 13:00 ET) are
    // tracked separately because they are still trading days.
    private static readonly HashSet<DateOnly> s_Holidays = new()
    {
        // 2025
        new DateOnly(2025, 11, 27), // Thanksgiving Day
        new DateOnly(2025, 12, 25), // Christmas Day

        // 2026
        new DateOnly(2026, 1, 1),   // New Year's Day
        new DateOnly(2026, 1, 19),  // MLK Day
        new DateOnly(2026, 2, 16),  // Presidents Day
        new DateOnly(2026, 4, 3),   // Good Friday
    };

    private static readonly HashSet<DateOnly> s_HalfDays = new()
    {
        new DateOnly(2025, 11, 28), // Day after Thanksgiving (early close 13:00 ET)
        new DateOnly(2025, 12, 24), // Christmas Eve (early close 13:00 ET)
    };

    public static bool IsTradingDay(DateOnly inDate)
    {
        if (inDate.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday) return false;
        if (s_Holidays.Contains(inDate)) return false;
        return true;
    }

    public static bool IsHalfDay(DateOnly inDate) => s_HalfDays.Contains(inDate);

    /// <summary>
    /// Iterate every trading day in [from, to] inclusive, skipping
    /// weekends and full holidays.
    /// </summary>
    public static IEnumerable<DateOnly> EnumerateTradingDays(DateOnly inFrom, DateOnly inTo)
    {
        for (var tmpDate = inFrom; tmpDate <= inTo; tmpDate = tmpDate.AddDays(1))
        {
            if (IsTradingDay(tmpDate)) yield return tmpDate;
        }
    }
}
