namespace MomentumBreakoutDetector.HistoryService.Domain;

/// <summary>
/// Trading-session enum used by <see cref="TradingCalendar.GetSessionMinutes"/>
/// to compute the minute-set the cache is expected to cover for a given
/// trading day. RTH = 09:30–16:00 ET; PreMarket = 04:00–09:30 ET;
/// AfterHours = 16:00–20:00 ET; ExtendedHours = full 04:00–20:00 ET window.
/// Times collapse to 09:30–13:00 (RTH) on half-days; pre/after windows are
/// undefined for half-days and currently treated as full-window.
/// </summary>
public enum TradingSession
{
    Rth,
    PreMarket,
    AfterHours,
    ExtendedHours,
}

/// <summary>
/// US equity / options trading calendar — hardcoded NYSE holiday and
/// half-day list, plus session-minute enumerators used by the on-demand
/// gap-detection logic in the Providers layer.
/// </summary>
/// <remarks>
/// <para>
/// Lifted from the seeder tool (<c>tools/seed/.../TradingCalendar.cs</c>)
/// during the intra-range gap-detection rework — the providers need the
/// same calendar to compute "expected coverage" per trading day, and
/// duplicating the holiday list across two projects is a maintenance
/// hazard. The seeder now project-references this assembly.
/// </para>
/// <para>
/// Verified against the NYSE 2025/2026 holiday calendar
/// (nyse.com/markets/hours-calendars). Window covered: 2022-01-03
/// → 2026-12-31 (sufficient for the 6-month seed window plus the
/// 12-month retention horizon). Extend <see cref="s_Holidays"/> /
/// <see cref="s_HalfDays"/> when the window slides.
/// </para>
/// <para>
/// All session-minute enumerators yield UTC <see cref="DateTime"/> with
/// <c>Kind=Utc</c>. ET → UTC conversion is daylight-savings-aware: we
/// resolve "Eastern Standard Time" via the Windows-or-IANA lookup so a
/// linux Docker host and a Windows dev box agree on the wall-clock
/// boundary.
/// </para>
/// </remarks>
public static class TradingCalendar
{
    // Full closures only. Half-days (early close at 13:00 ET) are
    // tracked separately because they are still trading days.
    private static readonly HashSet<DateOnly> s_Holidays = new()
    {
        // 2022
        new DateOnly(2022, 1, 17),  // MLK Day
        new DateOnly(2022, 2, 21),  // Presidents Day
        new DateOnly(2022, 4, 15),  // Good Friday
        new DateOnly(2022, 5, 30),  // Memorial Day
        new DateOnly(2022, 6, 20),  // Juneteenth (observed)
        new DateOnly(2022, 7, 4),   // Independence Day
        new DateOnly(2022, 9, 5),   // Labor Day
        new DateOnly(2022, 11, 24), // Thanksgiving Day
        new DateOnly(2022, 12, 26), // Christmas Day (observed)

        // 2023
        new DateOnly(2023, 1, 2),   // New Year's Day (observed)
        new DateOnly(2023, 1, 16),  // MLK Day
        new DateOnly(2023, 2, 20),  // Presidents Day
        new DateOnly(2023, 4, 7),   // Good Friday
        new DateOnly(2023, 5, 29),  // Memorial Day
        new DateOnly(2023, 6, 19),  // Juneteenth
        new DateOnly(2023, 7, 4),   // Independence Day
        new DateOnly(2023, 9, 4),   // Labor Day
        new DateOnly(2023, 11, 23), // Thanksgiving Day
        new DateOnly(2023, 12, 25), // Christmas Day

        // 2024
        new DateOnly(2024, 1, 1),   // New Year's Day
        new DateOnly(2024, 1, 15),  // MLK Day
        new DateOnly(2024, 2, 19),  // Presidents Day
        new DateOnly(2024, 3, 29),  // Good Friday
        new DateOnly(2024, 5, 27),  // Memorial Day
        new DateOnly(2024, 6, 19),  // Juneteenth
        new DateOnly(2024, 7, 4),   // Independence Day
        new DateOnly(2024, 9, 2),   // Labor Day
        new DateOnly(2024, 11, 28), // Thanksgiving Day
        new DateOnly(2024, 12, 25), // Christmas Day

        // 2025
        new DateOnly(2025, 1, 1),   // New Year's Day
        new DateOnly(2025, 1, 9),   // National Day of Mourning — President Carter (NYSE closed)
        new DateOnly(2025, 1, 20),  // MLK Day
        new DateOnly(2025, 2, 17),  // Presidents Day
        new DateOnly(2025, 4, 18),  // Good Friday
        new DateOnly(2025, 5, 26),  // Memorial Day
        new DateOnly(2025, 6, 19),  // Juneteenth
        new DateOnly(2025, 7, 4),   // Independence Day
        new DateOnly(2025, 9, 1),   // Labor Day
        new DateOnly(2025, 11, 27), // Thanksgiving Day
        new DateOnly(2025, 12, 25), // Christmas Day

        // 2026
        new DateOnly(2026, 1, 1),   // New Year's Day
        new DateOnly(2026, 1, 19),  // MLK Day
        new DateOnly(2026, 2, 16),  // Presidents Day
        new DateOnly(2026, 4, 3),   // Good Friday
        new DateOnly(2026, 5, 25),  // Memorial Day
        new DateOnly(2026, 6, 19),  // Juneteenth
        new DateOnly(2026, 7, 3),   // Independence Day (observed — July 4 falls Saturday)
        new DateOnly(2026, 9, 7),   // Labor Day
        new DateOnly(2026, 11, 26), // Thanksgiving Day
        new DateOnly(2026, 12, 25), // Christmas Day
    };

    private static readonly HashSet<DateOnly> s_HalfDays = new()
    {
        // 2022
        new DateOnly(2022, 7, 1),   // Pre-Independence Day (some years)
        new DateOnly(2022, 11, 25), // Day after Thanksgiving
        // 2023
        new DateOnly(2023, 7, 3),   // Pre-Independence Day
        new DateOnly(2023, 11, 24), // Day after Thanksgiving
        // 2024
        new DateOnly(2024, 7, 3),   // Pre-Independence Day
        new DateOnly(2024, 11, 29), // Day after Thanksgiving
        new DateOnly(2024, 12, 24), // Christmas Eve
        // 2025
        new DateOnly(2025, 7, 3),   // Pre-Independence Day
        new DateOnly(2025, 11, 28), // Day after Thanksgiving
        new DateOnly(2025, 12, 24), // Christmas Eve
        // 2026
        new DateOnly(2026, 11, 27), // Day after Thanksgiving
        new DateOnly(2026, 12, 24), // Christmas Eve
    };

    /// <summary>Eastern timezone resolved via cross-platform lookup.
    /// Linux Docker uses IANA "America/New_York"; Windows uses
    /// "Eastern Standard Time". We try both so the same code runs in
    /// the gRPC service container and in the dev-box test runner.</summary>
    private static readonly TimeZoneInfo s_EasternTz = ResolveEasternTimezone();

    private static TimeZoneInfo ResolveEasternTimezone()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch (TimeZoneNotFoundException) { /* fall through */ }
        catch (InvalidTimeZoneException) { /* fall through */ }

        try { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
        catch (TimeZoneNotFoundException) { /* fall through */ }
        catch (InvalidTimeZoneException) { /* fall through */ }

        // Last resort: fixed -05:00 (EST) — wrong half the year but
        // better than crashing. The DI seam should ensure the system tz
        // database is populated before this gets hit.
        return TimeZoneInfo.CreateCustomTimeZone("EST-Fallback",
            TimeSpan.FromHours(-5), "EST-Fallback", "EST-Fallback");
    }

    /// <summary>
    /// Convert a wall-clock Eastern (ET) date+time-of-day to UTC using
    /// the resolved Eastern timezone. DST-aware: in winter (EST) "midnight ET"
    /// is 05:00 UTC; in summer (EDT) it is 04:00 UTC. Used by the bars
    /// gap-detector to compute expected daily-bar timestamps that match
    /// the cached row shape (which stores midnight-ET-as-UTC).
    /// </summary>
    /// <remarks>
    /// Returns a <see cref="DateTime"/> with <see cref="DateTimeKind.Utc"/>.
    /// Mirrors the <c>ToUtc</c> helper formerly inlined in
    /// <c>LiveOptionsSnapshotCaptureService</c>; consolidated here so
    /// callers across the service share one DST policy.
    /// </remarks>
    public static DateTime ConvertEasternToUtc(DateOnly inEtDate, TimeSpan inEtTimeOfDay)
    {
        var tmpEt = new DateTime(
            inEtDate.Year, inEtDate.Month, inEtDate.Day, 0, 0, 0,
            DateTimeKind.Unspecified) + inEtTimeOfDay;
        return DateTime.SpecifyKind(
            TimeZoneInfo.ConvertTimeToUtc(tmpEt, s_EasternTz),
            DateTimeKind.Utc);
    }

    /// <summary>True iff the given date is a weekday and not a full
    /// NYSE closure. Half-days return <c>true</c> — they are still
    /// trading days, just with shortened sessions.</summary>
    public static bool IsTradingDay(DateOnly inDate)
    {
        if (inDate.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday) return false;
        if (s_Holidays.Contains(inDate)) return false;
        return true;
    }

    /// <summary>True iff the given date is a half-day (early close at 13:00 ET).</summary>
    public static bool IsHalfDay(DateOnly inDate) => s_HalfDays.Contains(inDate);

    /// <summary>
    /// Iterate every trading day in [<paramref name="inFrom"/>,
    /// <paramref name="inTo"/>] inclusive, skipping weekends and full
    /// holidays. Original seeder API name; keep as the canonical entry.
    /// </summary>
    public static IEnumerable<DateOnly> EnumerateTradingDays(DateOnly inFrom, DateOnly inTo)
    {
        for (var tmpDate = inFrom; tmpDate <= inTo; tmpDate = tmpDate.AddDays(1))
        {
            if (IsTradingDay(tmpDate)) yield return tmpDate;
        }
    }

    /// <summary>Materialize the trading-day set as a list. Convenience
    /// alias matching the brief's API spec. For large windows prefer
    /// <see cref="EnumerateTradingDays"/> and stream.</summary>
    public static IReadOnlyList<DateOnly> GetTradingDays(DateOnly inFrom, DateOnly inTo)
        => EnumerateTradingDays(inFrom, inTo).ToList();

    /// <summary>
    /// Enumerate the minute-bucketed UTC timestamps the cache is expected
    /// to cover for the given trading-day session. Used by the providers'
    /// gap-detection logic to compute <c>expected − cached − marked</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each yielded <see cref="DateTime"/> has <c>Kind=Utc</c> and
    /// represents the OPEN of a 1-minute bar. RTH on a normal day yields
    /// 390 minutes (09:30 → 16:00 ET, exclusive of 16:00). On a half-day
    /// RTH yields 210 minutes (09:30 → 13:00 ET).
    /// </para>
    /// <para>
    /// PreMarket / AfterHours / ExtendedHours yield their full
    /// pre-/post-RTH bands; half-days are NOT shortened on those bands
    /// (the half-day rule only applies to the RTH window per NYSE).
    /// </para>
    /// </remarks>
    public static IEnumerable<DateTime> GetSessionMinutes(
        DateOnly inDate, TradingSession inSession)
    {
        if (!IsTradingDay(inDate)) yield break;

        // Compute the session's ET wall-clock open + close for inDate.
        var tmpHalfDay = IsHalfDay(inDate);
        TimeSpan tmpOpen, tmpClose;
        switch (inSession)
        {
            case TradingSession.Rth:
                tmpOpen = new TimeSpan(9, 30, 0);
                tmpClose = tmpHalfDay ? new TimeSpan(13, 0, 0) : new TimeSpan(16, 0, 0);
                break;
            case TradingSession.PreMarket:
                tmpOpen = new TimeSpan(4, 0, 0);
                tmpClose = new TimeSpan(9, 30, 0);
                break;
            case TradingSession.AfterHours:
                // Half-day after-hours starts at 13:00, regular at 16:00.
                tmpOpen = tmpHalfDay ? new TimeSpan(13, 0, 0) : new TimeSpan(16, 0, 0);
                tmpClose = new TimeSpan(20, 0, 0);
                break;
            case TradingSession.ExtendedHours:
                tmpOpen = new TimeSpan(4, 0, 0);
                tmpClose = new TimeSpan(20, 0, 0);
                break;
            default:
                yield break;
        }

        // ET → UTC: build a DateTime "unspecified", convert via the resolved
        // tz. Crossing DST boundary mid-session is impossible for US equity
        // hours (DST switches at 2 AM ET, well before 04:00 pre-market open),
        // so a single conversion at session-open is safe.
        var tmpEtBase = new DateTime(inDate.Year, inDate.Month, inDate.Day, 0, 0, 0,
            DateTimeKind.Unspecified);
        var tmpEtOpen = tmpEtBase + tmpOpen;
        var tmpEtClose = tmpEtBase + tmpClose;
        var tmpUtcOpen = TimeZoneInfo.ConvertTimeToUtc(tmpEtOpen, s_EasternTz);
        var tmpUtcClose = TimeZoneInfo.ConvertTimeToUtc(tmpEtClose, s_EasternTz);

        // Yield each minute-open in [open, close).
        for (var tmpTs = tmpUtcOpen; tmpTs < tmpUtcClose; tmpTs = tmpTs.AddMinutes(1))
        {
            yield return DateTime.SpecifyKind(tmpTs, DateTimeKind.Utc);
        }
    }
}
