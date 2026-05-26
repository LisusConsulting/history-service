using Grpc.Core;

namespace MomentumBreakoutDetector.HistoryService.Validation;

/// <summary>
/// Server-side invariant: history-service serves past-day data only. Any
/// request whose range includes today or any future instant must be
/// rejected before any data fetch. Today's data must come from
/// MBD-local; this guard catches client-router bugs fast instead of
/// returning stale or empty payloads silently.
/// </summary>
/// <remarks>
/// "Today" is computed in America/New_York (handles DST). The boundary
/// is recomputed per request — a long-running request issued at 23:59 ET
/// the previous day is still validated against the right calendar day.
///
/// Boundary semantics: the boundary is the FIRST instant of today in ET,
/// converted to UTC. A request whose `to` is strictly less than the
/// boundary is entirely past-day. A request whose `to` is equal to the
/// boundary is treated as including today's first millisecond and is
/// rejected — the boundary is the inclusive lower bound for "today".
/// Don't silently truncate a straddling range either; that hides bugs in
/// the caller.
/// </remarks>
public static class PastOnlyRangeValidator
{
    private static readonly TimeZoneInfo s_EasternTz = ResolveEasternTz();

    /// <summary>
    /// Validate that a [from, to] range is entirely strictly before the
    /// today-in-ET boundary. Throws <see cref="RpcException"/> with
    /// <see cref="StatusCode.FailedPrecondition"/> on violation.
    /// </summary>
    /// <param name="inFromUtc">UTC start of the request range.</param>
    /// <param name="inToUtc">UTC end of the request range (inclusive).</param>
    public static void EnsurePastOnly(DateTime inFromUtc, DateTime inToUtc)
    {
        var tmpBoundaryUtc = ComputeTodayBoundaryUtc();
        // The whole range must be entirely below the boundary. Either
        // endpoint at-or-after the boundary fails. We check `to` first
        // (the common case is a backtest range that drifts past today),
        // then `from` (purely-future requests).
        if (inToUtc >= tmpBoundaryUtc || inFromUtc >= tmpBoundaryUtc)
        {
            throw BuildRejection(inFromUtc, inToUtc, tmpBoundaryUtc);
        }
    }

    /// <summary>
    /// Validate that a single point-in-time timestamp is strictly before
    /// the today-in-ET boundary. Used by point fetches like GetNbbo.
    /// </summary>
    public static void EnsurePastOnly(DateTime inTsUtc)
    {
        var tmpBoundaryUtc = ComputeTodayBoundaryUtc();
        if (inTsUtc >= tmpBoundaryUtc)
        {
            throw BuildRejection(inTsUtc, inTsUtc, tmpBoundaryUtc);
        }
    }

    private static DateTime ComputeTodayBoundaryUtc()
    {
        var tmpNowEt = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, s_EasternTz);

        // 2026-05-26 — post-close same-day override. When env var
        // HISTORY_POST_CLOSE_OPEN_AT_ET is set to "HH:mm" (ET) and the
        // current ET wall-clock is >= that time, today becomes past-day
        // (boundary advances to tomorrow's midnight ET). Use case:
        // live-vs-backtest divergence audit run after the close while
        // Polygon historical APIs already have full-day data.
        var tmpOverride = Environment.GetEnvironmentVariable("HISTORY_POST_CLOSE_OPEN_AT_ET");
        if (!string.IsNullOrWhiteSpace(tmpOverride) &&
            TimeSpan.TryParse(tmpOverride, out var tmpOpenAt))
        {
            var tmpOpenAtToday = tmpNowEt.Date.Add(tmpOpenAt);
            if (tmpNowEt >= tmpOpenAtToday)
            {
                // "Today is past" — boundary is tomorrow's midnight ET.
                var tmpTomorrowEtMidnight = new DateTime(
                    tmpNowEt.Year, tmpNowEt.Month, tmpNowEt.Day, 0, 0, 0, DateTimeKind.Unspecified)
                    .AddDays(1);
                return TimeZoneInfo.ConvertTimeToUtc(tmpTomorrowEtMidnight, s_EasternTz);
            }
        }

        var tmpTodayEtMidnight = new DateTime(
            tmpNowEt.Year, tmpNowEt.Month, tmpNowEt.Day, 0, 0, 0, DateTimeKind.Unspecified);
        return TimeZoneInfo.ConvertTimeToUtc(tmpTodayEtMidnight, s_EasternTz);
    }

    private static RpcException BuildRejection(DateTime inFromUtc, DateTime inToUtc, DateTime inBoundaryUtc)
    {
        var tmpBoundaryEt = TimeZoneInfo.ConvertTimeFromUtc(inBoundaryUtc, s_EasternTz);
        var tmpMessage =
            $"history-service serves past-day data only. " +
            $"Request range [{inFromUtc:O} → {inToUtc:O}] includes today " +
            $"(boundary: {tmpBoundaryEt:yyyy-MM-dd HH:mm:ss} ET). " +
            $"Today's data must come from MBD-local.";
        return new RpcException(new Status(StatusCode.FailedPrecondition, tmpMessage));
    }

    private static TimeZoneInfo ResolveEasternTz()
    {
        // Cross-platform: IANA on Linux/macOS, Windows tz id on Windows.
        // .NET 6+ accepts the IANA name on Windows too via ICU, but fall
        // back defensively in case of an unusual system tzdata setup.
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
    }
}
