namespace MomentumBreakoutDetector.HistoryService.Domain;

/// <summary>
/// Authoritative U.S. equity market calendar (vendored slim copy of
/// <c>MomentumBreakoutDetector.Domain.Services.IMarketCalendar</c>).
///
/// <para>
/// 2026-05-12: the history-service's <see cref="TradingCalendar"/>
/// static class previously kept a hardcoded NYSE holiday list through
/// 2026 — would silently stop being accurate Jan 2027. This interface
/// + <see cref="AlpacaMarketCalendar"/> implementation route the static
/// methods through Alpaca's <c>/v2/calendar</c> for arbitrary dates
/// (past, present, future). The static class keeps its existing API
/// (caller code unchanged); when DI sets <see cref="TradingCalendar.Source"/>
/// at startup, lookups route through this interface. The hardcoded
/// holiday list remains as the last-resort fallback.
/// </para>
/// </summary>
public interface IMarketCalendar
{
    /// <summary>True iff the given ET calendar date has a market session
    /// (normal-close OR early-close). False on weekends + full holidays.
    /// Returns <c>null</c> when the calendar can't answer (cache miss +
    /// upstream unreachable) so the caller can fall back to the
    /// hardcoded list.</summary>
    bool? TryIsTradingDay(DateOnly inEtDate);

    /// <summary>True iff the given date is a half-day (early close at
    /// 13:00 ET per the calendar's session). Null on can't-answer.</summary>
    bool? TryIsHalfDay(DateOnly inEtDate);

    /// <summary>Session detail for the given ET date, or null if
    /// weekend / full holiday. Returns null also on can't-answer; the
    /// caller distinguishes via <see cref="TryIsTradingDay"/>.</summary>
    MarketSession? GetSession(DateOnly inEtDate);
}

/// <summary>One market-session row, ET-local.</summary>
public sealed record MarketSession(
    DateOnly EtDate,
    TimeOnly OpenEt,
    TimeOnly CloseEt,
    bool IsEarlyClose);
