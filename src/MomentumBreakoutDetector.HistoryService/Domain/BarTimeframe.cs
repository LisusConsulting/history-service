namespace MomentumBreakoutDetector.HistoryService.Domain;

/// <summary>
/// Internal timeframe enum used by the cache + fetcher layer. Mirrors
/// MBD `Domain.BarTimeframe` (PR #129). Maps to/from the proto
/// `BarTimeframe` enum at the gRPC edge.
/// </summary>
public enum BarTimeframe
{
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    OneDay
}
