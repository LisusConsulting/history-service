namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Result of a NBBO lookup served by the provider.
///   <list type="bullet">
///     <item><see cref="Quote"/> non-null + <see cref="IsMissMarker"/> false:
///       a real NBBO. <see cref="CacheHit"/> distinguishes a DB / in-memory
///       hit (true) from a fresh upstream fetch that was just written through (false).</item>
///     <item><see cref="Quote"/> null + <see cref="IsMissMarker"/> true:
///       a recorded miss-marker matched. Caller must NOT retry.</item>
///     <item><see cref="Quote"/> null + <see cref="IsMissMarker"/> false:
///       transient upstream failure (timeout / exception). Caller may retry.</item>
///   </list>
/// </summary>
public sealed record OptionQuotesLookup(
    OptionQuoteRecord? Quote,
    bool CacheHit,
    bool IsMissMarker);

/// <summary>
/// In-memory + cache projection of one NBBO row. Mirrors the
/// historical_options_quotes columns the provider hydrates from postgres.
/// </summary>
public sealed record OptionQuoteRecord(
    string Ticker,
    DateTime RequestedTsUtc,
    DateTime AsOfTsUtc,
    decimal BidPrice,
    decimal AskPrice,
    int? BidSize,
    int? AskSize,
    int? BidExchange,
    int? AskExchange);

public interface IOptionQuotesProvider
{
    Task<OptionQuotesLookup> GetAtOrBeforeAsync(
        string inTicker,
        DateTime inTsUtc,
        CancellationToken inCt = default);
}
