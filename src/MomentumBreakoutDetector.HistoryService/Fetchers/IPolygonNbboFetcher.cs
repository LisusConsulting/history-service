namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// Result of a Polygon /v3/quotes NBBO point-in-time lookup. Null
/// when Polygon returned no quote in-window or the contract is outside
/// the plan's history depth (these are recorded as miss-markers by the
/// provider). Non-null when a real quote was found and should be
/// written through to the cache.
/// </summary>
public sealed record PolygonNbboResult(
    string Ticker,
    DateTime RequestedTsUtc,
    DateTime AsOfTsUtc,
    decimal BidPrice,
    decimal AskPrice,
    int? BidSize,
    int? AskSize,
    int? BidExchange,
    int? AskExchange);

/// <summary>
/// Outcome of a Polygon NBBO fetch attempt. Mirrors the three terminal
/// states the source MBD service handled (PR #98 + #121):
///   - <see cref="Hit"/>: a quote was returned. Provider writes through.
///   - <see cref="Miss"/>: Polygon returned no quote in-window OR plan
///     said "not entitled" for this date. Provider records a miss-marker.
///   - <see cref="Transient"/>: timeout or upstream blip. Provider does
///     NOT mark a miss; a future call retries.
/// </summary>
public enum PolygonNbboOutcome
{
    Hit = 0,
    Miss = 1,
    Transient = 2,
}

/// <summary>
/// Wraps a NBBO fetch attempt: outcome + payload (only populated on Hit).
/// MissReason is logged + persisted alongside the miss-marker so we can
/// later distinguish "no quote in window" from "plan-not-authorized".
/// </summary>
public sealed record PolygonNbboFetch(
    PolygonNbboOutcome Outcome,
    PolygonNbboResult? Quote,
    string? MissReason);

public interface IPolygonNbboFetcher
{
    /// <summary>
    /// Fetch the most-recent NBBO at-or-before <paramref name="inTsUtc"/>
    /// for <paramref name="inTicker"/> (O:-prefixed option ticker).
    /// </summary>
    Task<PolygonNbboFetch> FetchAsync(
        string inTicker,
        DateTime inTsUtc,
        CancellationToken inCt = default);
}
