using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using TreyThomasCodes.Polygon.RestClient.Exceptions;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// Coalescer key for <see cref="PolygonNbboFetcher"/>: same (ticker,
/// timestamp) → single upstream /v3/quotes call.
/// </summary>
internal readonly record struct NbboFetchKey(string Ticker, DateTime Ts);

/// <summary>
/// On-demand Polygon /v3/quotes NBBO fetch. Phase E: refactored from raw
/// HttpClient to the polygon-net-client SDK 0.10.0
/// (<see cref="IOptionsService.GetQuotesAsync(GetQuotesRequest, CancellationToken)"/>).
///
/// Same wire call as the original lift: <c>/v3/quotes/{ticker}?timestamp.lte=…</c>
/// with <c>order=desc</c> and <c>limit=1</c> to fetch the most recent NBBO
/// at-or-before the requested timestamp.
///
/// Production semantics preserved:
///   - Concurrency cap + per-call timeout — moved to the SDK pipeline
///     handlers (<c>ConcurrencyLimitingHandler</c> + <c>PerCallTimeoutHandler</c>).
///   - Three-state outcome (Hit / Miss / Transient) preserved verbatim.
///   - 401/403 + body containing "not authorized" → Miss with reason
///     "plan-not-authorized" (caller writes miss-marker).
///   - 429 + 5xx → Transient (caller does NOT write miss-marker; future
///     calls retry).
///
/// Note on SDK choice: the typed <c>GetQuotesAsync</c> method translates
/// non-success responses into <see cref="PolygonApiException"/>; we catch
/// that exception type and inspect <c>IsUnauthorized</c> / <c>IsForbidden</c>
/// / <c>IsRateLimited</c> to fan out into the same Hit/Miss/Transient
/// shape the original code derived from raw HttpStatusCode. The Options
/// service does NOT expose a Raw variant for /v3/quotes (only for
/// /v3/snapshot/options/{...}), so the typed API is the cleanest fit
/// here.
/// </summary>
public sealed class PolygonNbboFetcher : IPolygonNbboFetcher
{
    public const int DefaultPerCallTimeoutMs = 3000;
    public const int DefaultMaxConcurrentFetches = 8;

    private readonly IOptionsService m_Options;
    private readonly ILogger<PolygonNbboFetcher> m_Logger;
    private readonly SingleFlight<NbboFetchKey, PolygonNbboFetch> m_Coalescer = new();

    public PolygonNbboFetcher(
        IOptionsService inOptions,
        ILogger<PolygonNbboFetcher> inLogger)
    {
        m_Options = inOptions;
        m_Logger = inLogger;
    }

    /// <summary>
    /// IOptions overload retained for parity with the legacy ctor surface
    /// — DI bindings that previously bound <see cref="IOptions{HistoryServiceOptions}"/>
    /// still resolve. The options bag is unused: timeouts + concurrency
    /// live in the pipeline handlers now.
    /// </summary>
    public PolygonNbboFetcher(
        IOptionsService inOptions,
        IOptions<HistoryServiceOptions> _,
        ILogger<PolygonNbboFetcher> inLogger)
        : this(inOptions, inLogger)
    {
    }

    /// <summary>
    /// Coalescer-wrapped public entry: 100 concurrent callers asking for
    /// the same (ticker, ts) collapse to one upstream /v3/quotes call.
    /// </summary>
    public Task<PolygonNbboFetch> FetchAsync(
        string inTicker,
        DateTime inTsUtc,
        CancellationToken inCt = default)
    {
        var tmpKey = new NbboFetchKey(inTicker, inTsUtc);
        return m_Coalescer.ExecuteAsync(tmpKey,
            () => FetchAsync_Inner(inTicker, inTsUtc, inCt));
    }

    /// <summary>Diagnostic: in-flight coalescer entries.</summary>
    internal int InFlightCount => m_Coalescer.InFlightCount;

    private async Task<PolygonNbboFetch> FetchAsync_Inner(
        string inTicker,
        DateTime inTsUtc,
        CancellationToken inCt)
    {
        // Polygon accepts ISO-8601 with seconds, in UTC. e.g.
        // "2025-12-15T15:30:00Z". timestamp.lte + order=desc + limit=1
        // returns the most recent NBBO at-or-before the requested ts.
        var tmpTsParam = inTsUtc.ToString("yyyy-MM-ddTHH:mm:ssZ");
        var tmpRequest = new GetQuotesRequest
        {
            OptionsTicker = inTicker,
            TimestampLte = tmpTsParam,
            Order = "desc",
            Limit = 1,
        };

        try
        {
            var tmpResp = await m_Options
                .GetQuotesAsync(tmpRequest, inCt)
                .ConfigureAwait(false);

            var tmpQuote = tmpResp?.Results?.FirstOrDefault();
            if (tmpQuote is null
                || tmpQuote.BidPrice is null || tmpQuote.AskPrice is null
                || tmpQuote.SipTimestamp is null)
            {
                return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "no quote in window");
            }

            // SIP timestamp is nanoseconds since epoch.
            var tmpAsOf = DateTimeOffset
                .FromUnixTimeMilliseconds(tmpQuote.SipTimestamp.Value / 1_000_000L)
                .UtcDateTime;

            return new PolygonNbboFetch(
                PolygonNbboOutcome.Hit,
                new PolygonNbboResult(
                    Ticker: inTicker,
                    RequestedTsUtc: inTsUtc,
                    AsOfTsUtc: tmpAsOf,
                    BidPrice: tmpQuote.BidPrice.Value,
                    AskPrice: tmpQuote.AskPrice.Value,
                    BidSize: tmpQuote.BidSize,
                    AskSize: tmpQuote.AskSize,
                    BidExchange: tmpQuote.BidExchange,
                    AskExchange: tmpQuote.AskExchange),
                MissReason: null);
        }
        catch (PolygonApiException ex) when (ex.IsUnauthorized || ex.IsForbidden)
        {
            // Plan-tier limit. Polygon returns NOT_AUTHORIZED outside the
            // subscription's history depth (Options Advanced /v3 quotes
            // start 2022-03-07).
            m_Logger.LogInformation(
                "Quote NOT_AUTHORIZED for {Ticker} @ {Ts:O} — outside plan history depth",
                inTicker, inTsUtc);
            return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "plan-not-authorized");
        }
        catch (PolygonApiException ex) when (ex.IsNotFound)
        {
            // 404 — same shape as a Miss. Caller writes a marker.
            return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "not-found");
        }
        catch (PolygonApiException ex) when (ex.IsRateLimited)
        {
            m_Logger.LogWarning(
                "Quote 429 rate-limited for {Ticker} @ {Ts:O} — transient",
                inTicker, inTsUtc);
            return new PolygonNbboFetch(PolygonNbboOutcome.Transient, null, "rate-limited");
        }
        catch (TimeoutException)
        {
            // PerCallTimeoutHandler fired. Don't poison the cache —
            // future call retries.
            m_Logger.LogWarning(
                "Polygon quote fetch timed out for {Ticker} @ {Ts:O} — transient",
                inTicker, inTsUtc);
            return new PolygonNbboFetch(PolygonNbboOutcome.Transient, null, "timeout");
        }
        catch (Exception ex)
        {
            // 5xx (PolygonApiException with IsServerError), HttpRequestException,
            // JsonException, etc. Don't poison the cache on transient
            // upstream blips — same intent as the original lift.
            m_Logger.LogWarning(ex,
                "Polygon quote fetch failed for {Ticker} @ {Ts:O}",
                inTicker, inTsUtc);
            return new PolygonNbboFetch(PolygonNbboOutcome.Transient, null, "exception");
        }
    }
}
