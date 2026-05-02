using System.Diagnostics;
using Microsoft.Extensions.Logging;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using MomentumBreakoutDetector.HistoryService.Observability;
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

    /// <summary>
    /// Maximum allowed age of a returned quote relative to the requested
    /// bucket timestamp. /v3/quotes with <c>timestamp.lte</c> returns the
    /// most-recent NBBO at-or-before the bucket — for a contract that
    /// hasn't ticked yet in the session (e.g. Tuesday 09:30 open for an
    /// option last quoted Friday 16:14 close), Polygon returns the
    /// stale prior-session quote. Without this gate that quote would be
    /// persisted under the Tuesday-open bucket as if it were today's
    /// data. 300s mirrors the read-side fuzzy at-or-before window in
    /// <see cref="OptionQuotesProvider.DefaultStaleQuoteToleranceSeconds"/>:
    /// reads tolerate up to 5 min of staleness, so writes must too — but
    /// no more.
    /// </summary>
    public const int DefaultMaxQuoteAgeSeconds = 300;

    private readonly IOptionsService m_Options;
    private readonly ILogger<PolygonNbboFetcher> m_Logger;
    private readonly MetricsCollector? m_Metrics;
    private readonly SingleFlight<NbboFetchKey, PolygonNbboFetch> m_Coalescer = new();
    private readonly int m_MaxQuoteAgeSeconds;

    public PolygonNbboFetcher(
        IOptionsService inOptions,
        ILogger<PolygonNbboFetcher> inLogger,
        MetricsCollector? inMetrics = null,
        int inMaxQuoteAgeSeconds = DefaultMaxQuoteAgeSeconds)
    {
        m_Options = inOptions;
        m_Logger = inLogger;
        m_Metrics = inMetrics;
        m_MaxQuoteAgeSeconds = inMaxQuoteAgeSeconds > 0
            ? inMaxQuoteAgeSeconds
            : DefaultMaxQuoteAgeSeconds;
        m_Metrics?.RegisterInFlightProbe(MetricKind.Nbbo, () => m_Coalescer.InFlightCount);
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

        var tmpStopwatch = Stopwatch.StartNew();
        try
        {
            var tmpResp = await m_Options
                .GetQuotesAsync(tmpRequest, inCt)
                .ConfigureAwait(false);
            // Wire call landed (success or empty body); record the upstream
            // and latency. Exception paths below count as fetches too — we
            // record in their catch blocks.
            m_Metrics?.RecordUpstreamFetch(MetricKind.Nbbo, tmpStopwatch.Elapsed.TotalMilliseconds);

            var tmpQuote = tmpResp?.Results?.FirstOrDefault();
            if (tmpQuote is null
                || tmpQuote.BidPrice is null || tmpQuote.AskPrice is null
                || tmpQuote.SipTimestamp is null)
            {
                m_Logger.LogInformation(
                    "Polygon NBBO fetch: {Ticker} @ {Ts:O} → 0 quotes (miss) in {LatencyMs}ms",
                    inTicker, inTsUtc, tmpStopwatch.Elapsed.TotalMilliseconds);
                return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "no quote in window");
            }
            m_Logger.LogInformation(
                "Polygon NBBO fetch: {Ticker} @ {Ts:O} → 1 quote in {LatencyMs}ms",
                inTicker, inTsUtc, tmpStopwatch.Elapsed.TotalMilliseconds);

            // SIP timestamp is nanoseconds since epoch.
            var tmpAsOf = DateTimeOffset
                .FromUnixTimeMilliseconds(tmpQuote.SipTimestamp.Value / 1_000_000L)
                .UtcDateTime;

            // Freshness gate: /v3/quotes with timestamp.lte returns the
            // most-recent NBBO at-or-before the bucket — when a contract
            // hasn't ticked yet in the session, that's a prior-session
            // quote (e.g. Friday's 16:14 close for a Tuesday 09:30 bucket).
            // Reject anything older than DefaultMaxQuoteAgeSeconds so we
            // don't persist stale quotes under fresh-bucket keys.
            var tmpAgeSec = (inTsUtc - tmpAsOf).TotalSeconds;
            if (tmpAgeSec > m_MaxQuoteAgeSeconds)
            {
                m_Logger.LogInformation(
                    "Polygon NBBO fetch: {Ticker} @ {Ts:O} → stale quote (sip={Sip:O}, age={AgeSec:F0}s > {MaxAgeSec}s); treating as miss",
                    inTicker, inTsUtc, tmpAsOf, tmpAgeSec, m_MaxQuoteAgeSeconds);
                return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "stale-quote");
            }

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
            m_Metrics?.RecordUpstreamFetch(MetricKind.Nbbo, tmpStopwatch.Elapsed.TotalMilliseconds);
            m_Logger.LogInformation(
                "Quote NOT_AUTHORIZED for {Ticker} @ {Ts:O} — outside plan history depth",
                inTicker, inTsUtc);
            return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "plan-not-authorized");
        }
        catch (PolygonApiException ex) when (ex.IsNotFound)
        {
            m_Metrics?.RecordUpstreamFetch(MetricKind.Nbbo, tmpStopwatch.Elapsed.TotalMilliseconds);
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
