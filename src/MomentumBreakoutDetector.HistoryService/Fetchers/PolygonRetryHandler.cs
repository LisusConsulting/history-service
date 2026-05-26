using System.Net;
using Microsoft.Extensions.Logging;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// Lightweight transient-retry handler for the named "polygon" HttpClient.
/// Mirrors the retry shape that lived inside MBD's
/// TreyThomasCodes.Polygon SDK (PR #129/#131): up to 3 attempts on
/// network exceptions, 5xx responses, 408 Request Timeout, and 429
/// Too Many Requests. Does NOT retry on other 4xx — those are
/// authoritative misses (NOT_AUTHORIZED, 404) that <see cref="PolygonBarFetcher"/>
/// already converts to empty results + miss markers.
///
/// 2026-05-11 (adversarial review): added 429 to the transient set.
/// Pre-fix, Polygon's rate-limit responses fell through to the
/// "authoritative 4xx" path, where the fetcher converts non-200
/// responses to empty results + miss markers. A cold-start backtest
/// that briefly throttled would then poison the cache with phantom
/// "no data" markers, forcing manual re-trigger to backfill the
/// rate-limited gap. Respect the `Retry-After` header when present
/// (Polygon emits it on rate-limit responses).
///
/// Kept as a hand-rolled handler rather than pulling in Polly to keep
/// the new service's dependency surface minimal — the retry logic is
/// ~30 lines of straight-line code.
/// </summary>
public sealed class PolygonRetryHandler : DelegatingHandler
{
    private readonly ILogger<PolygonRetryHandler> m_Logger;
    private readonly int m_MaxAttempts;

    public PolygonRetryHandler(ILogger<PolygonRetryHandler> inLogger, int inMaxAttempts = 3)
    {
        m_Logger = inLogger;
        m_MaxAttempts = inMaxAttempts > 0 ? inMaxAttempts : 3;
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Exception? tmpLastEx = null;
        HttpResponseMessage? tmpLastResp = null;

        for (int tmpAttempt = 1; tmpAttempt <= m_MaxAttempts; tmpAttempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                tmpLastResp?.Dispose();
                tmpLastResp = await base.SendAsync(request, cancellationToken);
                if (!IsTransient(tmpLastResp.StatusCode)) return tmpLastResp;

                if (tmpAttempt < m_MaxAttempts)
                {
                    // Polygon emits a Retry-After header (delta-seconds form,
                    // per RFC 7231) on rate-limit responses. Honor it when
                    // present — short-circuiting our exponential backoff with
                    // the upstream's own guidance prevents wasted retries
                    // that would just trip the same throttle.
                    var tmpDelay = TryGetRetryAfter(tmpLastResp) ?? BackoffFor(tmpAttempt);
                    m_Logger.LogWarning(
                        "Polygon transient {Status} on attempt {Attempt}/{Max} — retrying in {DelayMs}ms",
                        (int)tmpLastResp.StatusCode, tmpAttempt, m_MaxAttempts, (int)tmpDelay.TotalMilliseconds);
                    await Task.Delay(tmpDelay, cancellationToken);
                    continue;
                }
                return tmpLastResp;
            }
            catch (HttpRequestException ex) when (tmpAttempt < m_MaxAttempts)
            {
                tmpLastEx = ex;
                m_Logger.LogWarning(ex,
                    "Polygon network error on attempt {Attempt}/{Max} — retrying",
                    tmpAttempt, m_MaxAttempts);
                await Task.Delay(BackoffFor(tmpAttempt), cancellationToken);
            }
        }

        if (tmpLastResp is not null) return tmpLastResp;
        throw tmpLastEx ?? new HttpRequestException("Polygon retry handler exhausted attempts.");
    }

    private static bool IsTransient(HttpStatusCode inStatus)
        => (int)inStatus >= 500
        || inStatus == HttpStatusCode.RequestTimeout
        || inStatus == HttpStatusCode.TooManyRequests;

    private static TimeSpan BackoffFor(int inAttempt)
        => TimeSpan.FromMilliseconds(200 * Math.Pow(2, inAttempt - 1));

    /// <summary>
    /// Parse RFC 7231 Retry-After header. Polygon emits the delta-seconds
    /// form on 429 responses. Falls back to null if the header is missing,
    /// malformed, or specifies an absolute date instead of a delta. The
    /// caller substitutes the standard exponential backoff in that case.
    /// Cap at 30s to prevent a hostile / misconfigured upstream from
    /// forcing very long sleeps that exceed gRPC client deadlines.
    /// </summary>
    private static TimeSpan? TryGetRetryAfter(HttpResponseMessage inResp)
    {
        var tmpHdr = inResp.Headers.RetryAfter;
        if (tmpHdr is null) return null;
        if (tmpHdr.Delta is { } tmpDelta)
        {
            return tmpDelta > TimeSpan.FromSeconds(30) ? TimeSpan.FromSeconds(30) : tmpDelta;
        }
        if (tmpHdr.Date is { } tmpDate)
        {
            var tmpDelta2 = tmpDate.UtcDateTime - DateTime.UtcNow;
            if (tmpDelta2 <= TimeSpan.Zero) return TimeSpan.Zero;
            return tmpDelta2 > TimeSpan.FromSeconds(30) ? TimeSpan.FromSeconds(30) : tmpDelta2;
        }
        return null;
    }
}
