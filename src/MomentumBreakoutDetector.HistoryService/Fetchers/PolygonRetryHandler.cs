using System.Net;
using Microsoft.Extensions.Logging;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// Lightweight transient-retry handler for the named "polygon" HttpClient.
/// Mirrors the retry shape that lived inside MBD's
/// TreyThomasCodes.Polygon SDK (PR #129/#131): up to 3 attempts on
/// network exceptions and 5xx responses, with a short exponential
/// backoff (200 ms, 400 ms). Does NOT retry on 4xx — those are
/// authoritative misses (NOT_AUTHORIZED, 404) that <see cref="PolygonBarFetcher"/>
/// already converts to empty results + miss markers.
///
/// Kept as a hand-rolled handler rather than pulling in Polly to keep
/// the new service's dependency surface minimal — the retry logic is
/// 30 lines of straight-line code.
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
                    m_Logger.LogWarning(
                        "Polygon transient {Status} on attempt {Attempt}/{Max} — retrying",
                        (int)tmpLastResp.StatusCode, tmpAttempt, m_MaxAttempts);
                    await Task.Delay(BackoffFor(tmpAttempt), cancellationToken);
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
        => (int)inStatus >= 500 || inStatus == HttpStatusCode.RequestTimeout;

    private static TimeSpan BackoffFor(int inAttempt)
        => TimeSpan.FromMilliseconds(200 * Math.Pow(2, inAttempt - 1));
}
