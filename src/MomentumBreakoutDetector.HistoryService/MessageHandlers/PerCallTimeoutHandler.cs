using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MomentumBreakoutDetector.HistoryService.MessageHandlers;

/// <summary>
/// Layered into the polygon-net-client SDK's HTTP pipeline (Phase E) to
/// enforce a per-call ceiling on every Polygon request — independent of
/// the Refit-bound HttpClient.Timeout (which we leave at the SDK default
/// of 30s as a hard backstop). The fetcher-level cancellation tokens
/// passed into <c>GetAggregatesAsync</c> / <c>GetQuotesAsync</c> /
/// <c>GetListContractsAsync</c> already cover caller-driven cancellation;
/// this handler exists so a stuck Polygon socket gets killed at the
/// pipeline layer too rather than waiting on the fallback HttpClient
/// timeout.
///
/// Lifted from the per-call CancellationTokenSource pattern that lived
/// inside the original raw-HttpClient fetchers (PolygonBarFetcher,
/// PolygonNbboFetcher, PolygonChainFetcher) before the SDK refactor.
/// </summary>
public sealed class PerCallTimeoutHandler : DelegatingHandler
{
    private readonly ILogger<PerCallTimeoutHandler> m_Logger;
    private readonly int m_TimeoutMs;

    /// <summary>Default per-call ceiling. 10s — same as the original chain
    /// fetcher (the heaviest of the three Polygon endpoints we touch);
    /// bars / quotes routinely complete well under this.</summary>
    public const int DefaultTimeoutMs = 10000;

    public PerCallTimeoutHandler(
        IOptions<HistoryServiceOptions> inOpts,
        ILogger<PerCallTimeoutHandler> inLogger)
    {
        m_Logger = inLogger;
        var tmpV = inOpts.Value.PolygonPerCallTimeoutMs;
        m_TimeoutMs = tmpV > 0 ? tmpV : DefaultTimeoutMs;
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        using var tmpCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        tmpCts.CancelAfter(m_TimeoutMs);
        try
        {
            return await base.SendAsync(request, tmpCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (tmpCts.IsCancellationRequested
                                                 && !cancellationToken.IsCancellationRequested)
        {
            m_Logger.LogWarning(
                "Polygon HTTP request timed out after {TimeoutMs}ms ({Method} {Url})",
                m_TimeoutMs, request.Method, request.RequestUri);
            throw new TimeoutException(
                $"Polygon HTTP request timed out after {m_TimeoutMs}ms");
        }
    }
}
