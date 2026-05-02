using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using MomentumBreakoutDetector.HistoryService.Observability;
using Refit;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.Models.Options;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// Coalescer key for <see cref="PolygonChainFetcher"/>. The public entry
/// performs a full multi-page sweep, so the natural coalesce boundary is
/// (symbol, asOfDate) — 50 concurrent callers collapse to ONE multi-page
/// upstream sweep.
/// </summary>
internal readonly record struct ChainFetchKey(string Symbol, DateOnly AsOfDate);

/// <summary>
/// On-demand Polygon /v3/reference/options/contracts fetch. Phase E:
/// refactored from raw HttpClient to the polygon-net-client SDK 0.10.0
/// (<see cref="IOptionsService.GetListContractsRawAsync"/>). The SDK now
/// exposes <c>GetListContractsAsync</c> + a Raw variant — the missing
/// piece that forced the original lift to bypass the SDK and use raw
/// HttpClient.
///
/// Endpoint choice: /v3/reference/options/contracts?as_of=YYYY-MM-DD,
/// fully deterministic historical (Lisus-approved 2026-04-30). DIFFERENT
/// from /v3/snapshot/options/{TSLA} which PR #120 banned for replay.
///
/// Pagination: the SDK doesn't auto-paginate; we loop on next_url
/// (extracting the cursor) preserving the original 50-page safety cap.
///
/// Production semantics preserved:
///   - Concurrency cap + per-page timeout — moved to SDK pipeline
///     handlers (<c>ConcurrencyLimitingHandler</c> + <c>PerCallTimeoutHandler</c>).
///     The per-page timeout reset that lived in the original loop is now
///     inherent: each <c>GetListContractsRawAsync</c> call gets its own
///     pipeline pass and thus its own timeout window.
///   - Fail-quiet on 401/403 + 4xx body containing "NOT_AUTHORIZED" —
///     return empty (caller writes miss-marker).
///   - Fail-quiet on 404 + 429 — return empty (caller treats as miss for
///     this run).
///   - Fail-loud on 5xx — propagate (typed via Raw's
///     EnsureSuccessStatusCode call).
///   - Per-page <see cref="TimeoutException"/> from the timeout handler
///     → treat as miss-for-this-run (same as the original).
/// </summary>
public interface IPolygonChainFetcher
{
    /// <summary>
    /// Fetch the option-chain enumeration for <paramref name="inSymbol"/>
    /// as of <paramref name="inAsOfDate"/> from Polygon. Returns the full
    /// page-aggregated contract list (typically 200-500 rows on TSLA);
    /// returns an empty list when Polygon has no data (caller writes a
    /// miss marker). Returns empty on per-page timeout / 4xx (treat-as-miss
    /// for this run); throws on 5xx / network failures.
    /// </summary>
    Task<IReadOnlyList<OptionsContract>> FetchChainAsync(
        string inSymbol, DateOnly inAsOfDate, CancellationToken inCt);
}

/// <summary>
/// Strongly-typed Polygon options for the chain fetcher. Bound from the
/// <c>Polygon:</c> configuration section so secrets land via env var
/// (<c>Polygon__ApiKey</c>) rather than hard-coded in source.
/// </summary>
public sealed class PolygonOptions
{
    public const string SectionName = "Polygon";
    public string ApiKey { get; set; } = string.Empty;
    public string BaseUrl { get; set; } = "https://api.polygon.io";
}

public sealed class PolygonChainFetcher : IPolygonChainFetcher
{
    private readonly IOptionsService m_Options;
    private readonly ILogger<PolygonChainFetcher> m_Logger;
    private readonly MetricsCollector? m_Metrics;
    private readonly SingleFlight<ChainFetchKey, IReadOnlyList<OptionsContract>> m_Coalescer = new();

    /// <summary>
    /// Default per-call ceiling. Now enforced by <c>PerCallTimeoutHandler</c>
    /// in the SDK pipeline; kept as a const for readers and PR
    /// archaeology.
    /// </summary>
    public const int DefaultPerCallTimeoutMs = 10000;

    /// <summary>Default concurrency cap.</summary>
    public const int DefaultMaxConcurrentFetches = 8;

    /// <summary>Page size on the /v3/reference/options/contracts request.
    /// 1000 is Polygon's max.</summary>
    internal const int PageLimit = 1000;

    /// <summary>Defensive ceiling on pagination. 50 × 1000 = 50K rows.</summary>
    internal const int MaxPagesPerCall = 50;

    public PolygonChainFetcher(
        IOptionsService inOptions,
        ILogger<PolygonChainFetcher> inLogger,
        MetricsCollector? inMetrics = null)
    {
        m_Options = inOptions;
        m_Logger = inLogger;
        m_Metrics = inMetrics;
        m_Metrics?.RegisterInFlightProbe(MetricKind.Chains, () => m_Coalescer.InFlightCount);
    }

    /// <summary>
    /// Coalescer-wrapped public entry: 50 concurrent callers requesting
    /// the same (symbol, asOfDate) chain collapse to ONE multi-page
    /// upstream sweep.
    /// </summary>
    public Task<IReadOnlyList<OptionsContract>> FetchChainAsync(
        string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
    {
        var tmpKey = new ChainFetchKey(inSymbol, inAsOfDate);
        return m_Coalescer.ExecuteAsync(tmpKey,
            () => FetchChainAsync_Inner(inSymbol, inAsOfDate, inCt));
    }

    /// <summary>Diagnostic: in-flight coalescer entries.</summary>
    internal int InFlightCount => m_Coalescer.InFlightCount;

    private async Task<IReadOnlyList<OptionsContract>> FetchChainAsync_Inner(
        string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
    {
        if (string.IsNullOrEmpty(inSymbol)) return Array.Empty<OptionsContract>();

        var tmpAsOfStr = inAsOfDate.ToString("yyyy-MM-dd");

        var tmpAll = new List<OptionsContract>(PageLimit);
        string? tmpCursor = null;
        var tmpPage = 0;

        try
        {
            do
            {
                tmpPage++;

                var tmpRequest = new GetListContractsRequest
                {
                    UnderlyingTicker = inSymbol,
                    AsOf = tmpAsOfStr,
                    Limit = PageLimit,
                    Cursor = tmpCursor,
                };

                ApiResponse<PolygonResponse<List<OptionsContract>>> tmpResp;
                var tmpPageWatch = Stopwatch.StartNew();
                try
                {
                    tmpResp = await m_Options
                        .GetListContractsRawAsync(tmpRequest, inCt)
                        .ConfigureAwait(false);
                    // One pagination page = one wire call. Record the
                    // upstream + latency per page so percentiles reflect
                    // real Polygon round-trips, not full-sweep totals.
                    m_Metrics?.RecordUpstreamFetch(MetricKind.Chains, tmpPageWatch.Elapsed.TotalMilliseconds);
                }
                catch (TimeoutException)
                {
                    // Per-page timeout fired in the pipeline handler —
                    // treat as miss-for-this-run rather than fail-loud.
                    // A hung pagination page on a cold-start backtest
                    // shouldn't abort the entire run.
                    m_Logger.LogWarning(
                        "Polygon /v3/reference/options/contracts timed out for {Symbol} as_of {AsOf} (page {Page}) — treating as miss for this run",
                        inSymbol, tmpAsOfStr, tmpPage);
                    return Array.Empty<OptionsContract>();
                }

                if (!tmpResp.IsSuccessStatusCode)
                {
                    var tmpHandled = TryHandleNonSuccess(
                        tmpResp.StatusCode, tmpResp.Error?.Content, inSymbol, tmpAsOfStr);
                    if (tmpHandled is not null) return tmpHandled;
                    // 5xx / unexpected → throw, abort the run.
                    if (tmpResp.Error is not null) throw tmpResp.Error;
                    throw new HttpRequestException(
                        $"Polygon chain returned {(int)tmpResp.StatusCode} {tmpResp.StatusCode}");
                }

                var tmpBody = tmpResp.Content;

                // Polygon's "200 + status:ERROR/NOT_AUTHORIZED" quirk —
                // body status sometimes contradicts the HTTP status code.
                if (tmpBody is not null
                    && (string.Equals(tmpBody.Status, "NOT_AUTHORIZED", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(tmpBody.Status, "ERROR", StringComparison.OrdinalIgnoreCase)))
                {
                    m_Logger.LogInformation(
                        "Chains 200/body status={Status} for {Symbol} as_of {AsOf} (page {Page})",
                        tmpBody.Status, inSymbol, tmpAsOfStr, tmpPage);
                    return Array.Empty<OptionsContract>();
                }

                var tmpResults = tmpBody?.Results;
                if (tmpResults is not null && tmpResults.Count > 0)
                {
                    tmpAll.AddRange(tmpResults);
                }

                tmpCursor = ContractsBackfillCursorHelper.ExtractCursor(tmpBody?.NextUrl);
            } while (!string.IsNullOrEmpty(tmpCursor)
                     && tmpPage < MaxPagesPerCall
                     && !inCt.IsCancellationRequested);

            m_Logger.LogInformation(
                "Polygon chain fetch: {Symbol} as_of={AsOf} → {Rows} contracts across {Pages} page(s)",
                inSymbol, tmpAsOfStr, tmpAll.Count, tmpPage);
            return tmpAll;
        }
        catch (HttpRequestException ex)
        {
            // Network-level fault. Bubble so the caller fails loud rather
            // than silently running with a stale chain.
            m_Logger.LogError(ex,
                "Polygon chain fetch network error for {Symbol} as_of {AsOf}",
                inSymbol, tmpAsOfStr);
            throw;
        }
    }

    /// <summary>
    /// Map the original-lift's 4xx ladder onto raw HTTP status codes.
    /// Returns an empty list (treat-as-miss for this run) for the cases
    /// the original code caught with named exceptions; returns null for
    /// "not handled — caller should EnsureSuccessStatusCode".
    /// </summary>
    private IReadOnlyList<OptionsContract>? TryHandleNonSuccess(
        HttpStatusCode inStatus, string? inBody, string inSymbol, string inAsOfStr)
    {
        var tmpBodyLooksUnauthorized =
            (inBody?.Contains("NOT_AUTHORIZED", StringComparison.OrdinalIgnoreCase) ?? false)
            || (inBody?.Contains("not entitled", StringComparison.OrdinalIgnoreCase) ?? false);

        if (inStatus is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden
            || tmpBodyLooksUnauthorized)
        {
            m_Logger.LogInformation(
                "Chains NOT_AUTHORIZED for {Symbol} as_of {AsOf} — outside plan history depth",
                inSymbol, inAsOfStr);
            return Array.Empty<OptionsContract>();
        }

        if (inStatus == HttpStatusCode.NotFound)
        {
            m_Logger.LogInformation("Chains 404 for {Symbol} as_of {AsOf}", inSymbol, inAsOfStr);
            return Array.Empty<OptionsContract>();
        }

        if (inStatus == HttpStatusCode.TooManyRequests)
        {
            m_Logger.LogWarning(
                "Chains 429 rate-limited for {Symbol} as_of {AsOf} — treating as miss for this run",
                inSymbol, inAsOfStr);
            return Array.Empty<OptionsContract>();
        }

        return null;
    }
}

/// <summary>
/// Cursor extraction helper. Lifted-as-is from the original
/// ContractsBackfillService — same query-string parsing.
/// </summary>
internal static class ContractsBackfillCursorHelper
{
    public static string? ExtractCursor(string? inNextUrl)
    {
        if (string.IsNullOrEmpty(inNextUrl)) return null;
        var tmpIdx = inNextUrl.IndexOf("cursor=", StringComparison.Ordinal);
        if (tmpIdx < 0) return null;
        var tmpRest = inNextUrl[(tmpIdx + "cursor=".Length)..];
        var tmpAmp = tmpRest.IndexOf('&');
        return tmpAmp < 0 ? tmpRest : tmpRest[..tmpAmp];
    }
}
