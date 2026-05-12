using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.Logging;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Observability;
using Refit;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.Models.Stocks;
using TreyThomasCodes.Polygon.RestClient.Requests.Stocks;
using TreyThomasCodes.Polygon.RestClient.Services;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// On-demand Polygon /v2/aggs bar fetch. Phase E: refactored from raw
/// HttpClient to the polygon-net-client SDK 0.10.0 (<see cref="IStocksService"/>).
///
/// Production semantics preserved verbatim:
///   - Per-call timeout — moved to <c>PerCallTimeoutHandler</c> in the
///     SDK's HTTP pipeline (see Program.cs DI wiring).
///   - Concurrency cap — moved to <c>ConcurrencyLimitingHandler</c> in the
///     SDK's HTTP pipeline.
///   - Fail-quiet on 401/403/404/429 — return empty list (caller writes a
///     miss marker). The SDK translates non-success into
///     <c>PolygonApiException</c>; we use the Raw variant
///     (<c>GetBarsRawAsync</c>) so we can inspect <see cref="HttpStatusCode"/>
///     directly + sniff the body for Polygon's "200 + status:NOT_AUTHORIZED"
///     quirk.
///   - Fail-loud on 5xx / network / timeout — propagate so the caller
///     surfaces as BacktestFailed.
///
/// Range-fetch shape: a single Polygon /v2/aggs call returns bars for a
/// [from, to] window, so a single fetch closes a contiguous gap. Gap
/// detection happens upstream in HistoricalBarsProvider.
/// </summary>
public interface IPolygonBarFetcher
{
    /// <summary>
    /// Fetch bars for the requested range from Polygon and return the
    /// mapped Bar list. Returns an empty list when Polygon has no data
    /// (caller writes a miss marker). Throws on 5xx / network / timeout
    /// failures so the caller fails loud rather than silently
    /// mis-modeling.
    /// </summary>
    Task<IReadOnlyList<Bar>> FetchBarsAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt);
}

/// <summary>
/// Coalescer key for <see cref="PolygonBarFetcher"/>. Two concurrent
/// requests with the same (symbol, fromUtc, toUtc, timeframe) are folded
/// into a single upstream Polygon call.
/// </summary>
internal readonly record struct BarFetchKey(
    string Symbol,
    DateTime FromUtc,
    DateTime ToUtc,
    BarTimeframe Timeframe);

public sealed class PolygonBarFetcher : IPolygonBarFetcher
{
    private readonly IStocksService m_Stocks;
    private readonly ILogger<PolygonBarFetcher> m_Logger;
    private readonly MetricsCollector? m_Metrics;
    private readonly SingleFlight<BarFetchKey, IReadOnlyList<Bar>> m_Coalescer = new();

    /// <summary>
    /// Default per-call ceiling. Now enforced by <c>PerCallTimeoutHandler</c>
    /// in the SDK pipeline; kept as a const for backwards-compat readers.
    /// </summary>
    public const int DefaultPerCallTimeoutMs = 3000;

    /// <summary>
    /// Default concurrency cap. Now enforced by
    /// <c>ConcurrencyLimitingHandler</c> in the SDK pipeline.
    /// </summary>
    public const int DefaultMaxConcurrentFetches = 8;

    public PolygonBarFetcher(
        IStocksService inStocks,
        ILogger<PolygonBarFetcher> inLogger,
        MetricsCollector? inMetrics = null)
    {
        m_Stocks = inStocks;
        m_Logger = inLogger;
        m_Metrics = inMetrics;
        // Self-register the coalescer in-flight count with the collector
        // so GetCacheStats can report live concurrency without an extra
        // backchannel.
        m_Metrics?.RegisterInFlightProbe(MetricKind.Bars, () => m_Coalescer.InFlightCount);
    }

    /// <summary>
    /// Coalescer-wrapped public entry: 100 concurrent callers asking for
    /// the same (symbol, range, timeframe) collapse to a single
    /// <see cref="FetchBarsAsync_Inner"/> upstream call. Late joiners
    /// share the originator's <see cref="CancellationToken"/>.
    /// </summary>
    public Task<IReadOnlyList<Bar>> FetchBarsAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt)
    {
        var tmpKey = new BarFetchKey(inSymbol, inFromUtc, inToUtc, inTimeframe);
        return m_Coalescer.ExecuteAsync(tmpKey,
            () => FetchBarsAsync_Inner(inSymbol, inFromUtc, inToUtc, inTimeframe, inCt));
    }

    /// <summary>
    /// Diagnostic: in-flight coalescer entries. Surfaced via micro-PR #8
    /// stats endpoint; tests also assert this drops to zero post-completion.
    /// </summary>
    internal int InFlightCount => m_Coalescer.InFlightCount;

    private async Task<IReadOnlyList<Bar>> FetchBarsAsync_Inner(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt)
    {
        if (inFromUtc > inToUtc) return Array.Empty<Bar>();

        var (tmpMultiplier, tmpTimespan) = MapTimeframe(inTimeframe);

        // Polygon's /v2/aggs accepts YYYY-MM-DD or unix-ms. Use ISO date
        // for readability; query the inclusive range and post-filter the
        // returned bars to [from, to] in UTC. Polygon interprets from/to
        // in the asset's primary exchange timezone at request time, but
        // the per-bar `t` (unix ms) is what we use for the actual bar
        // boundary so off-by-one bars don't creep in.
        var tmpFromStr = inFromUtc.ToString("yyyy-MM-dd");
        var tmpToStr = inToUtc.ToString("yyyy-MM-dd");

        var tmpRequest = new GetBarsRequest
        {
            Ticker = inSymbol,
            Multiplier = tmpMultiplier,
            Timespan = tmpTimespan,
            From = tmpFromStr,
            To = tmpToStr,
            Adjusted = true,
            Sort = SortOrder.Ascending,
            Limit = 50000,
        };

        ApiResponse<PolygonResponse<List<StockBar>>> tmpResp;
        var tmpStopwatch = Stopwatch.StartNew();
        try
        {
            // Raw variant exposes the underlying HttpResponseMessage so we
            // can fail-quiet on 4xx without the SDK throwing
            // PolygonApiException. Concurrency + per-call timeout are
            // applied by the pipeline handlers.
            tmpResp = await m_Stocks.GetBarsRawAsync(tmpRequest, inCt).ConfigureAwait(false);
            // Record fetch + latency at the wire boundary, regardless of
            // 200/4xx. SingleFlight coalesces above us so this fires
            // exactly once per actual upstream call.
            m_Metrics?.RecordUpstreamFetch(MetricKind.Bars, tmpStopwatch.Elapsed.TotalMilliseconds);
        }
        catch (TimeoutException ex)
        {
            // PerCallTimeoutHandler throws this when the per-call ceiling
            // fires. Fail-loud so the engine surfaces it.
            m_Logger.LogError(ex,
                "Polygon /v2/aggs timed out for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd}",
                inSymbol, inTimeframe, inFromUtc, inToUtc);
            throw;
        }

        // Auth / entitlement / not-found / rate-limit — treat as empty
        // result (caller writes a miss marker).
        if (tmpResp.StatusCode == HttpStatusCode.Unauthorized
            || tmpResp.StatusCode == HttpStatusCode.Forbidden)
        {
            m_Logger.LogInformation(
                "Bars NOT_AUTHORIZED ({Status}) for {Symbol} {Timeframe} {From}..{To} — outside plan history depth",
                tmpResp.StatusCode, inSymbol, inTimeframe, tmpFromStr, tmpToStr);
            return Array.Empty<Bar>();
        }
        if (tmpResp.StatusCode == HttpStatusCode.NotFound)
        {
            m_Logger.LogInformation(
                "Bars 404 for {Symbol} {Timeframe} {From}..{To}",
                inSymbol, inTimeframe, tmpFromStr, tmpToStr);
            return Array.Empty<Bar>();
        }
        if (tmpResp.StatusCode == HttpStatusCode.TooManyRequests)
        {
            // 2026-05-11 fix: throw on persistent 429 instead of returning
            // empty. Pre-fix the empty return triggered the caller's miss-
            // marker write, poisoning the cache: subsequent runs hit the
            // marker and never re-fetch — losing the rate-limited range
            // permanently until a manual backfill. The PolygonRetryHandler
            // already retries 429 with Retry-After respect (3 attempts);
            // if we still see 429 here, it's a sustained throttle that
            // shouldn't be conflated with an authoritative "no data"
            // response. Throwing surfaces it as a transient failure and
            // leaves the gap open for the next request to re-attempt.
            m_Logger.LogWarning(
                "Bars 429 rate-limited for {Symbol} {Timeframe} {From}..{To} — propagating as transient (not poisoning cache)",
                inSymbol, inTimeframe, tmpFromStr, tmpToStr);
            if (tmpResp.Error is not null) throw tmpResp.Error;
            throw new HttpRequestException(
                $"Polygon /v2/aggs rate-limited (429) for {inSymbol} {inTimeframe} {tmpFromStr}..{tmpToStr} after retry-handler exhausted attempts");
        }

        if (!tmpResp.IsSuccessStatusCode)
        {
            // 5xx + everything else — fail loud. ApiResponse exposes the
            // underlying ApiException via Error; throwing it preserves
            // the SDK's typed exception surface for the caller.
            if (tmpResp.Error is not null) throw tmpResp.Error;
            throw new HttpRequestException(
                $"Polygon /v2/aggs returned {(int)tmpResp.StatusCode} {tmpResp.StatusCode}");
        }

        var tmpBody = tmpResp.Content;

        // Polygon "NOT_AUTHORIZED" sometimes comes through as 200 +
        // body.status="NOT_AUTHORIZED" / "ERROR" — handle that.
        if (tmpBody is not null
            && (string.Equals(tmpBody.Status, "NOT_AUTHORIZED", StringComparison.OrdinalIgnoreCase)
                || string.Equals(tmpBody.Status, "ERROR", StringComparison.OrdinalIgnoreCase)))
        {
            m_Logger.LogInformation(
                "Bars 200/body status={Status} for {Symbol} {Timeframe} {From}..{To}",
                tmpBody.Status, inSymbol, inTimeframe, tmpFromStr, tmpToStr);
            return Array.Empty<Bar>();
        }

        var tmpResults = tmpBody?.Results;
        if (tmpResults is null || tmpResults.Count == 0)
        {
            m_Logger.LogInformation(
                "Polygon /v2/aggs returned 0 bars for {Symbol} {Timeframe} {From}..{To}",
                inSymbol, inTimeframe, tmpFromStr, tmpToStr);
            return Array.Empty<Bar>();
        }

        var tmpBars = new List<Bar>(tmpResults.Count);
        foreach (var tmpAgg in tmpResults)
        {
            if (tmpAgg.Timestamp is null
                || tmpAgg.Open is null || tmpAgg.High is null
                || tmpAgg.Low is null || tmpAgg.Close is null
                || tmpAgg.Volume is null)
            {
                continue;
            }

            var tmpTsUtc = DateTimeOffset
                .FromUnixTimeMilliseconds((long)tmpAgg.Timestamp.Value)
                .UtcDateTime;

            // Post-filter to the requested UTC range. Polygon's
            // date-string request includes the full asset-tz day, so an
            // inclusive UTC [from, to] needs explicit clipping.
            if (tmpTsUtc < inFromUtc || tmpTsUtc > inToUtc) continue;

            tmpBars.Add(new Bar(
                Symbol: inSymbol,
                Timestamp: tmpTsUtc,
                Open: tmpAgg.Open.Value,
                High: tmpAgg.High.Value,
                Low: tmpAgg.Low.Value,
                Close: tmpAgg.Close.Value,
                Volume: (decimal)tmpAgg.Volume.Value,
                VWAP: tmpAgg.VolumeWeightedAveragePrice ?? 0m));
        }

        m_Logger.LogInformation(
            "Polygon bars fetch: {Symbol} {From:yyyy-MM-ddTHH:mm} → {To:yyyy-MM-ddTHH:mm} {Timeframe} → {Rows} rows in {LatencyMs}ms",
            inSymbol, inFromUtc, inToUtc, inTimeframe, tmpBars.Count, tmpStopwatch.Elapsed.TotalMilliseconds);
        return tmpBars;
    }

    /// <summary>Map BarTimeframe to Polygon (multiplier, AggregateInterval).</summary>
    private static (int Multiplier, AggregateInterval Timespan) MapTimeframe(BarTimeframe inTimeframe)
        => inTimeframe switch
        {
            BarTimeframe.OneMinute => (1, AggregateInterval.Minute),
            BarTimeframe.FiveMinutes => (5, AggregateInterval.Minute),
            BarTimeframe.FifteenMinutes => (15, AggregateInterval.Minute),
            BarTimeframe.OneHour => (1, AggregateInterval.Hour),
            BarTimeframe.OneDay => (1, AggregateInterval.Day),
            _ => (1, AggregateInterval.Minute)
        };
}
