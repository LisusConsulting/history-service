using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Domain;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// On-demand Polygon /v2/aggs bar fetch with per-call timeout and
/// concurrency cap. Lifted from MBD's
/// `MomentumBreakoutDetector.Infrastructure.Data.PolygonBarFetcher`
/// (PR #129) and refactored to use a plain named HttpClient ("polygon")
/// instead of the vendored TreyThomasCodes.Polygon SDK so the new
/// service has no compile-time dep on MBD code.
///
/// Range-fetch shape: a single Polygon /v2/aggs call returns bars for a
/// [from, to] window, so a single fetch closes a contiguous gap. Gap
/// detection happens upstream in HistoricalBarsProvider — by the time we
/// get here the caller has already determined "this range is missing".
///
/// Boundedness: the fetch budget abstraction was removed 2026-05-01
/// (MBD PR #133). Determinism + idempotent cache writes + miss-marker
/// tables bound the total work to exactly the missing data for the
/// window; the rate limiter (3s timeout + SemaphoreSlim(8)) bounds
/// concurrent dollar-cost.
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

public sealed class PolygonBarFetcher : IPolygonBarFetcher
{
    private readonly IHttpClientFactory m_HttpClientFactory;
    private readonly ILogger<PolygonBarFetcher> m_Logger;
    private readonly SemaphoreSlim m_FetchConcurrencyGate;
    private readonly int m_PerCallTimeoutMs;
    private readonly string m_ApiKey;

    /// <summary>HTTP client name registered in DI for Polygon /v2/aggs calls.</summary>
    public const string HttpClientName = "polygon";

    /// <summary>
    /// Default per-call ceiling on a single Polygon /v2/aggs lookup
    /// (mirror of MBD PolygonBarFetcher.DefaultPerCallTimeoutMs = 3s).
    /// 5d of 1-min TSLA bars is ~1900 rows × ~80B = ~150KB so 3s is
    /// plenty under normal latency.
    /// </summary>
    public const int DefaultPerCallTimeoutMs = 3000;

    /// <summary>
    /// Default concurrency cap on in-flight Polygon bar fetches.
    /// Polygon's plan permits ~100 req/sec; with timeout=3s and
    /// MaxConcurrent=8 the pessimistic ceiling is ~2.7 fetches/sec.
    /// </summary>
    public const int DefaultMaxConcurrentFetches = 8;

    public PolygonBarFetcher(
        IHttpClientFactory inHttpClientFactory,
        IOptions<HistoryServiceOptions> inOptions,
        ILogger<PolygonBarFetcher> inLogger)
        : this(
            inHttpClientFactory,
            inLogger,
            // PolygonApiKey is shared with the NBBO fetcher (micro-PR #3).
            // Both speak to the same Polygon plan, same key.
            inOptions.Value.PolygonApiKey ?? string.Empty,
            inOptions.Value.PolygonPerCallTimeoutMs > 0 ? inOptions.Value.PolygonPerCallTimeoutMs : DefaultPerCallTimeoutMs,
            inOptions.Value.PolygonMaxConcurrentFetches > 0 ? inOptions.Value.PolygonMaxConcurrentFetches : DefaultMaxConcurrentFetches)
    {
    }

    /// <summary>
    /// Test-friendly ctor that accepts the API key + tuning directly so
    /// integration tests can wire up a stub without binding options.
    /// </summary>
    public PolygonBarFetcher(
        IHttpClientFactory inHttpClientFactory,
        ILogger<PolygonBarFetcher> inLogger,
        string inApiKey,
        int inPerCallTimeoutMs = DefaultPerCallTimeoutMs,
        int inMaxConcurrentFetches = DefaultMaxConcurrentFetches)
    {
        m_HttpClientFactory = inHttpClientFactory;
        m_Logger = inLogger;
        m_ApiKey = inApiKey ?? string.Empty;
        m_PerCallTimeoutMs = inPerCallTimeoutMs > 0 ? inPerCallTimeoutMs : DefaultPerCallTimeoutMs;
        var tmpMaxCc = inMaxConcurrentFetches > 0 ? inMaxConcurrentFetches : DefaultMaxConcurrentFetches;
        m_FetchConcurrencyGate = new SemaphoreSlim(tmpMaxCc, tmpMaxCc);
    }

    public async Task<IReadOnlyList<Bar>> FetchBarsAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt)
    {
        if (inFromUtc > inToUtc) return Array.Empty<Bar>();

        var (tmpMultiplier, tmpTimespan) = MapTimeframe(inTimeframe);

        // Per-call ceiling + concurrency cap. Linked CTS aborts a single
        // hung Polygon call without disturbing the caller's CT.
        using var tmpTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(inCt);
        tmpTimeoutCts.CancelAfter(m_PerCallTimeoutMs);

        await m_FetchConcurrencyGate.WaitAsync(inCt);
        try
        {
            // Polygon's /v2/aggs accepts YYYY-MM-DD or unix-ms. Use ISO
            // date for readability; we query the inclusive range and
            // post-filter the returned bars to [from, to] in UTC. Polygon
            // interprets from/to in the asset's primary exchange timezone
            // at request time, but the per-bar `t` (unix ms) is what we
            // use for the actual bar boundary so off-by-one bars don't
            // creep in.
            var tmpFromStr = inFromUtc.ToString("yyyy-MM-dd");
            var tmpToStr = inToUtc.ToString("yyyy-MM-dd");

            var tmpUrl =
                $"/v2/aggs/ticker/{Uri.EscapeDataString(inSymbol)}"
                + $"/range/{tmpMultiplier}/{tmpTimespan}"
                + $"/{tmpFromStr}/{tmpToStr}"
                + $"?adjusted=true&sort=asc&limit=50000"
                + $"&apiKey={Uri.EscapeDataString(m_ApiKey)}";

            var tmpClient = m_HttpClientFactory.CreateClient(HttpClientName);
            using var tmpResp = await tmpClient.GetAsync(tmpUrl, tmpTimeoutCts.Token);

            // Auth / entitlement / not-found — treat as empty result
            // (caller writes a miss marker). Same fail-quiet shape as the
            // MBD original.
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
                // 429 — back off rather than fail-loud. Caller writes a
                // miss-marker; subsequent runs hit the marker and skip
                // re-fetch. Without this, a single rate-limit blip aborts
                // a 30-day cold-start backtest.
                m_Logger.LogWarning(
                    "Bars 429 rate-limited for {Symbol} {Timeframe} {From}..{To} — treating as miss for this run",
                    inSymbol, inTimeframe, tmpFromStr, tmpToStr);
                return Array.Empty<Bar>();
            }

            tmpResp.EnsureSuccessStatusCode();

            await using var tmpStream = await tmpResp.Content.ReadAsStreamAsync(tmpTimeoutCts.Token);
            var tmpDoc = await JsonDocument.ParseAsync(tmpStream, cancellationToken: tmpTimeoutCts.Token);

            // Polygon "NOT_AUTHORIZED" sometimes comes through as 200 +
            // status="NOT_AUTHORIZED" in the body — handle that too.
            if (tmpDoc.RootElement.TryGetProperty("status", out var tmpStatusEl)
                && tmpStatusEl.ValueKind == JsonValueKind.String)
            {
                var tmpStatus = tmpStatusEl.GetString();
                if (string.Equals(tmpStatus, "NOT_AUTHORIZED", StringComparison.OrdinalIgnoreCase))
                {
                    m_Logger.LogInformation(
                        "Bars NOT_AUTHORIZED (200/body) for {Symbol} {Timeframe} {From}..{To}",
                        inSymbol, inTimeframe, tmpFromStr, tmpToStr);
                    return Array.Empty<Bar>();
                }
            }

            if (!tmpDoc.RootElement.TryGetProperty("results", out var tmpResults)
                || tmpResults.ValueKind != JsonValueKind.Array
                || tmpResults.GetArrayLength() == 0)
            {
                m_Logger.LogInformation(
                    "Polygon /v2/aggs returned 0 bars for {Symbol} {Timeframe} {From}..{To}",
                    inSymbol, inTimeframe, tmpFromStr, tmpToStr);
                return Array.Empty<Bar>();
            }

            var tmpBars = new List<Bar>(tmpResults.GetArrayLength());
            foreach (var tmpAgg in tmpResults.EnumerateArray())
            {
                if (!TryGetLong(tmpAgg, "t", out var tmpTs)) continue;
                if (!TryGetDecimal(tmpAgg, "o", out var tmpOpen)) continue;
                if (!TryGetDecimal(tmpAgg, "h", out var tmpHigh)) continue;
                if (!TryGetDecimal(tmpAgg, "l", out var tmpLow)) continue;
                if (!TryGetDecimal(tmpAgg, "c", out var tmpClose)) continue;
                if (!TryGetDecimal(tmpAgg, "v", out var tmpVolume)) continue;
                TryGetDecimal(tmpAgg, "vw", out var tmpVwap); // optional

                var tmpTsUtc = DateTimeOffset.FromUnixTimeMilliseconds(tmpTs).UtcDateTime;
                // Post-filter to the requested UTC range. Polygon's
                // date-string request includes the full asset-tz day, so
                // an inclusive UTC [from, to] needs explicit clipping.
                if (tmpTsUtc < inFromUtc || tmpTsUtc > inToUtc) continue;

                tmpBars.Add(new Bar(
                    Symbol: inSymbol,
                    Timestamp: tmpTsUtc,
                    Open: tmpOpen,
                    High: tmpHigh,
                    Low: tmpLow,
                    Close: tmpClose,
                    Volume: tmpVolume,
                    VWAP: tmpVwap));
            }

            m_Logger.LogInformation(
                "Polygon on-demand fetch: {Count} {Timeframe} bars for {Symbol} {From}..{To}",
                tmpBars.Count, inTimeframe, inSymbol, tmpFromStr, tmpToStr);
            return tmpBars;
        }
        catch (OperationCanceledException) when (tmpTimeoutCts.IsCancellationRequested
                                                 && !inCt.IsCancellationRequested)
        {
            // Per-call timeout — fail loud so the engine surfaces it.
            // Better to fail loud than silently mis-model.
            m_Logger.LogError(
                "Polygon /v2/aggs timed out after {TimeoutMs}ms for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd}",
                m_PerCallTimeoutMs, inSymbol, inTimeframe, inFromUtc, inToUtc);
            throw new TimeoutException(
                $"Polygon /v2/aggs timed out after {m_PerCallTimeoutMs}ms for "
                + $"{inSymbol} {inTimeframe} {inFromUtc:yyyy-MM-dd}..{inToUtc:yyyy-MM-dd}");
        }
        finally
        {
            m_FetchConcurrencyGate.Release();
        }
    }

    /// <summary>Map BarTimeframe to Polygon (multiplier, timespan).</summary>
    private static (int Multiplier, string Timespan) MapTimeframe(BarTimeframe inTimeframe)
        => inTimeframe switch
        {
            BarTimeframe.OneMinute => (1, "minute"),
            BarTimeframe.FiveMinutes => (5, "minute"),
            BarTimeframe.FifteenMinutes => (15, "minute"),
            BarTimeframe.OneHour => (1, "hour"),
            BarTimeframe.OneDay => (1, "day"),
            _ => (1, "minute")
        };

    private static bool TryGetLong(JsonElement inObj, string inProp, out long outValue)
    {
        outValue = 0;
        if (!inObj.TryGetProperty(inProp, out var tmpEl)) return false;
        if (tmpEl.ValueKind != JsonValueKind.Number) return false;
        return tmpEl.TryGetInt64(out outValue);
    }

    private static bool TryGetDecimal(JsonElement inObj, string inProp, out decimal outValue)
    {
        outValue = 0m;
        if (!inObj.TryGetProperty(inProp, out var tmpEl)) return false;
        if (tmpEl.ValueKind != JsonValueKind.Number) return false;
        return tmpEl.TryGetDecimal(out outValue);
    }
}
