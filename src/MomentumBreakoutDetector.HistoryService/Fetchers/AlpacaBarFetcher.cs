using System.Diagnostics;
using System.Net;
using Alpaca.Markets;
using Microsoft.Extensions.Logging;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Observability;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// Stock-bars fetcher backed by Alpaca's REST data API. Replaces
/// <see cref="PolygonBarFetcher"/> for the bars surface (Phase 2c, May 2026).
///
/// Why Alpaca for stock bars while options stay on Polygon:
///   - Polygon's plan returns <c>NOT_AUTHORIZED</c> for 1-min stock bars
///     older than ~2 years, which left the original TSLA backfill with
///     800 missed trading days (2022-08-25 onward) and 39 misses on the
///     forward seed. Alpaca's paid SIP feed has uncapped 1-min history.
///   - Alpaca's options surface is incomplete vs Polygon (no chains
///     snapshots, no full NBBO depth) — those endpoints stay on Polygon.
///
/// Production semantics mirror <see cref="PolygonBarFetcher"/>:
///   - SingleFlight coalescer: 100 concurrent callers asking for the same
///     (symbol, range, timeframe) collapse to a single upstream call.
///   - Fail-quiet on auth/rate-limit: 401/403/404/429 → empty list (caller
///     writes a miss marker). Subsequent runs short-circuit on the marker.
///   - Fail-loud on 5xx / network / timeout: propagate so the caller
///     surfaces the error instead of silently mis-modeling.
///
/// Concurrency cap: Alpaca's REST data limits are higher than the basic
/// Polygon tier (200/min vs 5/min historically). This fetcher relies on
/// the Alpaca SDK's built-in rate limit handling — we do NOT layer the
/// SDK behind <see cref="ConcurrencyLimitingHandler"/> because the SDK
/// does not expose its <c>HttpClient</c> for handler injection. The
/// SingleFlight coalescer is the dominant deduplication; HistoricalBars
/// Provider chunks 30-day windows so a single seed pass hits Alpaca with
/// at most a few hundred sequential calls per symbol.
///
/// Range-fetch shape: a single Alpaca request returns up to
/// <see cref="MaxPageSize"/> bars; the fetcher pages via NextPageToken
/// until exhausted, mirroring MBD's <c>AlpacaMarketDataProvider</c>.
/// </summary>
public sealed class AlpacaBarFetcher : IPolygonBarFetcher
{
    private readonly IHistoricalBarsClient<HistoricalBarsRequest> m_DataClient;
    private readonly MarketDataFeed m_Feed;
    private readonly ILogger<AlpacaBarFetcher> m_Logger;
    private readonly MetricsCollector? m_Metrics;
    private readonly SingleFlight<BarFetchKey, IReadOnlyList<Bar>> m_Coalescer = new();

    /// <summary>
    /// Alpaca's max page size on the historical bars endpoint. Two pages
    /// of 10k = 20k bars covers ~26 RTH days of 1-min, comfortably above
    /// the provider's 30-day chunk ceiling on most days.
    /// </summary>
    public const int MaxPageSize = 10000;

    /// <summary>
    /// Inject the narrowest interface that exposes
    /// <c>ListHistoricalBarsAsync</c> — <see cref="IHistoricalBarsClient{TRequest}"/>.
    /// <see cref="IAlpacaDataClient"/> implements this interface, so DI
    /// just upcasts. Narrowing keeps the production surface minimal and
    /// makes tests substitutable cleanly (NSubstitute on the wide
    /// <c>IAlpacaDataClient</c> trips on inherited-method tracking).
    /// </summary>
    public AlpacaBarFetcher(
        IHistoricalBarsClient<HistoricalBarsRequest> inDataClient,
        MarketDataFeed inFeed,
        ILogger<AlpacaBarFetcher> inLogger,
        MetricsCollector? inMetrics = null)
    {
        m_DataClient = inDataClient;
        m_Feed = inFeed;
        m_Logger = inLogger;
        m_Metrics = inMetrics;
        // Self-register the coalescer in-flight count with the collector
        // so GetCacheStats can report live concurrency without an extra
        // backchannel.
        m_Metrics?.RegisterInFlightProbe(MetricKind.Bars, () => m_Coalescer.InFlightCount);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<Bar>> FetchBarsAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt)
    {
        var tmpKey = new BarFetchKey(inSymbol, inFromUtc, inToUtc, inTimeframe);
        return m_Coalescer.ExecuteAsync(tmpKey,
            () => FetchBarsAsync_Inner(inSymbol, inFromUtc, inToUtc, inTimeframe, inCt));
    }

    /// <summary>Diagnostic: in-flight coalescer entries.</summary>
    internal int InFlightCount => m_Coalescer.InFlightCount;

    private async Task<IReadOnlyList<Bar>> FetchBarsAsync_Inner(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt)
    {
        if (inFromUtc > inToUtc) return Array.Empty<Bar>();

        var tmpAlpacaTimeframe = MapTimeframe(inTimeframe);
        var tmpStopwatch = Stopwatch.StartNew();
        var tmpAll = new List<Bar>();
        string? tmpPageToken = null;
        int tmpPageCount = 0;

        try
        {
            do
            {
                var tmpRequest = new HistoricalBarsRequest(inSymbol, inFromUtc, inToUtc, tmpAlpacaTimeframe)
                {
                    Feed = m_Feed,
                }.WithPageSize(MaxPageSize);
                if (tmpPageToken != null)
                {
                    tmpRequest = tmpRequest.WithPageToken(tmpPageToken);
                }

                var tmpPage = await m_DataClient
                    .ListHistoricalBarsAsync(tmpRequest, inCt)
                    .ConfigureAwait(false);

                tmpPageCount++;
                foreach (var tmpBar in tmpPage.Items)
                {
                    var tmpTsUtc = tmpBar.TimeUtc;
                    // Post-filter to the requested UTC range — Alpaca
                    // treats the [start, end] as inclusive but bar
                    // timestamps land on minute boundaries, so an
                    // end-of-day request can return the 16:00 bar.
                    if (tmpTsUtc < inFromUtc || tmpTsUtc > inToUtc) continue;

                    tmpAll.Add(new Bar(
                        Symbol: inSymbol,
                        Timestamp: tmpTsUtc,
                        Open: tmpBar.Open,
                        High: tmpBar.High,
                        Low: tmpBar.Low,
                        Close: tmpBar.Close,
                        Volume: tmpBar.Volume,
                        VWAP: tmpBar.Vwap));
                }

                tmpPageToken = tmpPage.NextPageToken;
            } while (!string.IsNullOrEmpty(tmpPageToken) && !inCt.IsCancellationRequested);

            // Record fetch + latency at the wire boundary. SingleFlight
            // coalesces above us so this fires exactly once per actual
            // upstream call (one logical "fetch" = potentially N pages).
            m_Metrics?.RecordUpstreamFetch(MetricKind.Bars, tmpStopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception tmpEx) when (TryExtractStatusCode(tmpEx, out var tmpStatus))
        {
            // Auth / entitlement / not-found / rate-limit — fail-quiet,
            // return empty, caller writes a miss marker. Mirrors
            // PolygonBarFetcher's contract so HistoricalBarsProvider's
            // upstream-call accounting is unchanged.
            //
            // We catch by status-code extraction rather than a hard
            // RestClientErrorException type-check because Alpaca's SDK
            // also surfaces HttpRequestException (with StatusCode set in
            // .NET 7+) for some transport-level failures.
            if (tmpStatus == HttpStatusCode.Unauthorized
                || tmpStatus == HttpStatusCode.Forbidden)
            {
                m_Logger.LogWarning(tmpEx,
                    "Alpaca bars NOT_AUTHORIZED ({Status}) for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd} — check ALPACA_API_KEY / data feed entitlement",
                    tmpStatus, inSymbol, inTimeframe, inFromUtc, inToUtc);
                return Array.Empty<Bar>();
            }
            if (tmpStatus == HttpStatusCode.NotFound)
            {
                m_Logger.LogInformation(
                    "Alpaca bars 404 for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd}",
                    inSymbol, inTimeframe, inFromUtc, inToUtc);
                return Array.Empty<Bar>();
            }
            if (tmpStatus == HttpStatusCode.TooManyRequests)
            {
                m_Logger.LogWarning(
                    "Alpaca bars 429 rate-limited for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd} — treating as miss for this run",
                    inSymbol, inTimeframe, inFromUtc, inToUtc);
                return Array.Empty<Bar>();
            }
            // 5xx / unknown — fail loud.
            m_Logger.LogError(tmpEx,
                "Alpaca bars failed ({Status}) for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd}",
                tmpStatus, inSymbol, inTimeframe, inFromUtc, inToUtc);
            throw;
        }

        if (tmpAll.Count == 0)
        {
            m_Logger.LogInformation(
                "Alpaca bars returned 0 rows for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd} ({Pages} page{S})",
                inSymbol, inTimeframe, inFromUtc, inToUtc, tmpPageCount, tmpPageCount == 1 ? "" : "s");
            return Array.Empty<Bar>();
        }

        m_Logger.LogInformation(
            "Alpaca bars fetch: {Symbol} {From:yyyy-MM-ddTHH:mm} → {To:yyyy-MM-ddTHH:mm} {Timeframe} → {Rows} rows in {LatencyMs}ms ({Pages} page{S})",
            inSymbol, inFromUtc, inToUtc, inTimeframe, tmpAll.Count,
            tmpStopwatch.Elapsed.TotalMilliseconds, tmpPageCount, tmpPageCount == 1 ? "" : "s");
        return tmpAll;
    }

    /// <summary>Map our <see cref="BarTimeframe"/> to Alpaca's <see cref="BarTimeFrame"/>.</summary>
    private static BarTimeFrame MapTimeframe(BarTimeframe inTimeframe) => inTimeframe switch
    {
        BarTimeframe.OneMinute => new BarTimeFrame(1, BarTimeFrameUnit.Minute),
        BarTimeframe.FiveMinutes => new BarTimeFrame(5, BarTimeFrameUnit.Minute),
        BarTimeframe.FifteenMinutes => new BarTimeFrame(15, BarTimeFrameUnit.Minute),
        BarTimeframe.OneHour => new BarTimeFrame(1, BarTimeFrameUnit.Hour),
        BarTimeframe.OneDay => new BarTimeFrame(1, BarTimeFrameUnit.Day),
        _ => new BarTimeFrame(1, BarTimeFrameUnit.Minute),
    };

    /// <summary>
    /// Best-effort status-code extraction. Alpaca's SDK throws
    /// <see cref="RestClientErrorException"/> (which exposes
    /// <c>HttpStatusCode</c> as a property) for upstream errors, but the
    /// transport layer can also surface <see cref="HttpRequestException"/>
    /// (.NET 7+ surfaces a typed <c>StatusCode</c> there). This helper
    /// covers both without forcing a hard SDK-type dependency on the
    /// catch site, which keeps the code testable with arbitrary
    /// exception subclasses.
    /// </summary>
    internal static bool TryExtractStatusCode(Exception inEx, out HttpStatusCode outStatus)
    {
        if (inEx is RestClientErrorException tmpRest && tmpRest.HttpStatusCode is { } tmpRestCode)
        {
            outStatus = tmpRestCode;
            return true;
        }
        if (inEx is HttpRequestException tmpHttp && tmpHttp.StatusCode is { } tmpCode)
        {
            outStatus = tmpCode;
            return true;
        }
        // Fallback: read a public/non-public HttpStatusCode-typed property
        // by reflection. This catches custom test subclasses and any
        // future SDK exception type that follows the same shape, without
        // making the production code throw on un-introspectable types.
        var tmpProp = inEx.GetType().GetProperty("HttpStatusCode")
                      ?? inEx.GetType().GetProperty("StatusCode");
        if (tmpProp is not null)
        {
            var tmpVal = tmpProp.GetValue(inEx);
            if (tmpVal is HttpStatusCode tmpHsc)
            {
                outStatus = tmpHsc;
                return true;
            }
        }
        outStatus = default;
        return false;
    }

    /// <summary>
    /// Parse a feed-name string ("sip", "iex", "otc"; case-insensitive)
    /// to <see cref="MarketDataFeed"/>. Defaults to <see cref="MarketDataFeed.Sip"/>
    /// on null/unknown — Lisus has the paid Alpaca subscription.
    /// </summary>
    public static MarketDataFeed ParseFeed(string? inFeed)
        => inFeed?.Trim().ToLowerInvariant() switch
        {
            "iex" => MarketDataFeed.Iex,
            "otc" => MarketDataFeed.Otc,
            _ => MarketDataFeed.Sip,
        };
}
