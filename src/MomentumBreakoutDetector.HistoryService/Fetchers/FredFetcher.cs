using System.Globalization;
using System.Net;
using System.Text.Json;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// Single FRED observation row mapped from the
/// <c>/fred/series/observations</c> endpoint. Decimal value is null when
/// FRED reports the canonical "." sentinel (genuinely missing observation
/// for that date — e.g. T10Y2Y on a market holiday).
/// </summary>
public sealed record FredObservationRow(
    string SeriesId,
    DateOnly ObservationDate,
    decimal? Value);

/// <summary>
/// On-demand FRED <c>/fred/series/observations</c> fetch with per-call
/// timeout and concurrency cap. Lifted from MBD PR #138 — same shape as
/// the bar / chain fetchers: linked-CTS timeout, SemaphoreSlim(8) gate,
/// fail-loud on transient errors so the consumer surfaces them rather
/// than silently mis-modeling.
///
/// FRED's published rate limit on the standard plan is 120 req/min,
/// well above what a backtest cold-start hits (3 series × N range
/// chunks). The semaphore + per-call timeout bound concurrent
/// dollar-cost; idempotent cache writes + miss-markers bound the total
/// work to exactly the missing data for the window.
/// </summary>
public interface IFredFetcher
{
    /// <summary>
    /// Fetch all observations for <paramref name="seriesId"/> in the
    /// inclusive date range [<paramref name="fromDate"/>, <paramref name="toDate"/>].
    /// Returns the full observation list (typically 1-90 rows depending
    /// on series cadence + window). FRED's "." sentinel surfaces as
    /// <see cref="FredObservationRow.Value"/> = null so the caller can
    /// distinguish "FRED reports no data on this date" from "FRED has not
    /// returned this date at all".
    ///
    /// Returns an empty list when FRED has no data (caller writes
    /// miss-markers per requested observation date). Throws on 5xx /
    /// network / timeout failures so the caller fails loud.
    /// </summary>
    Task<IReadOnlyList<FredObservationRow>> FetchSeriesAsync(
        string seriesId, DateOnly fromDate, DateOnly toDate,
        CancellationToken ct);
}

public sealed class FredFetcher : IFredFetcher
{
    private readonly IHttpClientFactory? _httpClientFactory;
    private readonly ILogger<FredFetcher> _logger;
    private readonly SemaphoreSlim _fetchConcurrencyGate;
    private readonly int _perCallTimeoutMs;
    private readonly string? _apiKey;

    /// <summary>
    /// Default per-call ceiling on a single FRED <c>/fred/series/observations</c>
    /// lookup. FRED responses are small (up to ~90 rows × ~120 bytes JSON
    /// ≈ 10KB) so 3s is plenty under normal latency.
    /// </summary>
    public const int DefaultPerCallTimeoutMs = 3000;

    /// <summary>
    /// Default concurrency cap on in-flight FRED fetches. FRED's 120/min
    /// budget vastly outpaces 8 concurrent × 3s timeout = ~2.7 fetches/sec
    /// pessimistic ceiling.
    /// </summary>
    public const int DefaultMaxConcurrentFetches = 8;

    public const string HttpClientName = "fred";

    private const string FredBase = "https://api.stlouisfed.org";

    public FredFetcher(
        ILogger<FredFetcher> logger,
        IHttpClientFactory? httpClientFactory = null,
        string? apiKey = null,
        int perCallTimeoutMs = DefaultPerCallTimeoutMs,
        int maxConcurrentFetches = DefaultMaxConcurrentFetches)
    {
        _logger = logger;
        _httpClientFactory = httpClientFactory;
        _apiKey = apiKey ?? Environment.GetEnvironmentVariable("FRED_API_KEY");
        _perCallTimeoutMs = perCallTimeoutMs > 0 ? perCallTimeoutMs : DefaultPerCallTimeoutMs;
        var maxCc = maxConcurrentFetches > 0 ? maxConcurrentFetches : DefaultMaxConcurrentFetches;
        _fetchConcurrencyGate = new SemaphoreSlim(maxCc, maxCc);
    }

    public async Task<IReadOnlyList<FredObservationRow>> FetchSeriesAsync(
        string seriesId, DateOnly fromDate, DateOnly toDate,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(seriesId)) return Array.Empty<FredObservationRow>();
        if (fromDate > toDate) return Array.Empty<FredObservationRow>();
        if (string.IsNullOrEmpty(_apiKey))
        {
            // Without a key FRED returns 400 "api_key is required". Fail-quiet
            // (empty list) rather than crash — the caller writes miss-markers
            // if it chooses, and ops surfaces the missing key elsewhere
            // (startup log line / health probe).
            _logger.LogWarning(
                "FRED_API_KEY not set — skipping FRED fetch for {Series} {From}..{To}",
                seriesId, fromDate, toDate);
            return Array.Empty<FredObservationRow>();
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(_perCallTimeoutMs);

        await _fetchConcurrencyGate.WaitAsync(ct);
        try
        {
            var client = _httpClientFactory?.CreateClient(HttpClientName)
                ?? new HttpClient { Timeout = TimeSpan.FromMilliseconds(_perCallTimeoutMs * 2) };

            var url = $"{FredBase}/fred/series/observations"
                    + $"?series_id={Uri.EscapeDataString(seriesId)}"
                    + $"&api_key={Uri.EscapeDataString(_apiKey)}"
                    + $"&file_type=json"
                    + $"&sort_order=asc"
                    + $"&observation_start={fromDate:yyyy-MM-dd}"
                    + $"&observation_end={toDate:yyyy-MM-dd}";

            using var resp = await client.GetAsync(url, timeoutCts.Token);

            // 4xx → treat as miss for this run. FRED's 400 typically means a
            // malformed series_id; 404 means the series doesn't exist. Either
            // way, the answer for this run is "no data" — failing loud would
            // abort an entire backtest on a single typo'd series.
            if (resp.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.BadRequest)
            {
                _logger.LogInformation(
                    "FRED {Status} for {Series} {From}..{To} — treating as miss for this run",
                    (int)resp.StatusCode, seriesId, fromDate, toDate);
                return Array.Empty<FredObservationRow>();
            }

            // 429 — respect Retry-After once, then surface as failure if still
            // rate-limited. A single retry is cheap; chronic rate-limit means
            // the whole run is busted, so failing loud is correct.
            if (resp.StatusCode == (HttpStatusCode)429)
            {
                var retryAfter = resp.Headers.RetryAfter?.Delta
                    ?? TimeSpan.FromSeconds(2);
                if (retryAfter > TimeSpan.FromSeconds(30))
                    retryAfter = TimeSpan.FromSeconds(30);
                _logger.LogWarning(
                    "FRED 429 for {Series} {From}..{To} — retrying once after {Delay}",
                    seriesId, fromDate, toDate, retryAfter);
                await Task.Delay(retryAfter, timeoutCts.Token);

                using var retry = await client.GetAsync(url, timeoutCts.Token);
                if (!retry.IsSuccessStatusCode)
                {
                    throw new HttpRequestException(
                        $"FRED rate-limited (429) after retry for {seriesId} {fromDate}..{toDate}");
                }
                var retryJson = await retry.Content.ReadAsStringAsync(timeoutCts.Token);
                return ParseObservations(seriesId, retryJson);
            }

            // 5xx / network → fail loud. Better to surface failure than
            // silently mis-model.
            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(timeoutCts.Token);
                _logger.LogError(
                    "FRED {Status} for {Series} {From}..{To}: {Body}",
                    (int)resp.StatusCode, seriesId, fromDate, toDate, body);
                throw new HttpRequestException(
                    $"FRED returned {(int)resp.StatusCode} for {seriesId} {fromDate}..{toDate}");
            }

            var json = await resp.Content.ReadAsStringAsync(timeoutCts.Token);
            return ParseObservations(seriesId, json);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested
                                                 && !ct.IsCancellationRequested)
        {
            _logger.LogError(
                "FRED /fred/series/observations timed out after {TimeoutMs}ms for {Series} {From}..{To}",
                _perCallTimeoutMs, seriesId, fromDate, toDate);
            throw new TimeoutException(
                $"FRED /fred/series/observations timed out after {_perCallTimeoutMs}ms for "
                + $"{seriesId} {fromDate}..{toDate}");
        }
        finally
        {
            _fetchConcurrencyGate.Release();
        }
    }

    /// <summary>
    /// Parse FRED's <c>{ "observations": [ { "date": ..., "value": ... } ] }</c>
    /// shape into <see cref="FredObservationRow"/>. The "." sentinel is
    /// preserved as <see cref="FredObservationRow.Value"/> = null so the
    /// caller can write a miss-marker for that observation date.
    /// </summary>
    internal static List<FredObservationRow> ParseObservations(string seriesId, string json)
    {
        using var doc = JsonDocument.Parse(json);
        if (!doc.RootElement.TryGetProperty("observations", out var obs))
            return new List<FredObservationRow>();

        var result = new List<FredObservationRow>(obs.GetArrayLength());
        foreach (var ob in obs.EnumerateArray())
        {
            if (!ob.TryGetProperty("date", out var dateProp)) continue;
            var dateStr = dateProp.GetString();
            if (string.IsNullOrEmpty(dateStr)) continue;
            if (!DateOnly.TryParseExact(dateStr, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out var date))
            {
                continue;
            }

            decimal? value = null;
            if (ob.TryGetProperty("value", out var valueProp))
            {
                var valueStr = valueProp.GetString();
                // FRED uses "." for missing observations on dates the series
                // doesn't publish (e.g. T10Y2Y on a market holiday).
                if (!string.IsNullOrEmpty(valueStr)
                    && valueStr != "."
                    && decimal.TryParse(valueStr, NumberStyles.Number, CultureInfo.InvariantCulture, out var dec))
                {
                    value = dec;
                }
            }

            result.Add(new FredObservationRow(seriesId, date, value));
        }
        return result;
    }
}
