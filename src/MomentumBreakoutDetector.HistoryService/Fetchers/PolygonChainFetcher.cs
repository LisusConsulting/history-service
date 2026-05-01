using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TreyThomasCodes.Polygon.Models.Options;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// On-demand Polygon /v3/reference/options/contracts fetch with per-call
/// timeout, concurrency cap, and pagination. Lifted from the MBD repo
/// (PR #130) for the standalone history-service (Phase 1, micro-PR #4).
///
/// IMPORTANT — endpoint choice:
///   We use /v3/reference/options/contracts?as_of=YYYY-MM-DD which is
///   fully deterministic historical (Lisus-approved 2026-04-30). This
///   is DIFFERENT from /v3/snapshot/options/{TSLA} which PR #120 banned
///   for replay (snapshot always returned "now" regardless of date asked).
///
/// IMPORTANT — implementation note:
///   The published <c>TreyThomasCodes.Polygon.RestClient</c> NuGet (0.9.0)
///   does NOT expose the /v3/reference/options/contracts endpoint — only
///   ChainSnapshot (banned), ContractDetails (single ticker), and
///   per-contract bars / trades / quotes. The MBD in-tree fork added
///   <c>OptionsService.GetListContractsAsync</c> on top of the SDK; the
///   history-service can't take a project ref to that fork (it lives in
///   the MBD repo). So we hit the endpoint directly via HttpClient. The
///   resulting <c>OptionsContract</c> DTOs come from the NuGet'd Models
///   assembly (which DOES include the type) so the contract surface in
///   <c>OptionChainProvider</c> matches the original lift.
///
/// Pagination: Polygon returns ~200-500 contracts on TSLA at any as_of;
/// well under the 1000-row page limit so most calls complete in a single
/// round-trip. We still loop on next_url defensively (safety cap of 50
/// pages mirrors the original ContractsBackfillService) so a corner-case
/// wide-strike chain doesn't truncate silently.
///
/// Boundedness: the original fetch-budget abstraction was removed
/// 2026-05-01 (MBD repo) because legitimate long backtests trip arbitrary
/// caps. Determinism + idempotent cache writes + miss-marker tables bound
/// the total work to exactly the missing data for the window; the rate
/// limiter (timeout + semaphore) bounds concurrent dollar-cost.
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
  private readonly HttpClient m_Http;
  private readonly PolygonOptions m_PolygonOptions;
  private readonly ILogger<PolygonChainFetcher> m_Logger;
  private readonly SemaphoreSlim m_FetchConcurrencyGate;
  private readonly int m_PerCallTimeoutMs;

  /// <summary>
  /// Default per-call ceiling on a single Polygon /v3/reference/options/
  /// contracts page lookup. PolygonBarFetcher / PostgresOptionQuoteService
  /// use 3s but this endpoint is heavier per call (1000-row page vs one
  /// NBBO point). Empirically a cold-start cursor page takes ~250ms
  /// network + ~600ms deserialization; 3s held two pages reliably but
  /// raced page 3 of a TSLA chain (~6 pages) under the live capstone
  /// proof on 2026-04-30. 10s gives 3-4× headroom (PR #132).
  /// </summary>
  public const int DefaultPerCallTimeoutMs = 10000;

  /// <summary>
  /// Default concurrency cap on in-flight Polygon chain fetches. Same as
  /// the bar / quote gate (8). The chain endpoint is rate-limited under
  /// the same per-second pool as the others on Polygon's plan tier.
  /// </summary>
  public const int DefaultMaxConcurrentFetches = 8;

  /// <summary>
  /// Page size on the /v3/reference/options/contracts request. 1000 is
  /// Polygon's max.
  /// </summary>
  internal const int PageLimit = 1000;

  /// <summary>
  /// Defensive ceiling on pagination. 50 × 1000 contracts = 50K rows.
  /// </summary>
  internal const int MaxPagesPerCall = 50;

  /// <summary>
  /// JSON options shared across all paged responses. Polygon uses
  /// snake_case for the wire format (e.g. <c>strike_price</c>); the
  /// NuGet'd <c>OptionsContract</c> DTOs already carry
  /// <c>[JsonPropertyName]</c> attributes for those, so this just sets
  /// case-insensitivity as a defensive belt-and-braces.
  /// </summary>
  private static readonly JsonSerializerOptions s_JsonOptions = new()
  {
    PropertyNameCaseInsensitive = true,
    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
  };

  public PolygonChainFetcher(
    HttpClient inHttp,
    IOptions<PolygonOptions> inPolygonOptions,
    ILogger<PolygonChainFetcher> inLogger,
    int inPerCallTimeoutMs = DefaultPerCallTimeoutMs,
    int inMaxConcurrentFetches = DefaultMaxConcurrentFetches)
  {
    m_Http = inHttp;
    m_PolygonOptions = inPolygonOptions.Value;
    m_Logger = inLogger;
    m_PerCallTimeoutMs = inPerCallTimeoutMs > 0 ? inPerCallTimeoutMs : DefaultPerCallTimeoutMs;
    var tmpMaxCc = inMaxConcurrentFetches > 0 ? inMaxConcurrentFetches : DefaultMaxConcurrentFetches;
    m_FetchConcurrencyGate = new SemaphoreSlim(tmpMaxCc, tmpMaxCc);
  }

  public async Task<IReadOnlyList<OptionsContract>> FetchChainAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
  {
    if (string.IsNullOrEmpty(inSymbol)) return Array.Empty<OptionsContract>();

    var tmpAsOfStr = inAsOfDate.ToString("yyyy-MM-dd");
    var tmpBase = m_PolygonOptions.BaseUrl.TrimEnd('/');
    var tmpApiKey = m_PolygonOptions.ApiKey;

    // Concurrency cap. Per-page timeout CTS is created INSIDE the loop
    // below — m_PerCallTimeoutMs is a per-page budget, not a per-fetch
    // budget. With pagination, the first version of this code put the
    // CancelAfter outside the loop and a 3-page chain fetch would
    // routinely trip the deadline during page 3's deserializer. Reset
    // per page → each page gets the full budget independently.
    await m_FetchConcurrencyGate.WaitAsync(inCt);
    try
    {
      var tmpAll = new List<OptionsContract>(PageLimit);
      string? tmpCursor = null;
      var tmpPage = 0;

      do
      {
        tmpPage++;

        // Per-page timeout CTS — reset each page so a slow page N doesn't
        // inherit the depleted budget from page N-1.
        using var tmpPageCts = CancellationTokenSource.CreateLinkedTokenSource(inCt);
        tmpPageCts.CancelAfter(m_PerCallTimeoutMs);

        // Polygon's pagination model: page 1 builds a normal query;
        // subsequent pages use the next_url cursor. Both flavors take
        // apiKey via query string.
        Uri tmpUri;
        if (tmpCursor is null)
        {
          var tmpQs = $"underlying_ticker={Uri.EscapeDataString(inSymbol)}"
                    + $"&as_of={tmpAsOfStr}"
                    + $"&limit={PageLimit}"
                    + $"&apiKey={Uri.EscapeDataString(tmpApiKey)}";
          tmpUri = new Uri($"{tmpBase}/v3/reference/options/contracts?{tmpQs}");
        }
        else
        {
          var tmpQs = $"underlying_ticker={Uri.EscapeDataString(inSymbol)}"
                    + $"&as_of={tmpAsOfStr}"
                    + $"&limit={PageLimit}"
                    + $"&cursor={Uri.EscapeDataString(tmpCursor)}"
                    + $"&apiKey={Uri.EscapeDataString(tmpApiKey)}";
          tmpUri = new Uri($"{tmpBase}/v3/reference/options/contracts?{tmpQs}");
        }

        using var tmpResp = await m_Http.GetAsync(tmpUri, HttpCompletionOption.ResponseHeadersRead, tmpPageCts.Token);

        if (!tmpResp.IsSuccessStatusCode)
        {
          var tmpBody = await SafeReadAsStringAsync(tmpResp, tmpPageCts.Token);
          var tmpHandled = TryHandleNonSuccess(tmpResp.StatusCode, tmpBody, inSymbol, tmpAsOfStr);
          if (tmpHandled is not null) return tmpHandled;
          tmpResp.EnsureSuccessStatusCode(); // 5xx → throw, abort the run
        }

        var tmpPayload = await tmpResp.Content.ReadFromJsonAsync<PolygonContractsListResponse>(
          s_JsonOptions, tmpPageCts.Token);

        var tmpResults = tmpPayload?.Results;
        if (tmpResults is not null && tmpResults.Count > 0)
        {
          tmpAll.AddRange(tmpResults);
        }

        tmpCursor = ContractsBackfillCursorHelper.ExtractCursor(tmpPayload?.NextUrl);
      } while (!string.IsNullOrEmpty(tmpCursor)
               && tmpPage < MaxPagesPerCall
               && !inCt.IsCancellationRequested);

      m_Logger.LogInformation(
        "Polygon on-demand chain fetch: {Count} contracts for {Symbol} as_of {AsOf} ({Pages} pages)",
        tmpAll.Count, inSymbol, tmpAsOfStr, tmpPage);
      return tmpAll;
    }
    catch (OperationCanceledException) when (!inCt.IsCancellationRequested)
    {
      // Per-page timeout fired — treat as miss-for-this-run rather than
      // fail-loud: a hung pagination page on a cold-start backtest
      // shouldn't abort the entire run.
      m_Logger.LogWarning(
        "Polygon /v3/reference/options/contracts timed out for {Symbol} as_of {AsOf} — treating as miss for this run",
        inSymbol, tmpAsOfStr);
      return Array.Empty<OptionsContract>();
    }
    catch (HttpRequestException ex)
    {
      // Network-level fault on the page request. Same intent as 5xx —
      // bubble the error so the caller fails loud rather than silently
      // running with a stale chain.
      m_Logger.LogError(ex, "Polygon chain fetch network error for {Symbol} as_of {AsOf}", inSymbol, tmpAsOfStr);
      throw;
    }
    finally
    {
      m_FetchConcurrencyGate.Release();
    }
  }

  /// <summary>
  /// Map the original-lift's 4xx ladder onto raw HTTP status codes.
  /// Returns an empty list (treat-as-miss for this run) for the cases
  /// that the SDK-flavored fetcher caught with named exceptions; returns
  /// null for "not handled — caller should EnsureSuccessStatusCode".
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
      // 429 — treat as miss for this run, avoid aborting a 30-day cold-
      // start backtest on a transient rate-limit.
      m_Logger.LogWarning(
        "Chains 429 rate-limited for {Symbol} as_of {AsOf} — treating as miss for this run",
        inSymbol, inAsOfStr);
      return Array.Empty<OptionsContract>();
    }

    return null;
  }

  private static async Task<string?> SafeReadAsStringAsync(HttpResponseMessage inResp, CancellationToken inCt)
  {
    try
    {
      return await inResp.Content.ReadAsStringAsync(inCt);
    }
    catch
    {
      return null;
    }
  }

  /// <summary>
  /// JSON shape of the /v3/reference/options/contracts response. Polygon
  /// returns <c>{ results: [...], next_url, status, request_id, count }</c>.
  /// We only consume <c>results</c> + <c>next_url</c>.
  /// </summary>
  private sealed class PolygonContractsListResponse
  {
    [JsonPropertyName("results")]
    public List<OptionsContract>? Results { get; set; }

    [JsonPropertyName("next_url")]
    public string? NextUrl { get; set; }

    [JsonPropertyName("status")]
    public string? Status { get; set; }
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
