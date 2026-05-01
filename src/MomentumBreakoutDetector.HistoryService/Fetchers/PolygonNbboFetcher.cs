using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MomentumBreakoutDetector.HistoryService.Fetchers;

/// <summary>
/// Lifted from MBD's <c>PostgresOptionQuoteService.FetchAndCacheAsync</c>
/// (PRs #98 + #121). Calls Polygon <c>/v3/quotes/{ticker}?timestamp.lte=…</c>
/// with limit=1 + order=desc to get the most recent NBBO at-or-before the
/// requested timestamp.
///
/// Differences vs. the MBD source:
///   - Plain HttpClient (the service is self-contained; no Refit / vendored
///     client wrapper). Same endpoint, same query shape.
///   - No <c>IBacktestFetchBudget</c> plumbing — that path was removed in
///     PR #133.
///   - 3 s per-call timeout via a linked CTS, mirroring
///     <c>DefaultPerCallTimeoutMs</c>.
///   - SemaphoreSlim concurrency cap (default 8) mirroring
///     <c>DefaultMaxConcurrentFetches</c>.
///
/// Registered as a singleton in Program.cs so the SemaphoreSlim is
/// process-wide and HttpClient is reused (handler pooling).
/// </summary>
public sealed class PolygonNbboFetcher : IPolygonNbboFetcher, IDisposable
{
    public const int DefaultPerCallTimeoutMs = 3000;
    public const int DefaultMaxConcurrentFetches = 8;

    private readonly HttpClient m_Http;
    private readonly ILogger<PolygonNbboFetcher> m_Logger;
    private readonly SemaphoreSlim m_Gate;
    private readonly int m_PerCallTimeoutMs;
    private readonly string m_ApiKey;

    private static readonly JsonSerializerOptions s_JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public PolygonNbboFetcher(
        HttpClient inHttp,
        IOptions<HistoryServiceOptions> inOpts,
        ILogger<PolygonNbboFetcher> inLogger)
    {
        m_Http = inHttp;
        m_Logger = inLogger;
        var tmpOpts = inOpts.Value;
        m_ApiKey = tmpOpts.PolygonApiKey ?? "";
        m_PerCallTimeoutMs = tmpOpts.PolygonPerCallTimeoutMs > 0
            ? tmpOpts.PolygonPerCallTimeoutMs
            : DefaultPerCallTimeoutMs;
        var tmpMaxCc = tmpOpts.PolygonMaxConcurrentFetches > 0
            ? tmpOpts.PolygonMaxConcurrentFetches
            : DefaultMaxConcurrentFetches;
        m_Gate = new SemaphoreSlim(tmpMaxCc, tmpMaxCc);

        if (m_Http.BaseAddress is null)
        {
            m_Http.BaseAddress = new Uri(
                string.IsNullOrWhiteSpace(tmpOpts.PolygonBaseUrl)
                    ? "https://api.polygon.io"
                    : tmpOpts.PolygonBaseUrl);
        }
    }

    public async Task<PolygonNbboFetch> FetchAsync(
        string inTicker,
        DateTime inTsUtc,
        CancellationToken inCt = default)
    {
        // Per-call 3 s ceiling. The linked CTS aborts a single hung call
        // without disturbing the caller's CT — same pattern as MBD's
        // TradingEngine.POLYGON_PER_TICK_TIMEOUT_MS (PR #110).
        using var tmpTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(inCt);
        tmpTimeoutCts.CancelAfter(m_PerCallTimeoutMs);

        await m_Gate.WaitAsync(inCt).ConfigureAwait(false);
        try
        {
            // Polygon accepts ISO-8601 with seconds, in UTC. e.g. "2025-12-15T15:30:00Z".
            var tmpTsParam = inTsUtc.ToString("yyyy-MM-ddTHH:mm:ssZ");
            var tmpUri = $"/v3/quotes/{Uri.EscapeDataString(inTicker)}"
                + $"?timestamp.lte={tmpTsParam}&order=desc&limit=1";
            if (!string.IsNullOrWhiteSpace(m_ApiKey))
            {
                tmpUri += $"&apiKey={Uri.EscapeDataString(m_ApiKey)}";
            }

            using var tmpReq = new HttpRequestMessage(HttpMethod.Get, tmpUri);
            using var tmpResp = await m_Http
                .SendAsync(tmpReq, HttpCompletionOption.ResponseHeadersRead, tmpTimeoutCts.Token)
                .ConfigureAwait(false);

            if (tmpResp.StatusCode == HttpStatusCode.Unauthorized
                || tmpResp.StatusCode == HttpStatusCode.Forbidden)
            {
                // Plan-tier limit. Polygon returns NOT_AUTHORIZED outside
                // the subscription's history depth (Options Advanced /v3
                // quotes start 2022-03-07). Same handling as MBD lift.
                m_Logger.LogInformation(
                    "Quote NOT_AUTHORIZED for {Ticker} @ {Ts:O} — outside plan history depth",
                    inTicker, inTsUtc);
                return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "plan-not-authorized");
            }

            tmpResp.EnsureSuccessStatusCode();

            var tmpPayload = await tmpResp.Content
                .ReadFromJsonAsync<PolygonQuotesEnvelope>(s_JsonOpts, tmpTimeoutCts.Token)
                .ConfigureAwait(false);

            var tmpQuote = tmpPayload?.Results?.FirstOrDefault();
            if (tmpQuote is null
                || tmpQuote.BidPrice is null || tmpQuote.AskPrice is null
                || tmpQuote.SipTimestamp is null)
            {
                return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "no quote in window");
            }

            // SIP timestamp is nanoseconds since epoch.
            var tmpAsOf = DateTimeOffset
                .FromUnixTimeMilliseconds(tmpQuote.SipTimestamp.Value / 1_000_000L)
                .UtcDateTime;

            return new PolygonNbboFetch(
                PolygonNbboOutcome.Hit,
                new PolygonNbboResult(
                    Ticker: inTicker,
                    RequestedTsUtc: inTsUtc,
                    AsOfTsUtc: tmpAsOf,
                    BidPrice: (decimal)tmpQuote.BidPrice.Value,
                    AskPrice: (decimal)tmpQuote.AskPrice.Value,
                    BidSize: tmpQuote.BidSize,
                    AskSize: tmpQuote.AskSize,
                    BidExchange: tmpQuote.BidExchange,
                    AskExchange: tmpQuote.AskExchange),
                MissReason: null);
        }
        catch (OperationCanceledException) when (tmpTimeoutCts.IsCancellationRequested
                                                 && !inCt.IsCancellationRequested)
        {
            m_Logger.LogWarning(
                "Polygon quote fetch timed out after {TimeoutMs}ms for {Ticker} @ {Ts:O} — transient",
                m_PerCallTimeoutMs, inTicker, inTsUtc);
            return new PolygonNbboFetch(PolygonNbboOutcome.Transient, null, "timeout");
        }
        catch (Exception ex)
        {
            // Don't poison the cache with transient errors.
            m_Logger.LogWarning(ex,
                "Polygon quote fetch failed for {Ticker} @ {Ts:O}",
                inTicker, inTsUtc);
            return new PolygonNbboFetch(PolygonNbboOutcome.Transient, null, "exception");
        }
        finally
        {
            m_Gate.Release();
        }
    }

    public void Dispose() => m_Gate.Dispose();

    // -------------- Polygon /v3/quotes payload shape --------------------
    // Public-ish surface kept internal; only the fields we read are typed.

    private sealed record PolygonQuotesEnvelope(
        [property: JsonPropertyName("results")] List<PolygonQuoteRow>? Results,
        [property: JsonPropertyName("status")] string? Status);

    private sealed record PolygonQuoteRow(
        [property: JsonPropertyName("bid_price")] double? BidPrice,
        [property: JsonPropertyName("ask_price")] double? AskPrice,
        [property: JsonPropertyName("bid_size")] int? BidSize,
        [property: JsonPropertyName("ask_size")] int? AskSize,
        [property: JsonPropertyName("bid_exchange")] int? BidExchange,
        [property: JsonPropertyName("ask_exchange")] int? AskExchange,
        [property: JsonPropertyName("sip_timestamp")] long? SipTimestamp);
}
