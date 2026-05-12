using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MomentumBreakoutDetector.HistoryService.Domain;

/// <summary>
/// Alpaca-backed <see cref="IMarketCalendar"/>. Caches calendar entries
/// keyed by ET date; populated lazily via on-demand range fetches.
///
/// <para>
/// History-service queries arbitrary historical dates (going back to
/// 2022 in our seed window) AND future dates (occasionally — backtest
/// "today" path with same-day projections), so a fixed ±30-day cache
/// doesn't fit. Strategy: lazy fetch by year. On a TryIsTradingDay
/// miss, fetch the calendar for the surrounding year and cache every
/// returned trading day; mark every non-returned date in the range as
/// non-trading.
/// </para>
///
/// <para>
/// Returns <c>null</c> from Try* methods when the calendar can't answer
/// (upstream unreachable AND year-cache empty). Caller (the
/// <see cref="TradingCalendar"/> static facade) then falls back to the
/// hardcoded NYSE holiday list — covers 2022-2026; for 2027+ dates an
/// outage means we degrade to "weekday is trading day", which is the
/// pre-existing pre-2026-05-12 behaviour.
/// </para>
/// </summary>
public sealed class AlpacaMarketCalendar : IMarketCalendar, IDisposable
{
    private readonly HttpClient m_Http;
    private readonly ILogger<AlpacaMarketCalendar> m_Logger;
    // Per-date cache: present-with-value = trading day with that session;
    // present-with-null = explicitly non-trading; absent = not-yet-fetched.
    private readonly ConcurrentDictionary<DateOnly, MarketSession?> m_Cache = new();
    private readonly ConcurrentDictionary<int, byte> m_FetchedYears = new();
    private readonly SemaphoreSlim m_FetchLock = new(1, 1);
    private bool m_Disposed;

    public AlpacaMarketCalendar(
        IHttpClientFactory inHttpFactory,
        ILogger<AlpacaMarketCalendar> inLogger)
    {
        m_Logger = inLogger;
        m_Http = inHttpFactory.CreateClient("alpaca-calendar");
        if (m_Http.DefaultRequestHeaders.Accept.Count == 0)
        {
            m_Http.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));
        }
    }

    public bool? TryIsTradingDay(DateOnly inEtDate)
    {
        EnsureYearFetched(inEtDate.Year);
        if (m_Cache.TryGetValue(inEtDate, out var tmpSession))
        {
            return tmpSession is not null;
        }
        // Year wasn't in the fetched set even after EnsureYearFetched —
        // upstream call failed. Surface null so the static facade falls
        // back to its hardcoded list.
        return null;
    }

    public bool? TryIsHalfDay(DateOnly inEtDate)
    {
        EnsureYearFetched(inEtDate.Year);
        if (m_Cache.TryGetValue(inEtDate, out var tmpSession))
        {
            return tmpSession?.IsEarlyClose ?? false;
        }
        return null;
    }

    public MarketSession? GetSession(DateOnly inEtDate)
    {
        EnsureYearFetched(inEtDate.Year);
        m_Cache.TryGetValue(inEtDate, out var tmpSession);
        return tmpSession;
    }

    /// <summary>
    /// Synchronous lazy-fetch the whole year if we don't have it cached.
    /// Best-effort: a failed fetch leaves the year unmarked-as-fetched
    /// so the next call retries; the immediate caller falls back via
    /// the null return on Try* methods.
    /// </summary>
    private void EnsureYearFetched(int inYear)
    {
        if (m_FetchedYears.ContainsKey(inYear)) return;
        m_FetchLock.Wait();
        try
        {
            if (m_FetchedYears.ContainsKey(inYear)) return;
            var tmpFrom = new DateOnly(inYear, 1, 1);
            var tmpTo = new DateOnly(inYear, 12, 31);
            // Block on the HTTP call. The static-facade call path is
            // synchronous (TradingCalendar.IsTradingDay returns bool, not
            // Task<bool>); we can't change that without churning ~20
            // call sites. The fetch happens once per year per process so
            // the blocking cost amortizes to near-zero.
            var tmpUrl = $"v2/calendar?start={tmpFrom:yyyy-MM-dd}&end={tmpTo:yyyy-MM-dd}";
            CalendarRow[]? tmpRows;
            try
            {
                tmpRows = m_Http.GetFromJsonAsync<CalendarRow[]>(tmpUrl).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                m_Logger.LogWarning(ex,
                    "AlpacaMarketCalendar: year {Year} fetch failed; falling back to hardcoded list",
                    inYear);
                return;
            }
            if (tmpRows is null) return;

            var tmpSeen = new HashSet<DateOnly>();
            foreach (var tmpRow in tmpRows)
            {
                if (!DateOnly.TryParse(tmpRow.Date, out var tmpDate)) continue;
                if (!TimeOnly.TryParse(tmpRow.Open, out var tmpOpen)) continue;
                if (!TimeOnly.TryParse(tmpRow.Close, out var tmpClose)) continue;
                var tmpEarly = tmpClose < new TimeOnly(15, 30);
                m_Cache[tmpDate] = new MarketSession(tmpDate, tmpOpen, tmpClose, tmpEarly);
                tmpSeen.Add(tmpDate);
            }
            // Mark every day in the year that wasn't returned as non-trading.
            for (var tmpD = tmpFrom; tmpD <= tmpTo; tmpD = tmpD.AddDays(1))
            {
                if (!tmpSeen.Contains(tmpD)) m_Cache[tmpD] = null;
            }
            m_FetchedYears[inYear] = 0;
            m_Logger.LogInformation(
                "AlpacaMarketCalendar: cached {Year} ({Sessions} trading days)",
                inYear, tmpSeen.Count);
        }
        finally
        {
            m_FetchLock.Release();
        }
    }

    public void Dispose()
    {
        if (m_Disposed) return;
        m_Disposed = true;
        m_FetchLock.Dispose();
        m_Http.Dispose();
    }

    private sealed record CalendarRow(
        [property: JsonPropertyName("date")] string Date,
        [property: JsonPropertyName("open")] string Open,
        [property: JsonPropertyName("close")] string Close);
}

/// <summary>
/// Background warm-up: at startup, pre-fetch the current year and the
/// adjacent year so the first call from a refresh service / on-demand
/// gap-detection doesn't pay the lazy-fetch latency.
/// </summary>
public sealed class MarketCalendarWarmupService : BackgroundService
{
    private readonly AlpacaMarketCalendar m_Calendar;
    private readonly ILogger<MarketCalendarWarmupService> m_Logger;

    public MarketCalendarWarmupService(
        AlpacaMarketCalendar inCalendar,
        ILogger<MarketCalendarWarmupService> inLogger)
    {
        m_Calendar = inCalendar;
        m_Logger = inLogger;
    }

    protected override Task ExecuteAsync(CancellationToken inCt)
    {
        // Touch the current year + the surrounding two so the lazy
        // fetcher runs once at startup rather than first-call.
        var tmpToday = DateOnly.FromDateTime(DateTime.UtcNow);
        try
        {
            m_Calendar.GetSession(tmpToday);
            m_Calendar.GetSession(new DateOnly(tmpToday.Year - 1, 1, 1));
            m_Calendar.GetSession(new DateOnly(tmpToday.Year + 1, 1, 1));
            m_Logger.LogInformation("MarketCalendarWarmupService: years {Prev}, {This}, {Next} pre-cached",
                tmpToday.Year - 1, tmpToday.Year, tmpToday.Year + 1);
        }
        catch (Exception ex)
        {
            m_Logger.LogWarning(ex, "MarketCalendarWarmupService: pre-warm failed; lazy fetch will retry");
        }
        return Task.CompletedTask;
    }
}
