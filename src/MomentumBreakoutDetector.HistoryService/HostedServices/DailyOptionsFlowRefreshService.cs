using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Providers;
using TradingCalendar = MomentumBreakoutDetector.HistoryService.Domain.TradingCalendar;

namespace MomentumBreakoutDetector.HistoryService.HostedServices;

/// <summary>
/// PR 3 — daily cron that rolls "yesterday's" daily_options_flow row for
/// each tracked symbol at 08:00 ET on every weekday. Pairs with PR 2's
/// seeder backfill: the seeder fills a historical window in one shot;
/// this service maintains the trailing edge as new trading days complete.
///
/// <para>
/// <b>Schedule:</b> fires Mon–Fri at 08:00 America/New_York. On Mon it
/// rolls Fri's flow; on Tue–Fri it rolls the immediately-preceding
/// trading day's flow. Sat/Sun are skipped entirely (no fire). Holidays
/// that fall on a weekday: the cron still fires at 08:00 ET (the timer
/// fires every weekday) but
/// <see cref="TradingCalendar.PreviousTradingDay"/> walks back to the
/// most recent actual trading day, so the flow row written is for that
/// day. A run on Tue after a Mon holiday rolls Fri's flow.
/// </para>
///
/// <para>
/// <b>Idempotency:</b> the underlying provider's UPSERT is keyed on
/// (underlying, trade_date), so if the row already exists (e.g.
/// hand-backfilled or written by an earlier run that crashed mid-loop)
/// the second write overwrites with the same numbers and bumps
/// <c>fetched_at</c>. No data corruption hazard.
/// </para>
///
/// <para>
/// <b>Why 08:00 ET.</b> Polygon's daily aggregates settle a few hours
/// after the previous session close (typically by 22:00 ET same day, but
/// occasionally with re-print activity overnight). 08:00 ET puts us
/// safely after any after-hours re-prints, ahead of the 09:30 ET market
/// open, and well before any pre-market backtest run that may reference
/// yesterday's flow.
/// </para>
///
/// <para>
/// <b>Time injection.</b> Constructor takes <see cref="TimeProvider"/>
/// (System.TimeProvider, .NET 8+) so unit tests can fast-forward across
/// weekday boundaries with <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>.
/// </para>
/// </summary>
public sealed class DailyOptionsFlowRefreshService : BackgroundService
{
    private readonly IDailyOptionsFlowComputer m_Computer;
    private readonly IDailyOptionsFlowProvider m_Provider;
    private readonly TimeProvider m_TimeProvider;
    private readonly ILogger<DailyOptionsFlowRefreshService> m_Logger;
    private readonly DailyOptionsFlowRefreshOptions m_Opts;
    private readonly TimeZoneInfo m_EasternTz;

    public DailyOptionsFlowRefreshService(
        IDailyOptionsFlowComputer inComputer,
        IDailyOptionsFlowProvider inProvider,
        TimeProvider inTimeProvider,
        ILogger<DailyOptionsFlowRefreshService> inLogger,
        IOptions<DailyOptionsFlowRefreshOptions> inOpts)
    {
        m_Computer = inComputer;
        m_Provider = inProvider;
        m_TimeProvider = inTimeProvider;
        m_Logger = inLogger;
        m_Opts = inOpts.Value;
        m_EasternTz = ResolveEasternTz();
    }

    /// <summary>
    /// Compute the next-fire instant in UTC. Public so tests can validate
    /// the schedule in isolation. Always returns a strictly-future UTC
    /// timestamp (i.e. > <paramref name="inNowUtc"/>). Algorithm: convert
    /// "now" to ET wall-clock, walk forward day-by-day until we find a
    /// weekday whose <c>FireHourEt:FireMinuteEt</c> is strictly future,
    /// then convert back to UTC via <see cref="TimeZoneInfo.ConvertTimeToUtc"/>
    /// (DST-aware).
    /// </summary>
    public DateTimeOffset ComputeNextFireUtc(DateTimeOffset inNowUtc)
    {
        var tmpNowEt = TimeZoneInfo.ConvertTimeFromUtc(inNowUtc.UtcDateTime, m_EasternTz);

        // Build today's fire instant as a kind=Unspecified ET wall-clock,
        // then convert to a UTC instant for comparison + future returns.
        var tmpFireEtWall = new DateTime(
            tmpNowEt.Year, tmpNowEt.Month, tmpNowEt.Day,
            m_Opts.FireHourEt, m_Opts.FireMinuteEt, 0,
            DateTimeKind.Unspecified);
        var tmpFireUtc = new DateTimeOffset(
            TimeZoneInfo.ConvertTimeToUtc(tmpFireEtWall, m_EasternTz),
            TimeSpan.Zero);

        // If today's fire window is past or weekend, walk forward in
        // ET-wall-clock days until we find a weekday whose fire is
        // strictly future.
        while (tmpFireUtc <= inNowUtc
               || tmpFireEtWall.DayOfWeek == DayOfWeek.Saturday
               || tmpFireEtWall.DayOfWeek == DayOfWeek.Sunday)
        {
            tmpFireEtWall = tmpFireEtWall.AddDays(1);
            tmpFireUtc = new DateTimeOffset(
                TimeZoneInfo.ConvertTimeToUtc(tmpFireEtWall, m_EasternTz),
                TimeSpan.Zero);
        }

        return tmpFireUtc;
    }

    protected override async Task ExecuteAsync(CancellationToken inStopping)
    {
        m_Logger.LogInformation(
            "DailyOptionsFlowRefreshService starting; tracked symbols = [{Symbols}]; fires daily at {Hour:D2}:{Minute:D2} ET (Mon-Fri)",
            string.Join(",", m_Opts.Symbols), m_Opts.FireHourEt, m_Opts.FireMinuteEt);

        while (!inStopping.IsCancellationRequested)
        {
            var tmpNowUtc = m_TimeProvider.GetUtcNow();
            var tmpNextFireUtc = ComputeNextFireUtc(tmpNowUtc);
            var tmpDelay = tmpNextFireUtc - tmpNowUtc;

            m_Logger.LogInformation(
                "DailyOptionsFlowRefreshService: next fire at {NextFireUtc:O} ({DelayHours:F2}h from now)",
                tmpNextFireUtc, tmpDelay.TotalHours);

            try
            {
                await Task.Delay(tmpDelay, m_TimeProvider, inStopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }

            // After the delay completes, fire the refresh. Wrap in a
            // try/catch so a single failed cycle does not kill the
            // background loop — the next day's fire still happens.
            try
            {
                await RunOnceAsync(inStopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                m_Logger.LogError(ex,
                    "DailyOptionsFlowRefreshService cycle failed — continuing to next fire");
            }
        }
    }

    /// <summary>
    /// Fire one refresh cycle: compute "yesterday's trading day" and
    /// roll flow rows for all tracked symbols. Public + virtual so tests
    /// can drive a single cycle deterministically.
    /// </summary>
    public async Task RunOnceAsync(CancellationToken inCt)
    {
        // Compute "yesterday's trading day" relative to NOW in ET.
        var tmpNowEt = TimeZoneInfo.ConvertTime(m_TimeProvider.GetUtcNow(), m_EasternTz);
        var tmpToday = DateOnly.FromDateTime(tmpNowEt.DateTime);
        var tmpPreviousTradingDay = PreviousTradingDay(tmpToday);

        m_Logger.LogInformation(
            "DailyOptionsFlowRefreshService: rolling flow for {Date} (today-in-ET={Today})",
            tmpPreviousTradingDay, tmpToday);

        foreach (var tmpSymbol in m_Opts.Symbols)
        {
            inCt.ThrowIfCancellationRequested();
            try
            {
                var tmpRow = await m_Computer.ComputeAsync(
                    tmpSymbol, tmpPreviousTradingDay,
                    inMaxDte: m_Opts.MaxDte,
                    inConcurrency: m_Opts.Concurrency,
                    inCt: inCt).ConfigureAwait(false);

                if (tmpRow is null)
                {
                    // Empty contract universe ⇒ chain not warmed for that
                    // (symbol, day). Record a miss-marker and continue;
                    // operator can backfill later via the seeder.
                    await m_Provider.RecordMissAsync(
                        tmpSymbol, tmpPreviousTradingDay, tmpPreviousTradingDay,
                        "no-chain-rows-cached", inCt).ConfigureAwait(false);
                    m_Logger.LogWarning(
                        "DailyOptionsFlowRefreshService: chain not warmed for {Symbol} {Date} — recorded miss-marker",
                        tmpSymbol, tmpPreviousTradingDay);
                    continue;
                }

                await m_Provider.UpsertAsync(new[] { tmpRow }, inCt).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                // Per-symbol fail-quiet so one symbol's blip does not
                // skip the rest of the tracked list. Cron is daily —
                // tomorrow's run retries.
                m_Logger.LogError(ex,
                    "DailyOptionsFlowRefreshService: refresh failed for {Symbol} {Date} — continuing to next symbol",
                    tmpSymbol, tmpPreviousTradingDay);
            }
        }
    }

    /// <summary>
    /// Walk backward from <paramref name="inFromDate"/> until we land on
    /// a trading day. Returns the most-recent date strictly before
    /// <paramref name="inFromDate"/> that <see cref="TradingCalendar.IsTradingDay"/>
    /// considers a trading day. Public + static so tests can pin the
    /// calendar walk in isolation.
    /// </summary>
    public static DateOnly PreviousTradingDay(DateOnly inFromDate)
    {
        var tmpProbe = inFromDate.AddDays(-1);
        while (!TradingCalendar.IsTradingDay(tmpProbe))
        {
            tmpProbe = tmpProbe.AddDays(-1);
        }
        return tmpProbe;
    }

    private static TimeZoneInfo ResolveEasternTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
    }
}

/// <summary>
/// Configuration bind for <see cref="DailyOptionsFlowRefreshService"/>.
/// Bound from <c>History:DailyFlowRefresh:*</c>.
/// </summary>
public sealed class DailyOptionsFlowRefreshOptions
{
    public const string SectionName = "History:DailyFlowRefresh";

    /// <summary>Tracked symbols. Default <c>["TSLA"]</c>.</summary>
    public IList<string> Symbols { get; set; } = new List<string> { "TSLA" };

    /// <summary>Hour-of-day ET when the cron fires (24h). Default 8.</summary>
    public int FireHourEt { get; set; } = 8;

    /// <summary>Minute-of-hour when the cron fires. Default 0 (i.e. 08:00 ET).</summary>
    public int FireMinuteEt { get; set; } = 0;

    /// <summary>Maximum DTE (days-to-expiry) for the contract universe. Default 60.</summary>
    public int MaxDte { get; set; } = 60;

    /// <summary>Concurrency cap on per-contract /v2/aggs fetches. Default 32.</summary>
    public int Concurrency { get; set; } = 32;
}
