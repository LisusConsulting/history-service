using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Providers;
using TradingCalendar = MomentumBreakoutDetector.HistoryService.Domain.TradingCalendar;

namespace MomentumBreakoutDetector.HistoryService.HostedServices;

/// <summary>
/// Wave C / PR 6 of the ATM-IV full historical coverage plan
/// (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md).
/// Daily 08:00 ET cron that rolls "yesterday's" daily_atm_iv row for
/// each tracked symbol on every weekday. Pairs with the seeder backfill
/// (<c>--surface daily_atm_iv</c>) which fills a historical window in
/// one shot; this service maintains the trailing edge as new trading
/// days complete.
///
/// <para>
/// <b>Schedule.</b> Mon-Fri at 08:00 America/New_York, identical to
/// <see cref="DailyOptionsFlowRefreshService"/>. On Mon it rolls Fri's
/// row; on Tue-Fri it rolls the immediately-preceding trading day.
/// Holidays falling on a weekday: the cron still fires at 08:00 ET (the
/// timer fires every weekday) but
/// <see cref="TradingCalendar.PreviousTradingDay"/>-equivalent walk
/// finds the prior actual trading day.
/// </para>
///
/// <para>
/// <b>Why 08:00 ET.</b> Aligns with the daily_options_flow cron — both
/// surfaces depend on yesterday's underlying close + persisted snapshots,
/// which settle by 22:00 ET prior day. 08:00 ET puts us safely after any
/// after-hours re-prints, ahead of the 09:30 ET market open, and well
/// before any pre-market backtest run that may reference yesterday's IV.
/// </para>
///
/// <para>
/// <b>Idempotency.</b> The underlying provider's UPSERT is keyed on
/// (underlying_ticker, trade_date), so a re-fire (operator-triggered or
/// container restart) overwrites with the same value (the aggregation
/// is deterministic over the same input snapshots) and bumps
/// <c>fetched_at</c>. If yesterday's snapshot universe was empty (e.g.
/// the live-capture flag was OFF or chain missing), the aggregator
/// returns null and the cron records a miss-marker via
/// <see cref="IDailyAtmIvProvider.RecordMissAsync"/>.
/// </para>
///
/// <para>
/// <b>Time injection.</b> Constructor takes <see cref="TimeProvider"/>
/// so unit tests can fast-forward across weekday boundaries with
/// <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>.
/// </para>
/// </summary>
public sealed class DailyAtmIvRefreshService : BackgroundService
{
    private readonly IDailyAtmIvAggregator m_Aggregator;
    private readonly IDailyAtmIvProvider m_Provider;
    private readonly TimeProvider m_TimeProvider;
    private readonly ILogger<DailyAtmIvRefreshService> m_Logger;
    private readonly DailyAtmIvRefreshOptions m_Opts;
    private readonly TimeZoneInfo m_EasternTz;

    public DailyAtmIvRefreshService(
        IDailyAtmIvAggregator inAggregator,
        IDailyAtmIvProvider inProvider,
        TimeProvider inTimeProvider,
        ILogger<DailyAtmIvRefreshService> inLogger,
        IOptions<DailyAtmIvRefreshOptions> inOpts)
    {
        m_Aggregator = inAggregator;
        m_Provider = inProvider;
        m_TimeProvider = inTimeProvider;
        m_Logger = inLogger;
        m_Opts = inOpts.Value;
        m_EasternTz = ResolveEasternTz();
    }

    /// <summary>
    /// Compute the next-fire instant in UTC. Public so tests can validate
    /// the schedule in isolation. Always returns a strictly-future UTC
    /// timestamp (i.e. &gt; <paramref name="inNowUtc"/>). Mirrors
    /// <see cref="DailyOptionsFlowRefreshService.ComputeNextFireUtc"/>.
    /// </summary>
    public DateTimeOffset ComputeNextFireUtc(DateTimeOffset inNowUtc)
    {
        var tmpNowEt = TimeZoneInfo.ConvertTimeFromUtc(inNowUtc.UtcDateTime, m_EasternTz);

        var tmpFireEtWall = new DateTime(
            tmpNowEt.Year, tmpNowEt.Month, tmpNowEt.Day,
            m_Opts.FireHourEt, m_Opts.FireMinuteEt, 0,
            DateTimeKind.Unspecified);
        var tmpFireUtc = new DateTimeOffset(
            TimeZoneInfo.ConvertTimeToUtc(tmpFireEtWall, m_EasternTz),
            TimeSpan.Zero);

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
            "DailyAtmIvRefreshService starting; tracked symbols=[{Symbols}] fires {Hour:D2}:{Minute:D2} ET (Mon-Fri)",
            string.Join(",", m_Opts.Symbols), m_Opts.FireHourEt, m_Opts.FireMinuteEt);

        while (!inStopping.IsCancellationRequested)
        {
            var tmpNowUtc = m_TimeProvider.GetUtcNow();
            var tmpNextFireUtc = ComputeNextFireUtc(tmpNowUtc);
            var tmpDelay = tmpNextFireUtc - tmpNowUtc;

            m_Logger.LogInformation(
                "DailyAtmIvRefreshService: next fire at {NextFireUtc:O} ({DelayHours:F2}h from now)",
                tmpNextFireUtc, tmpDelay.TotalHours);

            try
            {
                await Task.Delay(tmpDelay, m_TimeProvider, inStopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }

            try
            {
                await RunOnceAsync(inStopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                m_Logger.LogError(ex,
                    "DailyAtmIvRefreshService cycle failed — continuing to next fire");
            }
        }
    }

    /// <summary>
    /// Fire one refresh cycle. Public + virtual so tests can drive a
    /// single cycle deterministically. For each tracked symbol:
    ///   1. Compute "yesterday's trading day".
    ///   2. Aggregate per-contract snapshots into a daily row.
    ///   3. UPSERT (success) or RecordMiss (empty aggregate).
    /// </summary>
    public async Task RunOnceAsync(CancellationToken inCt)
    {
        var tmpNowEt = TimeZoneInfo.ConvertTime(m_TimeProvider.GetUtcNow(), m_EasternTz);
        var tmpToday = DateOnly.FromDateTime(tmpNowEt.DateTime);
        var tmpPreviousTradingDay = PreviousTradingDay(tmpToday);

        m_Logger.LogInformation(
            "DailyAtmIvRefreshService: rolling daily_atm_iv for {Date} (today-in-ET={Today})",
            tmpPreviousTradingDay, tmpToday);

        foreach (var tmpSymbol in m_Opts.Symbols)
        {
            inCt.ThrowIfCancellationRequested();
            try
            {
                var tmpRow = await m_Aggregator.AggregateAsync(
                    tmpSymbol, tmpPreviousTradingDay, inCt).ConfigureAwait(false);

                if (tmpRow is null)
                {
                    await m_Provider.RecordMissAsync(
                        tmpSymbol, tmpPreviousTradingDay, tmpPreviousTradingDay,
                        "no-snapshot-rows", inCt).ConfigureAwait(false);
                    m_Logger.LogWarning(
                        "DailyAtmIvRefreshService: zero ATM-band snapshots for {Symbol} {Date} — recorded miss-marker",
                        tmpSymbol, tmpPreviousTradingDay);
                    continue;
                }

                await m_Provider.UpsertAsync(new[] { tmpRow }, inCt).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                m_Logger.LogError(ex,
                    "DailyAtmIvRefreshService: refresh failed for {Symbol} {Date} — continuing to next symbol",
                    tmpSymbol, tmpPreviousTradingDay);
            }
        }
    }

    /// <summary>
    /// Walk backward from <paramref name="inFromDate"/> until we land on
    /// a trading day (per <see cref="TradingCalendar.IsTradingDay"/>).
    /// Public + static so tests can pin the calendar walk in isolation.
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
/// Configuration bind for <see cref="DailyAtmIvRefreshService"/>.
/// Bound from <c>History:DailyAtmIvRefresh:*</c>.
/// </summary>
public sealed class DailyAtmIvRefreshOptions
{
    public const string SectionName = "History:DailyAtmIvRefresh";

    /// <summary>Tracked symbols. Default <c>["TSLA"]</c>.</summary>
    public IList<string> Symbols { get; set; } = new List<string> { "TSLA" };

    /// <summary>Hour-of-day ET when the cron fires (24h). Default 8.</summary>
    public int FireHourEt { get; set; } = 8;

    /// <summary>Minute-of-hour when the cron fires. Default 0 (i.e. 08:00 ET).</summary>
    public int FireMinuteEt { get; set; } = 0;
}
