using Dapper;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using TreyThomasCodes.Polygon.Models.Options;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;
using TradingCalendar = MomentumBreakoutDetector.HistoryService.Domain.TradingCalendar;

namespace MomentumBreakoutDetector.HistoryService.HostedServices;

/// <summary>
/// Wave B / PR 4 of the ATM-IV full historical coverage plan
/// (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md).
/// Captures live option snapshots from Polygon's
/// <c>/v3/snapshot/options/{underlying}</c> every 5 minutes during RTH,
/// filtered to ATM±5% × 0-60 DTE, and persists with
/// <c>source='polygon_live'</c> into <c>historical_options_snapshots</c>.
///
/// <para>
/// <b>Flag-gated.</b> Deploys with
/// <see cref="LiveOptionsSnapshotCaptureOptions.LiveSnapshotCaptureEnabled"/>
/// = <c>false</c>. Operator flips ON post-bootstrap (Wave C / PR 9). When
/// the flag is <c>false</c>, the service registers but the per-tick body
/// short-circuits — no Polygon calls, no DB writes. The schedule itself
/// still ticks (so flipping the flag mid-flight applies on the next tick
/// boundary, no restart needed).
/// </para>
///
/// <para>
/// <b>Schedule.</b> RTH-window-only; sleeps until the next scheduled
/// 5-min tick during 09:30–16:00 ET on weekdays. Outside RTH and on
/// weekends/holidays the service idles until the next RTH boundary.
/// Half-days (early-close at 13:00 ET): the service stops capturing at
/// the half-day close, mirroring the trading calendar's half-day session
/// minutes (no captures during 13:00–16:00 ET on those dates). DST is
/// handled by the trading calendar's resolved Eastern timezone.
/// </para>
///
/// <para>
/// <b>ATM band.</b> The strike band is ±<see cref="LiveOptionsSnapshotCaptureOptions.StrikeBandPct"/>
/// (default 5%) of the underlying's <i>current</i> price. We resolve the
/// underlying price by reading <see cref="OptionUnderlyingAsset.Price"/>
/// off the first non-null snapshot in the chain payload — this is the
/// price Polygon used to populate its own greeks/IV, so it's the natural
/// anchor for "what's ATM right now". If no snapshot has the underlying
/// price set (genuinely empty chain), the cycle is a no-op for that
/// symbol.
/// </para>
///
/// <para>
/// <b>Idempotency.</b> Each persist is an INSERT … ON CONFLICT (ticker,
/// snapshot_date) DO UPDATE so an out-of-band re-fire (timer drift,
/// container restart) overwrites with the freshest values. The 5-min
/// cadence is not strict; quote-time microseconds embedded in
/// snapshot_date eliminate accidental collisions across capture cycles.
/// </para>
///
/// <para>
/// <b>Concurrency.</b> One scheduled fire per process. Within a fire the
/// service issues one chain-snapshot HTTP call per tracked symbol
/// sequentially (the per-symbol API call is concurrency-bounded inside
/// the polygon-net-client SDK's pluggable handler chain — see
/// <c>ConcurrencyLimitingHandler</c> in this repo). This service never
/// fans out per-contract calls (the chain-snapshot call returns the
/// whole filtered band in one round-trip).
/// </para>
/// </summary>
public sealed class LiveOptionsSnapshotCaptureService : BackgroundService
{
    private readonly IOptionsService m_PolygonOptions;
    private readonly TimeProvider m_TimeProvider;
    private readonly ILogger<LiveOptionsSnapshotCaptureService> m_Logger;
    private readonly LiveOptionsSnapshotCaptureOptions m_Opts;
    private readonly string m_ConnectionString;
    private readonly TimeZoneInfo m_EasternTz;

    public LiveOptionsSnapshotCaptureService(
        IOptionsService inPolygonOptions,
        TimeProvider inTimeProvider,
        ILogger<LiveOptionsSnapshotCaptureService> inLogger,
        IOptions<LiveOptionsSnapshotCaptureOptions> inOpts,
        IOptions<HistoryServiceOptions> inHistoryOpts)
    {
        m_PolygonOptions = inPolygonOptions;
        m_TimeProvider = inTimeProvider;
        m_Logger = inLogger;
        m_Opts = inOpts.Value;
        m_ConnectionString = inHistoryOpts.Value.ConnectionString;
        m_EasternTz = ResolveEasternTz();
    }

    /// <summary>
    /// Compute the next-fire instant in UTC. Public so tests can validate
    /// the schedule in isolation. Always returns a strictly-future UTC
    /// timestamp ≥ <paramref name="inNowUtc"/>. Algorithm:
    /// <list type="number">
    ///   <item>If "now" in ET is before today's RTH-open on a trading day,
    ///         next fire is today's 09:30 ET.</item>
    ///   <item>If "now" is during RTH on a trading day, next fire is the
    ///         next 5-min boundary (00, 05, 10, 15, …) at or after now+1s.
    ///         If that boundary lands at or after today's RTH-close
    ///         (16:00 ET, or 13:00 ET on half-days), advance to the next
    ///         trading day's 09:30 ET.</item>
    ///   <item>If "now" is after RTH-close or non-trading day, advance
    ///         to the next trading day's 09:30 ET.</item>
    /// </list>
    /// </summary>
    public DateTimeOffset ComputeNextFireUtc(DateTimeOffset inNowUtc)
    {
        var tmpNowEt = TimeZoneInfo.ConvertTimeFromUtc(inNowUtc.UtcDateTime, m_EasternTz);
        var tmpToday = DateOnly.FromDateTime(tmpNowEt);

        if (TradingCalendar.IsTradingDay(tmpToday))
        {
            var tmpOpen = ToUtc(tmpToday, new TimeSpan(9, 30, 0));
            var tmpClose = TradingCalendar.IsHalfDay(tmpToday)
                ? ToUtc(tmpToday, new TimeSpan(13, 0, 0))
                : ToUtc(tmpToday, new TimeSpan(16, 0, 0));

            if (inNowUtc < tmpOpen) return tmpOpen;
            if (inNowUtc < tmpClose)
            {
                // Find the next 5-min boundary at or after now+1s.
                // Boundary anchored to the RTH-open instant.
                var tmpElapsed = inNowUtc - tmpOpen;
                var tmpIntervalMin = m_Opts.IntervalMinutes;
                var tmpNextSlot = (int)System.Math.Floor(tmpElapsed.TotalMinutes / tmpIntervalMin) + 1;
                var tmpNextFire = tmpOpen.AddMinutes(tmpNextSlot * tmpIntervalMin);
                if (tmpNextFire >= tmpClose) return NextTradingDayOpen(tmpToday);
                return tmpNextFire;
            }
        }

        // Non-trading day, or past today's close → next trading day's open.
        return NextTradingDayOpen(tmpToday);
    }

    private DateTimeOffset NextTradingDayOpen(DateOnly inFromToday)
    {
        var tmpProbe = inFromToday.AddDays(1);
        while (!TradingCalendar.IsTradingDay(tmpProbe)) tmpProbe = tmpProbe.AddDays(1);
        return ToUtc(tmpProbe, new TimeSpan(9, 30, 0));
    }

    private DateTimeOffset ToUtc(DateOnly inDate, TimeSpan inEtTime)
    {
        var tmpEt = new DateTime(inDate.Year, inDate.Month, inDate.Day, 0, 0, 0,
            DateTimeKind.Unspecified) + inEtTime;
        return new DateTimeOffset(TimeZoneInfo.ConvertTimeToUtc(tmpEt, m_EasternTz),
            TimeSpan.Zero);
    }

    protected override async Task ExecuteAsync(CancellationToken inStopping)
    {
        m_Logger.LogInformation(
            "LiveOptionsSnapshotCaptureService starting; enabled={Enabled} symbols=[{Symbols}] " +
            "interval={IntervalMin}min strike-band=±{Pct} dte-max={Dte}",
            m_Opts.LiveSnapshotCaptureEnabled,
            string.Join(",", m_Opts.LiveSnapshotCaptureSymbols),
            m_Opts.IntervalMinutes,
            m_Opts.StrikeBandPct,
            m_Opts.SnapshotDteMaxDays);

        while (!inStopping.IsCancellationRequested)
        {
            var tmpNowUtc = m_TimeProvider.GetUtcNow();
            var tmpNextFireUtc = ComputeNextFireUtc(tmpNowUtc);
            var tmpDelay = tmpNextFireUtc - tmpNowUtc;
            if (tmpDelay <= TimeSpan.Zero) tmpDelay = TimeSpan.FromSeconds(1);

            try
            {
                await Task.Delay(tmpDelay, m_TimeProvider, inStopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }

            // Flag check happens AFTER the delay so toggling the flag
            // mid-flight takes effect on the next scheduled tick — no
            // restart needed.
            if (!m_Opts.LiveSnapshotCaptureEnabled)
            {
                m_Logger.LogDebug(
                    "LiveOptionsSnapshotCaptureService tick at {Ts:O} skipped — flag disabled",
                    tmpNextFireUtc);
                continue;
            }

            try
            {
                await RunOnceAsync(tmpNextFireUtc, inStopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                m_Logger.LogError(ex,
                    "LiveOptionsSnapshotCaptureService cycle at {Ts:O} failed — continuing",
                    tmpNextFireUtc);
            }
        }
    }

    /// <summary>
    /// Fire one capture cycle. Public + virtual so tests can drive a
    /// single cycle deterministically without going through the timer.
    /// </summary>
    public async Task RunOnceAsync(DateTimeOffset inSnapshotTs, CancellationToken inCt)
    {
        foreach (var tmpSymbol in m_Opts.LiveSnapshotCaptureSymbols)
        {
            inCt.ThrowIfCancellationRequested();
            try
            {
                await CaptureForSymbolAsync(tmpSymbol, inSnapshotTs, inCt).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                m_Logger.LogError(ex,
                    "LiveOptionsSnapshotCaptureService: capture failed for {Symbol} at {Ts:O} — continuing to next symbol",
                    tmpSymbol, inSnapshotTs);
            }
        }
    }

    private async Task CaptureForSymbolAsync(
        string inSymbol, DateTimeOffset inSnapshotTs, CancellationToken inCt)
    {
        // Limit DTE band by querying with an explicit upper-bound on
        // expiration_date (today + DTE max). The chain-snapshot endpoint
        // does NOT expose a strike-band query parameter, so we fetch all
        // strikes within the DTE window and filter to ATM±5% client-side
        // after we know the underlying price.
        var tmpToday = DateOnly.FromDateTime(inSnapshotTs.UtcDateTime);
        var tmpMaxExp = tmpToday.AddDays(m_Opts.SnapshotDteMaxDays);

        var tmpReq = new GetChainSnapshotRequest
        {
            UnderlyingAsset = inSymbol,
            ExpirationDateGte = tmpToday.ToString("yyyy-MM-dd"),
            ExpirationDateLte = tmpMaxExp.ToString("yyyy-MM-dd"),
            // Polygon caps at 250 per page; we don't paginate here
            // because ATM±5% × 0-60 DTE on TSLA is well under one page
            // worth (~120 contracts). If the band gets wider later, add
            // cursor pagination.
            Limit = 250,
        };

        var tmpResp = await m_PolygonOptions.GetChainSnapshotAsync(tmpReq, inCt).ConfigureAwait(false);
        var tmpRows = tmpResp?.Results;
        if (tmpRows is null || tmpRows.Count == 0)
        {
            m_Logger.LogWarning(
                "LiveOptionsSnapshotCaptureService: empty chain for {Symbol} at {Ts:O}",
                inSymbol, inSnapshotTs);
            return;
        }

        // Resolve underlying price from the first non-null snapshot.
        var tmpUnderlyingPrice = tmpRows
            .Select(r => r.UnderlyingAsset?.Price)
            .FirstOrDefault(p => p is not null and > 0m);
        if (tmpUnderlyingPrice is null)
        {
            m_Logger.LogWarning(
                "LiveOptionsSnapshotCaptureService: no underlying price in chain for {Symbol} at {Ts:O}",
                inSymbol, inSnapshotTs);
            return;
        }

        // Filter to ATM band.
        var tmpBand = (decimal)m_Opts.StrikeBandPct;
        var tmpKLow = tmpUnderlyingPrice.Value * (1m - tmpBand);
        var tmpKHigh = tmpUnderlyingPrice.Value * (1m + tmpBand);

        var tmpFiltered = FilterAtmBand(tmpRows, tmpKLow, tmpKHigh).ToList();

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        var tmpPersisted = 0;
        foreach (var tmpSnap in tmpFiltered)
        {
            inCt.ThrowIfCancellationRequested();
            await PersistSnapshotAsync(
                tmpConn, tmpSnap, inSnapshotTs.UtcDateTime, tmpUnderlyingPrice.Value, inCt)
                .ConfigureAwait(false);
            tmpPersisted++;
        }

        m_Logger.LogInformation(
            "LiveOptionsSnapshotCaptureService: {Symbol} {Ts:O} S={S:F2} band=[{Lo:F2}, {Hi:F2}] " +
            "chain={Total} → ATM-band={Filtered} persisted={Persisted}",
            inSymbol, inSnapshotTs, tmpUnderlyingPrice, tmpKLow, tmpKHigh,
            tmpRows.Count, tmpFiltered.Count, tmpPersisted);
    }

    /// <summary>
    /// Filter a chain to the ATM band (strike in [<paramref name="inKLow"/>,
    /// <paramref name="inKHigh"/>]). Internal so unit tests can drive the
    /// filter against a hand-built chain.
    /// </summary>
    internal static IEnumerable<OptionSnapshot> FilterAtmBand(
        IEnumerable<OptionSnapshot> inRows, decimal inKLow, decimal inKHigh)
    {
        foreach (var tmpRow in inRows)
        {
            var tmpStrike = tmpRow.Details?.StrikePrice;
            if (tmpStrike is null) continue;
            if (tmpStrike < inKLow || tmpStrike > inKHigh) continue;
            yield return tmpRow;
        }
    }

    /// <summary>
    /// Persist one snapshot with <c>source='polygon_live'</c>. UPSERT on
    /// (ticker, snapshot_date) so an out-of-band re-fire of the same
    /// timestamp overwrites the freshest values.
    /// </summary>
    private static async Task PersistSnapshotAsync(
        NpgsqlConnection inConn, OptionSnapshot inSnap, DateTime inSnapshotDate,
        decimal inUnderlyingPrice, CancellationToken inCt)
    {
        var tmpTicker = inSnap.Details?.Ticker;
        if (string.IsNullOrWhiteSpace(tmpTicker)) return;

        await inConn.ExecuteAsync(
            """
            INSERT INTO historical_options_snapshots
              (ticker, snapshot_date, bid_price, ask_price, volume, open_interest,
               implied_volatility, delta, gamma, theta, vega,
               underlying_price, source)
            VALUES
              (@Ticker, @Ts, @Bid, @Ask, @Vol, @OI,
               @Iv, @Delta, @Gamma, @Theta, @Vega,
               @Underlying, 'polygon_live')
            ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
              bid_price          = EXCLUDED.bid_price,
              ask_price          = EXCLUDED.ask_price,
              volume             = EXCLUDED.volume,
              open_interest      = EXCLUDED.open_interest,
              implied_volatility = EXCLUDED.implied_volatility,
              delta              = EXCLUDED.delta,
              gamma              = EXCLUDED.gamma,
              theta              = EXCLUDED.theta,
              vega               = EXCLUDED.vega,
              underlying_price   = EXCLUDED.underlying_price,
              source             = EXCLUDED.source
            """,
            new
            {
                Ticker = tmpTicker,
                Ts = inSnapshotDate,
                Bid = (object?)inSnap.LastQuote?.Bid ?? DBNull.Value,
                Ask = (object?)inSnap.LastQuote?.Ask ?? DBNull.Value,
                Vol = (object?)inSnap.Day?.Volume ?? DBNull.Value,
                OI = (object?)(long?)inSnap.OpenInterest ?? DBNull.Value,
                Iv = (object?)inSnap.ImpliedVolatility ?? DBNull.Value,
                Delta = (object?)inSnap.Greeks?.Delta ?? DBNull.Value,
                Gamma = (object?)inSnap.Greeks?.Gamma ?? DBNull.Value,
                Theta = (object?)inSnap.Greeks?.Theta ?? DBNull.Value,
                Vega = (object?)inSnap.Greeks?.Vega ?? DBNull.Value,
                Underlying = inUnderlyingPrice,
            }).ConfigureAwait(false);
    }

    private static TimeZoneInfo ResolveEasternTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
    }
}

/// <summary>
/// Configuration bind for <see cref="LiveOptionsSnapshotCaptureService"/>.
/// Bound from <c>History:LiveSnapshotCapture:*</c> per the plan brief.
/// </summary>
public sealed class LiveOptionsSnapshotCaptureOptions
{
    public const string SectionName = "History:LiveSnapshotCapture";

    /// <summary>
    /// Master enable flag. <b>Defaults OFF per plan brief</b> — operator
    /// flips ON post-bootstrap (Wave C / PR 9). When false, the service
    /// is registered but per-tick body short-circuits.
    /// </summary>
    public bool LiveSnapshotCaptureEnabled { get; set; } = false;

    /// <summary>Tracked symbols. Default <c>["TSLA"]</c>.</summary>
    public IList<string> LiveSnapshotCaptureSymbols { get; set; } = new List<string> { "TSLA" };

    /// <summary>Capture cadence in minutes. Default 5 per plan brief.</summary>
    public int IntervalMinutes { get; set; } = 5;

    /// <summary>Strike band as a fraction of underlying price. Default 0.05 (±5%).</summary>
    public double StrikeBandPct { get; set; } = 0.05;

    /// <summary>Maximum days-to-expiry. Default 60.</summary>
    public int SnapshotDteMaxDays { get; set; } = 60;
}
