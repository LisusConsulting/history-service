using System.Diagnostics;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;
// TradingCalendar lives in MomentumBreakoutDetector.HistoryService.Domain.
// Pulled in via type-alias rather than `using namespace` because the Domain
// namespace also defines BarTimeframe, which collides with the proto enum
// of the same name in this file.
using TradingCalendar = MomentumBreakoutDetector.HistoryService.Domain.TradingCalendar;
// The generated gRPC client lives at
// MomentumBreakoutDetector.HistoryService.Contracts.V1.HistoryServiceContainer.HistoryServiceClient.
// Alias the static container so we don't collide with this assembly's
// own MomentumBreakoutDetector.HistoryService.* namespace tree.
using HistoryServiceContainer = MomentumBreakoutDetector.HistoryService.Contracts.V1.HistoryService;

namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// One-shot seed driver. Walks a date window day-by-day, fetches the
/// option chain, filters to ATM ± 5% strikes / DTE ≤ 10, then pulls
/// minute-resolution NBBO for every (contract, RTH minute) tuple.
/// Bars + chains + macro are warmed up-front via EnsureRangeCached.
/// </summary>
public sealed class SeedEngine
{
    private readonly HistoryServiceContainer.HistoryServiceClient m_Client;
    private readonly SeedOptions m_Opts;
    private readonly Checkpoint m_Cp;
    private readonly StreamWriter? m_LogWriter;

    // Throughput accounting.
    private long m_NbboCallsThisDay;
    private long m_NbboCallsTotal;
    private long m_NbboFailuresTotal;
    private readonly Stopwatch m_RunSw = new();

    public SeedEngine(
        HistoryServiceContainer.HistoryServiceClient inClient,
        SeedOptions inOpts,
        Checkpoint inCp,
        StreamWriter? inLogWriter)
    {
        m_Client = inClient;
        m_Opts = inOpts;
        m_Cp = inCp;
        m_LogWriter = inLogWriter;
    }

    public async Task RunAsync(CancellationToken inCt)
    {
        m_RunSw.Start();

        // ── 1. Range warmup (bars + chains + macro). NBBO is excluded from
        //      the warmup; minute NBBO for ~240 contracts × 390 minutes is
        //      what the per-day loop owns and is too expensive for a
        //      blanket EnsureRangeCached.
        Log($"warmup: bars + chains + macro for {m_Opts.Symbol} {m_Opts.From:yyyy-MM-dd}..{m_Opts.To:yyyy-MM-dd}");
        await WarmupRangeAsync(inCt);

        // ── 2. Day-by-day loop.
        var tmpStartFrom = m_Cp.LastCompletedDate is { } lc ? lc.AddDays(1) : m_Opts.From;
        var tmpDays = TradingCalendar.EnumerateTradingDays(tmpStartFrom, m_Opts.To).ToList();
        var tmpTotalDays = tmpDays.Count;
        Log($"plan: {tmpTotalDays} trading day(s) to process (resume-from={tmpStartFrom:yyyy-MM-dd})");

        var tmpStatsBefore = await m_Client.GetCacheStatsAsync(
            new GetCacheStatsRequest(), cancellationToken: inCt);
        Log($"stats(before): {FormatStats(tmpStatsBefore)}");

        // Periodic progress reporter.
        using var tmpReporterCts = CancellationTokenSource.CreateLinkedTokenSource(inCt);
        var tmpReporter = Task.Run(() => ReporterLoopAsync(tmpDays.Count, tmpReporterCts.Token), tmpReporterCts.Token);

        for (int i = 0; i < tmpDays.Count; i++)
        {
            inCt.ThrowIfCancellationRequested();
            var tmpDate = tmpDays[i];
            await ProcessDayAsync(tmpDate, i + 1, tmpDays.Count, inCt);

            m_Cp.LastCompletedDate = tmpDate;
            m_Cp.TotalDaysFetched++;
            await m_Cp.SaveAsync(m_Opts.CheckpointFile, inCt);
        }

        tmpReporterCts.Cancel();
        try { await tmpReporter; } catch (OperationCanceledException) { /* expected */ }

        // ── 3. Final report.
        var tmpStatsAfter = await m_Client.GetCacheStatsAsync(
            new GetCacheStatsRequest(), cancellationToken: inCt);
        m_RunSw.Stop();

        Log("==================== final report ====================");
        Log($"symbol               : {m_Opts.Symbol}");
        Log($"window               : {m_Opts.From:yyyy-MM-dd} .. {m_Opts.To:yyyy-MM-dd}");
        Log($"trading days         : {tmpTotalDays} (newly processed)");
        Log($"total days fetched   : {m_Cp.TotalDaysFetched} (incl. resume history)");
        Log($"nbbo calls this run  : {m_NbboCallsTotal:N0} ({m_NbboFailuresTotal:N0} failures)");
        Log($"wall clock           : {FormatHms(m_RunSw.Elapsed)}");
        Log($"avg rate             : {(m_NbboCallsTotal / Math.Max(1.0, m_RunSw.Elapsed.TotalSeconds)):F1} calls/s");
        Log($"stats(after)         : {FormatStats(tmpStatsAfter)}");
        Log($"checkpoint           : {Path.GetFullPath(m_Opts.CheckpointFile)}");
    }

    private async Task WarmupRangeAsync(CancellationToken inCt)
    {
        var tmpReq = new EnsureRangeCachedRequest
        {
            DataClasses = { DataClass.Bars, DataClass.Chains, DataClass.Macro },
            FromTs = Timestamp.FromDateTime(DateTime.SpecifyKind(
                m_Opts.From.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc)),
            ToTs = Timestamp.FromDateTime(DateTime.SpecifyKind(
                m_Opts.To.ToDateTime(TimeOnly.MaxValue), DateTimeKind.Utc)),
            BarTimeframe = BarTimeframe.Minute,
        };
        tmpReq.Symbols.Add(m_Opts.Symbol);

        using var tmpCall = m_Client.EnsureRangeCached(tmpReq, cancellationToken: inCt);
        await foreach (var tmpProgress in tmpCall.ResponseStream.ReadAllAsync(inCt))
        {
            Log($"warmup[{tmpProgress.DataClass}] status={tmpProgress.Status} " +
                $"complete={tmpProgress.KeysComplete}/{tmpProgress.KeysTotal} " +
                $"missing={tmpProgress.KeysMissing} upstream={tmpProgress.UpstreamCalls} " +
                $"({tmpProgress.ElapsedMs}ms) {tmpProgress.Message}");
            if (tmpProgress.Status == WarmupStatus.Failed)
            {
                throw new InvalidOperationException(
                    $"warmup failed for {tmpProgress.DataClass}: {tmpProgress.Message}");
            }
        }
    }

    private async Task ProcessDayAsync(DateOnly inDate, int inDayIndex, int inDayCount, CancellationToken inCt)
    {
        var tmpDaySw = Stopwatch.StartNew();
        m_NbboCallsThisDay = 0;

        // ── 1. Fetch the day's chain.
        var tmpAsOf = DateTime.SpecifyKind(inDate.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc);
        var tmpChainResp = await m_Client.GetOptionChainAsync(
            new GetOptionChainRequest
            {
                UnderlyingTicker = m_Opts.Symbol,
                AsOfDate = Timestamp.FromDateTime(tmpAsOf),
            }, cancellationToken: inCt);
        Log($"day {inDate:yyyy-MM-dd} ({inDayIndex}/{inDayCount}): chain has {tmpChainResp.Contracts.Count} contracts");

        if (tmpChainResp.Contracts.Count == 0)
        {
            Log($"day {inDate:yyyy-MM-dd}: skipping — empty chain (likely missing data day)");
            return;
        }

        // ── 2. Get TSLA close on this day to determine ATM strike.
        var tmpClose = await GetUnderlyingCloseAsync(inDate, inCt);
        if (tmpClose is null or <= 0)
        {
            Log($"day {inDate:yyyy-MM-dd}: skipping — no underlying close (likely missing bars)");
            return;
        }
        var tmpAtm = tmpClose.Value;
        var tmpStrikeMin = tmpAtm * (1 - m_Opts.StrikeBandPct);
        var tmpStrikeMax = tmpAtm * (1 + m_Opts.StrikeBandPct);

        // ── 3. Filter contracts: strike band + DTE ≤ 10.
        var tmpFiltered = tmpChainResp.Contracts
            .Where(c =>
            {
                if (c.StrikePrice < tmpStrikeMin || c.StrikePrice > tmpStrikeMax) return false;
                var tmpExp = c.ExpirationDate.ToDateTime();
                var tmpDte = (DateOnly.FromDateTime(tmpExp).DayNumber - inDate.DayNumber);
                return tmpDte >= 0 && tmpDte <= m_Opts.DteMaxDays;
            })
            .ToList();

        Log($"day {inDate:yyyy-MM-dd}: filtered to {tmpFiltered.Count} contracts " +
            $"(close=${tmpAtm:F2}, strike band ${tmpStrikeMin:F2}..${tmpStrikeMax:F2}, dte≤{m_Opts.DteMaxDays})");

        if (tmpFiltered.Count == 0) return;

        // ── 4. Build the (contract, minute) work list. RTH = 9:30 ET → 16:00
        //      ET = 13:30 UTC → 20:00 UTC during EST (Nov-Mar) and 13:30 UTC
        //      → 20:00 UTC during EDT — wait, EST is UTC-5 / EDT is UTC-4,
        //      so 9:30 ET → 14:30 UTC (winter) / 13:30 UTC (summer). Use the
        //      official market_close - 6.5h convention. 390 minutes either
        //      way.
        var tmpRthMinutes = EnumerateRthMinutes(inDate, halfDay: TradingCalendar.IsHalfDay(inDate)).ToList();
        var tmpTotalCalls = tmpFiltered.Count * tmpRthMinutes.Count;
        Log($"day {inDate:yyyy-MM-dd}: {tmpRthMinutes.Count} RTH minute(s) × " +
            $"{tmpFiltered.Count} contracts = {tmpTotalCalls:N0} NBBO calls (concurrency={m_Opts.Concurrency})");

        // ── 5. Drive concurrency-bounded NBBO fetch.
        var tmpSem = new SemaphoreSlim(m_Opts.Concurrency, m_Opts.Concurrency);
        var tmpTasks = new List<Task>(tmpTotalCalls);

        foreach (var tmpMinute in tmpRthMinutes)
        {
            foreach (var tmpContract in tmpFiltered)
            {
                inCt.ThrowIfCancellationRequested();
                await tmpSem.WaitAsync(inCt);
                var tmpTicker = tmpContract.Ticker;
                var tmpTs = tmpMinute;
                tmpTasks.Add(Task.Run(async () =>
                {
                    try { await FetchNbboWithRetryAsync(tmpTicker, tmpTs, inCt); }
                    finally { tmpSem.Release(); }
                }, inCt));
            }
        }
        await Task.WhenAll(tmpTasks);

        tmpDaySw.Stop();
        var tmpRate = m_NbboCallsThisDay / Math.Max(1.0, tmpDaySw.Elapsed.TotalSeconds);
        var tmpRemaining = inDayCount - inDayIndex;
        var tmpEta = TimeSpan.FromSeconds(tmpDaySw.Elapsed.TotalSeconds * tmpRemaining);
        m_Cp.TotalKeysFetched += m_NbboCallsThisDay;
        Log($"day {inDate:yyyy-MM-dd}: done in {FormatHms(tmpDaySw.Elapsed)} " +
            $"({m_NbboCallsThisDay:N0} calls, {tmpRate:F1}/s); ~ETA {FormatHms(tmpEta)} remaining");
    }

    private async Task<double?> GetUnderlyingCloseAsync(DateOnly inDate, CancellationToken inCt)
    {
        // Daily close = the last minute bar before 16:00 ET. The bars
        // path warms minute bars at the start; here we just ask for the
        // 15:59 ET → 16:00 ET window and take the close of the final
        // populated bar to handle half-days transparently.
        var tmpFromUtc = inDate.ToDateTime(new TimeOnly(13, 30, 0));
        var tmpToUtc = inDate.ToDateTime(new TimeOnly(21, 0, 0));
        var tmpResp = await m_Client.GetBarsAsync(new GetBarsRequest
        {
            Symbol = m_Opts.Symbol,
            Timeframe = BarTimeframe.Minute,
            FromTs = Timestamp.FromDateTime(DateTime.SpecifyKind(tmpFromUtc, DateTimeKind.Utc)),
            ToTs = Timestamp.FromDateTime(DateTime.SpecifyKind(tmpToUtc, DateTimeKind.Utc)),
        }, cancellationToken: inCt);
        return tmpResp.Bars.Count == 0 ? null : tmpResp.Bars[^1].Close;
    }

    /// <summary>
    /// Yield every minute timestamp in RTH (9:30 ET → 16:00 ET). Half-days
    /// truncate at 13:00 ET.
    /// </summary>
    /// <remarks>
    /// US/Eastern is DST-sensitive. .NET's TimeZoneInfo is the most
    /// reliable way to convert; fall back to "Eastern Standard Time" on
    /// Windows and "America/New_York" on Linux via the cross-platform
    /// IANA name resolver.
    /// </remarks>
    private static IEnumerable<Timestamp> EnumerateRthMinutes(DateOnly inDate, bool halfDay)
    {
        var tmpEt = ResolveEasternTz();
        var tmpOpenLocal = inDate.ToDateTime(new TimeOnly(9, 30, 0));
        var tmpCloseLocal = inDate.ToDateTime(halfDay ? new TimeOnly(13, 0, 0) : new TimeOnly(16, 0, 0));
        var tmpOpenUtc = TimeZoneInfo.ConvertTimeToUtc(tmpOpenLocal, tmpEt);
        var tmpCloseUtc = TimeZoneInfo.ConvertTimeToUtc(tmpCloseLocal, tmpEt);

        for (var tmpTs = tmpOpenUtc; tmpTs < tmpCloseUtc; tmpTs = tmpTs.AddMinutes(1))
        {
            yield return Timestamp.FromDateTime(DateTime.SpecifyKind(tmpTs, DateTimeKind.Utc));
        }
    }

    private static TimeZoneInfo ResolveEasternTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
    }

    private async Task FetchNbboWithRetryAsync(string inTicker, Timestamp inTs, CancellationToken inCt)
    {
        for (int tmpAttempt = 1; tmpAttempt <= 2; tmpAttempt++)
        {
            try
            {
                _ = await m_Client.GetNbboAsync(new GetNbboRequest
                {
                    Ticker = inTicker,
                    Ts = inTs,
                }, cancellationToken: inCt);
                Interlocked.Increment(ref m_NbboCallsThisDay);
                Interlocked.Increment(ref m_NbboCallsTotal);
                return;
            }
            catch (RpcException ex) when (tmpAttempt == 1 && ex.StatusCode != StatusCode.Cancelled)
            {
                await Task.Delay(500, inCt);
            }
            catch (Exception ex) when (tmpAttempt == 1 && ex is not OperationCanceledException)
            {
                await Task.Delay(500, inCt);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref m_NbboFailuresTotal);
                Log($"WARN nbbo fail {inTicker}@{inTs.ToDateTime():O} after retry: {ex.GetType().Name}: {ex.Message}");
                return;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Periodic progress reporter — prints once a minute to keep the
    // operator's console alive during long stretches.
    // ─────────────────────────────────────────────────────────────────
    private async Task ReporterLoopAsync(int inDayCount, CancellationToken inCt)
    {
        while (!inCt.IsCancellationRequested)
        {
            try { await Task.Delay(TimeSpan.FromSeconds(60), inCt); }
            catch (OperationCanceledException) { return; }

            var tmpDone = m_Cp.TotalDaysFetched;
            var tmpRate = m_NbboCallsTotal / Math.Max(1.0, m_RunSw.Elapsed.TotalSeconds);
            var tmpEtaSeconds = tmpRate <= 0 ? 0 : (inDayCount - tmpDone) * (m_RunSw.Elapsed.TotalSeconds / Math.Max(1, tmpDone));
            Log($"[heartbeat] day={tmpDone}/{inDayCount} calls={m_NbboCallsTotal:N0} " +
                $"rate={tmpRate:F1}/s eta={FormatHms(TimeSpan.FromSeconds(tmpEtaSeconds))}");
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Formatting helpers.
    // ─────────────────────────────────────────────────────────────────
    private static string FormatStats(GetCacheStatsResponse inResp)
    {
        var tmpParts = inResp.ClassStats
            .OrderBy(c => c.DataClass.ToString(), StringComparer.Ordinal)
            .Select(c => $"{c.DataClass}(req={c.TotalRequests} hit={c.CacheHits} up={c.UpstreamFetches} miss={c.MissMarkers} p50={c.LatencyP50Ms:F1}ms p95={c.LatencyP95Ms:F1}ms)");
        return string.Join(", ", tmpParts);
    }

    private static string FormatHms(TimeSpan inTs)
        => $"{(int)inTs.TotalHours}h {inTs.Minutes:D2}m {inTs.Seconds:D2}s";

    private void Log(string inMsg)
    {
        var tmpLine = $"[{DateTime.UtcNow:HH:mm:ss}] {inMsg}";
        Console.WriteLine(tmpLine);
        m_LogWriter?.WriteLine(tmpLine);
        m_LogWriter?.Flush();
    }
}
