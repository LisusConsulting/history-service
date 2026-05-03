using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Providers;
using TradingCalendar = MomentumBreakoutDetector.HistoryService.Domain.TradingCalendar;

namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// Wave C / PR 6 — backfill engine for the <c>daily_atm_iv</c> aggregate
/// table. Drives <see cref="DailyAtmIvAggregator"/> across a date window
/// and persists via <see cref="DailyAtmIvProvider"/> (UPSERT keyed on
/// (underlying, trade_date), idempotent).
///
/// <para>
/// <b>Used after</b> the BS-snapshot backfill (PR 3) populates
/// <c>historical_options_snapshots</c> with <c>source='computed_bs'</c>
/// rows for the historical range. Running this surface afterwards
/// rolls the per-day aggregate forward into <c>daily_atm_iv</c> so the
/// MBD backtest engine sees full-history coverage.
/// </para>
///
/// <para>
/// <b>Direct-DB write.</b> Bypasses gRPC. Same shape as the other
/// daily/snapshot seeder engines: read from the local Postgres, compute,
/// write back via the provider's UPSERT. The seeder process needs the
/// same connection string env var (<c>HISTORY__CONNECTIONSTRING</c>) the
/// other surfaces use.
/// </para>
///
/// <para>
/// <b>Idempotent.</b> Resume from checkpoint walks day-by-day; the
/// per-day aggregate is deterministic from the snapshot table's contents
/// at read time, and the UPSERT collapses re-writes. Re-running the
/// seeder over the same window produces the same daily_atm_iv table
/// state (with updated <c>fetched_at</c> stamps).
/// </para>
/// </summary>
public sealed class DailyAtmIvSeederEngine
{
    private readonly SeedOptions m_Opts;
    private readonly Checkpoint m_Cp;
    private readonly StreamWriter? m_LogWriter;
    private readonly IDailyAtmIvAggregator m_Aggregator;
    private readonly IDailyAtmIvProvider m_Provider;

    private long m_DaysWithRowsTotal;
    private long m_DaysWithMissTotal;
    private readonly Stopwatch m_RunSw = new();

    public DailyAtmIvSeederEngine(
        SeedOptions inOpts,
        Checkpoint inCp,
        IDailyAtmIvAggregator inAggregator,
        IDailyAtmIvProvider inProvider,
        StreamWriter? inLogWriter)
    {
        m_Opts = inOpts;
        m_Cp = inCp;
        m_Aggregator = inAggregator;
        m_Provider = inProvider;
        m_LogWriter = inLogWriter;
    }

    /// <summary>Public factory that wires the Postgres-backed aggregator
    /// + provider against the supplied connection string. Used by the
    /// seeder Program when launching the daily_atm_iv surface.</summary>
    public static DailyAtmIvSeederEngine Create(
        SeedOptions inOpts, Checkpoint inCp, string inPostgresConn, StreamWriter? inLogWriter)
    {
        var tmpAggregator = new DailyAtmIvAggregator(
            inPostgresConn, NullLogger<DailyAtmIvAggregator>.Instance);
        var tmpProvider = new DailyAtmIvProvider(
            inPostgresConn, NullLogger<DailyAtmIvProvider>.Instance);
        return new DailyAtmIvSeederEngine(inOpts, inCp, tmpAggregator, tmpProvider, inLogWriter);
    }

    public async Task RunAsync(CancellationToken inCt)
    {
        m_RunSw.Start();

        var tmpStartFrom = m_Cp.LastCompletedDate is { } lc ? lc.AddDays(1) : m_Opts.From;
        var tmpDays = TradingCalendar.EnumerateTradingDays(tmpStartFrom, m_Opts.To).ToList();
        Log($"plan: surface=daily_atm_iv {tmpDays.Count} trading day(s) " +
            $"(resume-from={tmpStartFrom:yyyy-MM-dd}) symbol={m_Opts.Symbol}");

        for (var i = 0; i < tmpDays.Count; i++)
        {
            inCt.ThrowIfCancellationRequested();
            var tmpDate = tmpDays[i];
            await ProcessDayAsync(tmpDate, i + 1, tmpDays.Count, inCt).ConfigureAwait(false);

            m_Cp.LastCompletedDate = tmpDate;
            m_Cp.TotalDaysFetched++;
            await m_Cp.SaveAsync(m_Opts.CheckpointFile, inCt).ConfigureAwait(false);
        }

        m_RunSw.Stop();
        Log("==================== final report (daily_atm_iv) ====================");
        Log($"symbol               : {m_Opts.Symbol}");
        Log($"window               : {m_Opts.From:yyyy-MM-dd} .. {m_Opts.To:yyyy-MM-dd}");
        Log($"trading days         : {tmpDays.Count} (newly processed)");
        Log($"days with rows       : {m_DaysWithRowsTotal}");
        Log($"days with miss-marker: {m_DaysWithMissTotal}");
        Log($"wall clock           : {FormatHms(m_RunSw.Elapsed)}");
        Log($"checkpoint           : {Path.GetFullPath(m_Opts.CheckpointFile)}");
    }

    private async Task ProcessDayAsync(DateOnly inDate, int inIndex, int inTotal, CancellationToken inCt)
    {
        var tmpDaySw = Stopwatch.StartNew();
        var tmpRow = await m_Aggregator.AggregateAsync(m_Opts.Symbol, inDate, inCt).ConfigureAwait(false);

        if (tmpRow is null)
        {
            await m_Provider.RecordMissAsync(
                m_Opts.Symbol, inDate, inDate, "no-snapshot-rows", inCt).ConfigureAwait(false);
            Interlocked.Increment(ref m_DaysWithMissTotal);
            tmpDaySw.Stop();
            Log($"day {inDate:yyyy-MM-dd} ({inIndex}/{inTotal}): MISS (no ATM-band snapshots) ({FormatHms(tmpDaySw.Elapsed)})");
            return;
        }

        await m_Provider.UpsertAsync(new[] { tmpRow }, inCt).ConfigureAwait(false);
        Interlocked.Increment(ref m_DaysWithRowsTotal);
        tmpDaySw.Stop();
        Log($"day {inDate:yyyy-MM-dd} ({inIndex}/{inTotal}): atm_iv={tmpRow.AtmIv:F4} " +
            $"contracts={tmpRow.ContractCount} ({FormatHms(tmpDaySw.Elapsed)})");
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
