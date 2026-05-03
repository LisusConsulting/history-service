namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// Surface the seeder targets. <see cref="Bars"/> is the original mode
/// (chains + bars + macro warmup, then minute NBBO per (contract, RTH-min)
/// — driven entirely through gRPC). <see cref="DailyOptionsFlow"/> is
/// PR 2's new mode: per-(symbol, day) aggregate of Polygon /v2/aggs daily
/// volume across short-DTE contracts, written through a direct DB
/// connection to the new <c>daily_options_flow</c> table.
/// </summary>
public enum Surface
{
    /// <summary>Original surface: bars + chains + macro warmup, minute NBBO per RTH minute.</summary>
    Bars = 0,
    /// <summary>PR 2 backfill: aggregate per-contract daily volume into the <c>daily_options_flow</c> hypertable.</summary>
    DailyOptionsFlow = 1,
}

/// <summary>
/// Strongly-typed seed-driver invocation. Built from CLI args in <see cref="Program"/>.
/// </summary>
public sealed class SeedOptions
{
    /// <summary>Which surface the run targets. Default: <see cref="Surface.Bars"/> (original behaviour).</summary>
    public Surface Surface { get; init; } = Surface.Bars;

    public string Symbol { get; init; } = "TSLA";
    public DateOnly From { get; init; }
    public DateOnly To { get; init; }
    public int Concurrency { get; init; } = 32;
    public string CheckpointFile { get; init; } = "./checkpoint.json";
    public string HistoryGrpcHost { get; init; } = "localhost";
    public int HistoryGrpcPort { get; init; } = 30005;
    public string? LogFile { get; init; }

    /// <summary>Strike band as a fraction of underlying close. 0.05 = ATM ± 5%.</summary>
    public double StrikeBandPct { get; init; } = 0.05;

    /// <summary>Maximum days-to-expiry from the as-of date. Default 10.</summary>
    public int DteMaxDays { get; init; } = 10;

    // ── DailyOptionsFlow surface ──────────────────────────────────────

    /// <summary>
    /// Direct Postgres connection string the daily-flow seeder writes
    /// through. Required when <see cref="Surface"/> = <see cref="Surface.DailyOptionsFlow"/>;
    /// the daily-flow writes originate from the seeder rather than from
    /// the gRPC service, so they bypass the gRPC layer entirely. Falls
    /// back to the <c>HISTORY__CONNECTIONSTRING</c> env var when omitted.
    /// </summary>
    public string? PostgresConn { get; init; }

    /// <summary>
    /// Maximum days-to-expiry for the daily-flow surface's contract
    /// universe (DTE 0..MaxDte). Default 60 — matches MBD's
    /// <c>OptionsAnalysisService.MAX_DTE</c> and the legacy
    /// <c>OptionsVolumeBackfillService</c> aggregator window. Distinct
    /// from <see cref="DteMaxDays"/> (which is the bars-surface ATM-NBBO
    /// window, default 10).
    /// </summary>
    public int FlowMaxDte { get; init; } = 60;
}
