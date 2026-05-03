namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// Surface the seeder targets. <see cref="Bars"/> is the original mode
/// (chains + bars + macro warmup, then minute NBBO per (contract, RTH-min)
/// — driven entirely through gRPC). <see cref="DailyOptionsFlow"/> is
/// PR 2's mode: per-(symbol, day) aggregate of Polygon /v2/aggs daily
/// volume across short-DTE contracts, written through a direct DB
/// connection to the new <c>daily_options_flow</c> table.
/// <see cref="OptionsSnapshots"/> is Wave B / PR 3 of the ATM-IV plan:
/// per-(contract, EOD-trade-date) Black-Scholes IV + greeks rows for
/// the ATM-band contract universe, written through a direct DB
/// connection to <c>historical_options_snapshots</c>.
/// </summary>
public enum Surface
{
    /// <summary>Original surface: bars + chains + macro warmup, minute NBBO per RTH minute.</summary>
    Bars = 0,
    /// <summary>PR 2 backfill: aggregate per-contract daily volume into the <c>daily_options_flow</c> hypertable.</summary>
    DailyOptionsFlow = 1,
    /// <summary>Wave B / PR 3 backfill (ATM-IV plan): per-contract EOD BS-computed snapshots
    /// into the <c>historical_options_snapshots</c> hypertable. Compute method governed by
    /// <see cref="SeedOptions.ComputeMethod"/>.</summary>
    OptionsSnapshots = 2,
}

/// <summary>
/// Compute method for the <see cref="Surface.OptionsSnapshots"/> seeder
/// surface. Only <see cref="Bs"/> ships in PR 3; <see cref="Polygon"/> is
/// reserved for a later refactor that consolidates the live-capture cron
/// + a one-shot polygon-snapshot import under the same driver.
/// </summary>
public enum SnapshotComputeMethod
{
    /// <summary>Black-Scholes solver (Wave A / PR 2). Inputs: NBBO mid + bars close + DGS3MO macro.</summary>
    Bs = 0,
    /// <summary>Polygon /v3/snapshot/options/{underlying} pull. Reserved — not implemented in PR 3.</summary>
    Polygon = 1,
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

    // ── OptionsSnapshots surface (Wave B / PR 3 of the ATM-IV plan) ──

    /// <summary>
    /// Compute method for the <see cref="Surface.OptionsSnapshots"/>
    /// surface. Default <see cref="SnapshotComputeMethod.Bs"/> — only
    /// method implemented in PR 3.
    /// </summary>
    public SnapshotComputeMethod ComputeMethod { get; init; } = SnapshotComputeMethod.Bs;

    /// <summary>
    /// Maximum days-to-expiry for the OptionsSnapshots surface's contract
    /// universe (DTE 0..MaxDte). Default 60 — matches the live-capture
    /// cron (PR 4) so historical and forward-going snapshots cover the
    /// same DTE band. Distinct from <see cref="FlowMaxDte"/> (60) and
    /// <see cref="DteMaxDays"/> (10).
    /// </summary>
    public int SnapshotDteMaxDays { get; init; } = 60;
}
