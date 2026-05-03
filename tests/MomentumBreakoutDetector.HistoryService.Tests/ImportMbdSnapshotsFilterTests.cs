using MomentumBreakoutDetector.HistoryService.ImportMbdSnapshots;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Wave C / PR 9 — pin the bootstrap-import filter predicate
/// (ATM ± 5% × DTE [0, 60]). The filter is the only piece of the
/// import that runs in-process for every source row, so a regression
/// is high-impact (drops valid rows, or admits out-of-band rows that
/// pollute the destination band).
/// </summary>
public class ImportMbdSnapshotsFilterTests
{
    /// <summary>
    /// Sentinel: caller passed an explicit override for ExpirationDate
    /// (including null). We use a sentinel because the params are nullable
    /// — `inExp = null` is otherwise indistinguishable from "use default."
    /// </summary>
    private static readonly DateTime DefaultExp = new DateTime(2024, 7, 5, 0, 0, 0, DateTimeKind.Utc);

    private static SourceSnapshotRow Row(
        decimal? inUnderlying = 100m,
        decimal? inStrike = 100m,
        DateTime? inExp = null,
        bool inExpOverride = false,
        DateTime? inSnapshot = null)
        => new SourceSnapshotRow(
            Ticker: "O:TSLA240705C00100000",
            SnapshotDate: inSnapshot ?? new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc),
            UnderlyingTicker: "TSLA",
            ContractType: "call",
            StrikePrice: inStrike,
            ExpirationDate: inExpOverride ? inExp : (inExp ?? DefaultExp),
            BidPrice: 1m, AskPrice: 1.1m,
            Volume: 100, OpenInterest: 50,
            ImpliedVolatility: 0.4m,
            Delta: 0.5m, Gamma: 0.01m, Theta: -0.05m, Vega: 0.2m,
            UnderlyingPrice: inUnderlying);

    [Fact]
    public void Keeps_AtmExactlyOnBoundary()
    {
        // |105 − 100| / 100 = 5% exactly → keep (band is inclusive).
        ImportRunner.FilterPredicate(Row(inUnderlying: 100m, inStrike: 105m), 0.05m, 60).ShouldBeTrue();
    }

    [Fact]
    public void Drops_StrikeOutsideBand()
    {
        // |110 − 100| / 100 = 10% > 5% → drop.
        ImportRunner.FilterPredicate(Row(inUnderlying: 100m, inStrike: 110m), 0.05m, 60).ShouldBeFalse();
    }

    [Fact]
    public void Drops_NullUnderlyingPrice()
    {
        ImportRunner.FilterPredicate(Row(inUnderlying: null), 0.05m, 60).ShouldBeFalse();
    }

    [Fact]
    public void Drops_ZeroUnderlyingPrice()
    {
        ImportRunner.FilterPredicate(Row(inUnderlying: 0m), 0.05m, 60).ShouldBeFalse();
    }

    [Fact]
    public void Drops_NullStrike()
    {
        ImportRunner.FilterPredicate(Row(inStrike: null), 0.05m, 60).ShouldBeFalse();
    }

    [Fact]
    public void Drops_NullExpiration()
    {
        ImportRunner.FilterPredicate(Row(inExp: null, inExpOverride: true), 0.05m, 60).ShouldBeFalse();
    }

    [Fact]
    public void Drops_DteNegative()
    {
        // Snapshot 2024-06-03, expiration 2024-06-01 → DTE = −2 → drop.
        ImportRunner.FilterPredicate(
            Row(inSnapshot: new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc),
                inExp: new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc)),
            0.05m, 60).ShouldBeFalse();
    }

    [Fact]
    public void Drops_DteOverMax()
    {
        // 90 days out → DTE = 90, max = 60 → drop.
        ImportRunner.FilterPredicate(
            Row(inSnapshot: new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc),
                inExp: new DateTime(2024, 9, 1, 0, 0, 0, DateTimeKind.Utc)),
            0.05m, 60).ShouldBeFalse();
    }

    [Fact]
    public void Keeps_DteOnMaxBoundary()
    {
        // Snapshot 2024-06-03, expiration 2024-08-02 → DTE = 60 → keep.
        ImportRunner.FilterPredicate(
            Row(inSnapshot: new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc),
                inExp: new DateTime(2024, 8, 2, 0, 0, 0, DateTimeKind.Utc)),
            0.05m, 60).ShouldBeTrue();
    }

    [Fact]
    public void Keeps_DteZeroSameDay()
    {
        // 0 DTE keep — same-day expiry contracts are valid for the
        // bootstrap (forward-going cron also captures them).
        ImportRunner.FilterPredicate(
            Row(inSnapshot: new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc),
                inExp: new DateTime(2024, 6, 3, 0, 0, 0, DateTimeKind.Utc)),
            0.05m, 60).ShouldBeTrue();
    }

    [Fact]
    public void RespectsCustomBandWider()
    {
        // 10% band — strike 110 with underlying 100 = 10% → exactly at
        // the new boundary → keep.
        ImportRunner.FilterPredicate(Row(inUnderlying: 100m, inStrike: 110m), 0.10m, 60).ShouldBeTrue();
    }
}
