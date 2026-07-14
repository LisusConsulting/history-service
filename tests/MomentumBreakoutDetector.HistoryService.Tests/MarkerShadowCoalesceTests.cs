using MomentumBreakoutDetector.HistoryService.Providers;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// 2026-07-14: pins the marker-shadow coalesce + binary-search that replaced
/// the O(expected × markers) nested scan in
/// <see cref="HistoricalBarsProvider.EnsureRangeCachedAsync"/> (the
/// full-history-warmup jam that starved chart reads: MU/PLTR/NVDA each held
/// ~0.6–0.9M fragmented 1-min no-data markers, and 0.9M expected × 0.9M
/// markers ≈ 8×10^11 comparisons per warmup). Pure math — no Postgres.
/// </summary>
public sealed class MarkerShadowCoalesceTests
{
    private static HistoricalBarsProvider.MissRow M(DateTime inFrom, DateTime inTo)
        => new(inFrom, inTo);

    private static DateTime U(int inH, int inM)
        => new(2022, 10, 19, inH, inM, 0, DateTimeKind.Utc);

    [Fact]
    public void Coalesce_MergesOverlappingAdjacentAndFragmented()
    {
        // Deliberately unsorted + fragmented + overlapping.
        var tmpMarkers = new[]
        {
            M(U(14, 3), U(14, 3)),   // single minute
            M(U(14, 4), U(14, 4)),   // adjacent to above
            M(U(14, 2), U(14, 5)),   // overlaps + extends both
            M(U(15, 0), U(15, 10)),  // separate range
        };

        var tmpIntervals = HistoricalBarsProvider.CoalesceMarkerIntervals(tmpMarkers);

        tmpIntervals.Count.ShouldBe(2);
        tmpIntervals[0].From.ShouldBe(U(14, 2));
        tmpIntervals[0].To.ShouldBe(U(14, 5));
        tmpIntervals[1].From.ShouldBe(U(15, 0));
        tmpIntervals[1].To.ShouldBe(U(15, 10));
    }

    [Fact]
    public void Coalesce_Empty_ReturnsEmpty()
        => HistoricalBarsProvider
            .CoalesceMarkerIntervals(Array.Empty<HistoricalBarsProvider.MissRow>())
            .ShouldBeEmpty();

    [Fact]
    public void IsCovered_Boundaries_Inside_And_Gaps()
    {
        var tmpIv = HistoricalBarsProvider.CoalesceMarkerIntervals(new[]
        {
            M(U(14, 2), U(14, 5)),
            M(U(15, 0), U(15, 10)),
        });

        HistoricalBarsProvider.IsCoveredByIntervals(tmpIv, U(14, 2)).ShouldBeTrue();  // lower bound
        HistoricalBarsProvider.IsCoveredByIntervals(tmpIv, U(14, 5)).ShouldBeTrue();  // upper bound
        HistoricalBarsProvider.IsCoveredByIntervals(tmpIv, U(14, 3)).ShouldBeTrue();  // inside
        HistoricalBarsProvider.IsCoveredByIntervals(tmpIv, U(15, 10)).ShouldBeTrue(); // 2nd upper bound
        HistoricalBarsProvider.IsCoveredByIntervals(tmpIv, U(14, 1)).ShouldBeFalse(); // before all
        HistoricalBarsProvider.IsCoveredByIntervals(tmpIv, U(14, 6)).ShouldBeFalse(); // in the gap
        HistoricalBarsProvider.IsCoveredByIntervals(tmpIv, U(15, 11)).ShouldBeFalse();// after all
    }

    [Fact]
    public void Equivalence_CoalesceBinarySearch_MatchesNaiveScan()
    {
        // Property check across a fragmented + overlapping marker set: the new
        // coalesce+binary-search yields the SAME covered-set as the old
        // per-ts × per-marker linear scan it replaced (byte-identical result,
        // just O(log n) not O(n)).
        var tmpRnd = new Random(20260714);
        var tmpBase = new DateTime(2022, 10, 19, 4, 0, 0, DateTimeKind.Utc);
        var tmpMarkers = new List<HistoricalBarsProvider.MissRow>();
        for (var i = 0; i < 600; i++)
        {
            var tmpStart = tmpRnd.Next(0, 960);
            var tmpLen = tmpRnd.Next(0, 5);
            tmpMarkers.Add(M(tmpBase.AddMinutes(tmpStart), tmpBase.AddMinutes(tmpStart + tmpLen)));
        }

        var tmpIntervals = HistoricalBarsProvider.CoalesceMarkerIntervals(tmpMarkers);

        for (var m = 0; m <= 960; m++)
        {
            var tmpTs = tmpBase.AddMinutes(m);
            var tmpNaive = tmpMarkers.Any(x => tmpTs >= x.RangeFrom && tmpTs <= x.RangeTo);
            var tmpFast = HistoricalBarsProvider.IsCoveredByIntervals(tmpIntervals, tmpTs);
            tmpFast.ShouldBe(tmpNaive, $"coverage mismatch at minute offset {m}");
        }
    }
}
