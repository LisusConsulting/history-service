using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Intra-range gap-detection tests for <see cref="HistoricalBarsProvider.EnsureRangeCachedAsync"/>
/// — the rework that swapped cache-edge gap detection for
/// expected-minus-cached-minus-marked detection (Lisus brief 2026-05-02).
///
/// <para>
/// The old logic could not see gaps INSIDE the cached range — a 30-minute
/// halt mid-session was invisible because cache-MIN/MAX still spanned the
/// day. The new pass enumerates the calendar's expected timestamps and
/// diffs against cached + marked. Markers are now range-coalesced on
/// write so a whole missing afternoon collapses to ONE row, not 195.
/// </para>
///
/// <para>
/// All tests use Wednesday 2026-04-15, a known full trading day in the
/// calendar (no half-day, no holiday). Window: 14:00..14:29 UTC = 10:00
/// ET..10:29 ET = 30 RTH minutes. Small enough to enumerate by hand;
/// large enough to exercise gap-coalesce logic.
/// </para>
/// </summary>
public sealed class BarsIntraRangeGapDetectionTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer m_Pg = new PostgreSqlBuilder()
        .WithImage("timescale/timescaledb:latest-pg16")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    private string m_ConnStr = null!;

    // 30-minute RTH window mid-session.
    private static readonly DateTime s_From = new(2026, 4, 15, 14, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime s_To = new(2026, 4, 15, 14, 29, 0, DateTimeKind.Utc);

    public async Task InitializeAsync()
    {
        await m_Pg.StartAsync();
        m_ConnStr = m_Pg.GetConnectionString();
        await ApplySchemaAsync(m_ConnStr);
    }

    public Task DisposeAsync() => m_Pg.DisposeAsync().AsTask();

    // ── 1. Empty cache + empty markers → fetches whole range ────────────

    [Fact]
    public async Task EmptyCacheAndMarkers_FetchesWholeRange()
    {
        var tmpStub = new RecordingFetcher(returnEmpty: false, fillBars: 30);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        var tmpUpstream = await tmpProvider.EnsureRangeCachedAsync(
            "TSLA", s_From, s_To, BarTimeframe.OneMinute);

        tmpUpstream.ShouldBe(1, "single chunk for a 30-minute range");
        tmpStub.RecordedChunks.Count.ShouldBe(1);
        tmpStub.RecordedChunks[0].From.ShouldBe(s_From);
        tmpStub.RecordedChunks[0].To.ShouldBe(s_To);

        // Cache should now contain 30 bars.
        var tmpCachedCount = await CountBarsAsync("1min");
        tmpCachedCount.ShouldBe(30L);
    }

    // ── 2. Partial cache → fetches only missing slots ───────────────────

    [Fact]
    public async Task PartialCache_FetchesOnlyMissingMinutes()
    {
        // Pre-seed 20 of the 30 minutes — drop 14:10..14:19 (10 minutes
        // mid-window). Expected behaviour: gap detector identifies the
        // 10-minute missing range, fetches it.
        await SeedBarsAsync(s_From, s_To, gapStart: s_From.AddMinutes(10), gapEnd: s_From.AddMinutes(19));

        var tmpStub = new RecordingFetcher(returnEmpty: false, fillBars: 10);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        var tmpUpstream = await tmpProvider.EnsureRangeCachedAsync(
            "TSLA", s_From, s_To, BarTimeframe.OneMinute);

        tmpUpstream.ShouldBe(1, "1 contiguous gap → 1 fetch");
        tmpStub.RecordedChunks.Count.ShouldBe(1);
        // The fetcher should have been asked specifically for the gap window.
        tmpStub.RecordedChunks[0].From.ShouldBe(s_From.AddMinutes(10));
        tmpStub.RecordedChunks[0].To.ShouldBe(s_From.AddMinutes(19));
    }

    // ── 3. Range markers covering some gaps → those skipped ─────────────

    [Fact]
    public async Task ExistingMarkers_ShadowMissingMinutes()
    {
        // Cache: 0 bars. Marker covers 14:10..14:19 (10 minutes).
        // Expected: fetch the un-shadowed 20 minutes split into 2 ranges
        // (14:00..14:09 + 14:20..14:29).
        await InsertMarkerAsync("1min", s_From.AddMinutes(10), s_From.AddMinutes(19));

        var tmpStub = new RecordingFetcher(returnEmpty: false, fillBars: 10);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        var tmpUpstream = await tmpProvider.EnsureRangeCachedAsync(
            "TSLA", s_From, s_To, BarTimeframe.OneMinute);

        tmpUpstream.ShouldBe(2, "2 disjoint un-shadowed ranges → 2 fetches");
        tmpStub.RecordedChunks.Count.ShouldBe(2);
        // Sorted-by-From because the gap detector iterates expected ts
        // in order.
        tmpStub.RecordedChunks[0].From.ShouldBe(s_From);
        tmpStub.RecordedChunks[0].To.ShouldBe(s_From.AddMinutes(9));
        tmpStub.RecordedChunks[1].From.ShouldBe(s_From.AddMinutes(20));
        tmpStub.RecordedChunks[1].To.ShouldBe(s_To);
    }

    // ── 4. Truncate markers → re-fetches their ranges ───────────────────

    [Fact]
    public async Task TruncatingMarkers_TriggersReFetchOnNextCall()
    {
        var tmpStub = new RecordingFetcher(returnEmpty: true, fillBars: 0);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        // First call: empty upstream → 1 marker covering the range.
        await tmpProvider.EnsureRangeCachedAsync("TSLA", s_From, s_To, BarTimeframe.OneMinute);
        var tmpMarkerCountAfter1st = await CountMarkersAsync("1min");
        tmpMarkerCountAfter1st.ShouldBe(1L);

        // Second call: marker shadows everything → 0 fetches.
        tmpStub.RecordedChunks.Clear();
        await tmpProvider.EnsureRangeCachedAsync("TSLA", s_From, s_To, BarTimeframe.OneMinute);
        tmpStub.RecordedChunks.Count.ShouldBe(0,
            "marker should shadow the full range — no upstream call");

        // Truncate the markers → next call must re-fetch.
        await TruncateMarkersAsync();
        tmpStub.RecordedChunks.Clear();
        var tmpUpstream = await tmpProvider.EnsureRangeCachedAsync(
            "TSLA", s_From, s_To, BarTimeframe.OneMinute);
        tmpUpstream.ShouldBe(1, "after truncate, gap is back → 1 fetch");
        tmpStub.RecordedChunks.Count.ShouldBe(1);
    }

    // ── 5. Empty upstream over 30 minutes → 1 marker row, not 30 ───────

    [Fact]
    public async Task EmptyUpstream_30Minutes_WritesOneRangeMarker()
    {
        var tmpStub = new RecordingFetcher(returnEmpty: true, fillBars: 0);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        await tmpProvider.EnsureRangeCachedAsync(
            "TSLA", s_From, s_To, BarTimeframe.OneMinute);

        // Crucial assertion: ONE row, not 30.
        var tmpMarkerCount = await CountMarkersAsync("1min");
        tmpMarkerCount.ShouldBe(1L,
            "30 contiguous missing minutes must collapse into a SINGLE marker row");

        // The single row must span the full requested range.
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        var tmpRow = await tmpConn.QueryFirstAsync<(DateTime From, DateTime To)>(
            """
            SELECT range_from AS "From", range_to AS "To"
            FROM historical_bars_misses
            WHERE symbol = 'TSLA' AND timeframe = '1min'
            """);
        tmpRow.From.ShouldBe(s_From);
        tmpRow.To.ShouldBe(s_To);
    }

    // ── 6. Adjacent markers (prior + new run) → coalesced ───────────────

    [Fact]
    public async Task AdjacentMarkersAcrossRuns_AreCoalesced()
    {
        // Pre-seed a marker for 14:00..14:14 (15 min). Then run a new
        // ensure for 14:00..14:29 with empty upstream — the new "gap"
        // is 14:15..14:29 (15 min) which abuts the existing marker. The
        // RangeMarkerWriter should produce ONE row 14:00..14:29, not two.
        await InsertMarkerAsync("1min", s_From, s_From.AddMinutes(14));
        var tmpInitialCount = await CountMarkersAsync("1min");
        tmpInitialCount.ShouldBe(1L);

        var tmpStub = new RecordingFetcher(returnEmpty: true, fillBars: 0);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        await tmpProvider.EnsureRangeCachedAsync(
            "TSLA", s_From, s_To, BarTimeframe.OneMinute);

        var tmpFinalCount = await CountMarkersAsync("1min");
        tmpFinalCount.ShouldBe(1L,
            "new marker should merge with the existing adjacent marker");

        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        var tmpRow = await tmpConn.QueryFirstAsync<(DateTime From, DateTime To)>(
            """
            SELECT range_from AS "From", range_to AS "To"
            FROM historical_bars_misses
            WHERE symbol = 'TSLA' AND timeframe = '1min'
            """);
        tmpRow.From.ShouldBe(s_From);
        tmpRow.To.ShouldBe(s_To, "merged marker should span the full union");
    }

    // ── 7. Multiple disjoint gaps in one day → multiple markers ─────────

    [Fact]
    public async Task TwoDisjointGaps_BothEmpty_WriteTwoRangeMarkers()
    {
        // Cache covers everything except 14:05..14:09 (5 min) and 14:20..14:24 (5 min).
        // Both gaps' upstream fetches return empty → 2 distinct markers.
        await SeedBarsAsync(s_From, s_To,
            (s_From.AddMinutes(5), s_From.AddMinutes(9)),
            (s_From.AddMinutes(20), s_From.AddMinutes(24)));

        var tmpStub = new RecordingFetcher(returnEmpty: true, fillBars: 0);
        var tmpProvider = new HistoricalBarsProvider(
            m_ConnStr, tmpStub, NullLogger<HistoricalBarsProvider>.Instance);

        await tmpProvider.EnsureRangeCachedAsync(
            "TSLA", s_From, s_To, BarTimeframe.OneMinute);

        var tmpMarkerCount = await CountMarkersAsync("1min");
        tmpMarkerCount.ShouldBe(2L,
            "two disjoint missing ranges → two distinct marker rows");
    }

    // ── 8. Pure-function: ComputeExpectedTimestamps semantics ───────────

    [Fact]
    public void ComputeExpectedTimestamps_RthOnFullDay_Yields960Minutes()
    {
        // Apr 15 2026 is a full Wednesday → ExtendedHours = 960 minutes.
        // Request the entire day window [04:00 ET, 20:00 ET) = [08:00 UTC, 24:00 UTC).
        var tmpFrom = new DateTime(2026, 4, 15, 8, 0, 0, DateTimeKind.Utc);
        var tmpTo = new DateTime(2026, 4, 15, 23, 59, 0, DateTimeKind.Utc);
        var tmpExpected = HistoricalBarsProvider.ComputeExpectedTimestamps(
            tmpFrom, tmpTo, BarTimeframe.OneMinute);
        tmpExpected.Count.ShouldBe(960);
    }

    [Fact]
    public void ComputeExpectedTimestamps_OnSaturday_YieldsZero()
    {
        var tmpSat = new DateOnly(2026, 4, 18);
        TradingCalendar.IsTradingDay(tmpSat).ShouldBeFalse();
        var tmpExpected = HistoricalBarsProvider.ComputeExpectedTimestamps(
            new DateTime(2026, 4, 18, 13, 30, 0, DateTimeKind.Utc),
            new DateTime(2026, 4, 18, 20, 0, 0, DateTimeKind.Utc),
            BarTimeframe.OneMinute);
        tmpExpected.ShouldBeEmpty();
    }

    // ── 8b. ComputeExpectedTimestamps — daily bars are DST-aware ────────

    [Theory]
    // Spring-forward 2024: Mar 10 = last EST day → 05:00 UTC; Mar 11 =
    // first EDT day → 04:00 UTC. Mar 10 is a Sunday (non-trading), but
    // the helper's date math is independent of trading-day filter — so
    // we use Mar 8 (Friday EST) and Mar 11 (Monday EDT) for the boundary.
    // Note Mar 10 itself is a non-trading day so the expected list won't
    // include it; we cover it instead with the standalone
    // ComputeExpectedTimestamps_DailyBars_DstAware test below.
    [InlineData(2024, 3, 8, 5)]   // Fri EST
    [InlineData(2024, 3, 11, 4)]  // Mon EDT
    [InlineData(2024, 11, 1, 4)]  // Fri EDT (last EDT trading day before fall-back)
    [InlineData(2024, 11, 4, 5)]  // Mon EST (first EST trading day after fall-back)
    public void ComputeExpectedTimestamps_DailyBars_DstAware(int year, int month, int day, int expectedUtcHour)
    {
        // Window spanning the whole UTC day so the returned ts must
        // fall inside [from, to].
        var tmpFrom = new DateTime(year, month, day, 0, 0, 0, DateTimeKind.Utc);
        var tmpTo = new DateTime(year, month, day, 23, 59, 0, DateTimeKind.Utc);

        var tmpResult = HistoricalBarsProvider.ComputeExpectedTimestamps(
            tmpFrom, tmpTo, BarTimeframe.OneDay);

        tmpResult.Count.ShouldBe(1, $"{year}-{month:00}-{day:00} should yield exactly one daily bar timestamp");
        tmpResult[0].Hour.ShouldBe(expectedUtcHour,
            $"midnight ET on {year}-{month:00}-{day:00} should map to {expectedUtcHour:00}:00 UTC");
        tmpResult[0].Year.ShouldBe(year);
        tmpResult[0].Month.ShouldBe(month);
        tmpResult[0].Day.ShouldBe(day);
        tmpResult[0].Kind.ShouldBe(DateTimeKind.Utc);
    }

    [Fact]
    public void ComputeExpectedTimestamps_DailyBars_FridayAcrossDstBoundary_BothPresent()
    {
        // Regression for the empirically-confirmed Friday under-representation
        // bug. Pre-patch ComputeExpectedTimestamps yielded 04:00 UTC for
        // every Friday year-round; the cached EST Friday rows sit at
        // 05:00 UTC and were incorrectly flagged "missing", letting
        // miss-markers shadow them on subsequent runs.
        // Post-patch the function must yield the matching DST-aware hour
        // for both an EST Friday AND an EDT Friday inside the same query.
        var tmpFrom = new DateTime(2024, 1, 5, 0, 0, 0, DateTimeKind.Utc);   // Fri EST
        var tmpTo   = new DateTime(2024, 7, 12, 23, 59, 0, DateTimeKind.Utc); // Fri EDT

        var tmpResult = HistoricalBarsProvider.ComputeExpectedTimestamps(
            tmpFrom, tmpTo, BarTimeframe.OneDay);

        var tmpEstFriday = tmpResult.FirstOrDefault(t =>
            t.Year == 2024 && t.Month == 1 && t.Day == 5);
        tmpEstFriday.ShouldNotBe(default(DateTime));
        tmpEstFriday.Hour.ShouldBe(5, "Jan 5 2024 (EST) Friday must be at 05:00 UTC");

        var tmpEdtFriday = tmpResult.FirstOrDefault(t =>
            t.Year == 2024 && t.Month == 7 && t.Day == 12);
        tmpEdtFriday.ShouldNotBe(default(DateTime));
        tmpEdtFriday.Hour.ShouldBe(4, "Jul 12 2024 (EDT) Friday must be at 04:00 UTC");
    }

    [Fact]
    public void CoalesceContiguous_GroupsAdjacentMinutes()
    {
        var tmpInput = new[]
        {
            new DateTime(2026, 4, 15, 14, 0, 0, DateTimeKind.Utc),
            new DateTime(2026, 4, 15, 14, 1, 0, DateTimeKind.Utc),
            new DateTime(2026, 4, 15, 14, 2, 0, DateTimeKind.Utc),
            // Gap.
            new DateTime(2026, 4, 15, 14, 5, 0, DateTimeKind.Utc),
            new DateTime(2026, 4, 15, 14, 6, 0, DateTimeKind.Utc),
        };
        var tmpRanges = HistoricalBarsProvider.CoalesceContiguous(tmpInput, TimeSpan.FromMinutes(1));
        tmpRanges.Count.ShouldBe(2);
        tmpRanges[0].From.ShouldBe(tmpInput[0]);
        tmpRanges[0].To.ShouldBe(tmpInput[2]);
        tmpRanges[1].From.ShouldBe(tmpInput[3]);
        tmpRanges[1].To.ShouldBe(tmpInput[4]);
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private async Task<long> CountBarsAsync(string inTimeframe)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        return await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_bars WHERE symbol = 'TSLA' AND timeframe = @TF",
            new { TF = inTimeframe });
    }

    private async Task<long> CountMarkersAsync(string inTimeframe)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        return await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_bars_misses WHERE symbol = 'TSLA' AND timeframe = @TF",
            new { TF = inTimeframe });
    }

    private async Task TruncateMarkersAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync("TRUNCATE historical_bars_misses");
    }

    private async Task InsertMarkerAsync(string inTimeframe, DateTime inFrom, DateTime inTo)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            INSERT INTO historical_bars_misses (symbol, timeframe, range_from, range_to, reason, fetched_at)
            VALUES ('TSLA', @TF, @From, @To, 'test-seed', NOW())
            """,
            new { TF = inTimeframe, From = inFrom, To = inTo });
    }

    /// <summary>Seed bars for [from, to] inclusive minute-by-minute,
    /// optionally skipping any of the supplied [gapFrom, gapTo] sub-ranges.</summary>
    private async Task SeedBarsAsync(
        DateTime inFrom, DateTime inTo,
        DateTime? gapStart = null, DateTime? gapEnd = null)
    {
        var tmpGap = gapStart.HasValue && gapEnd.HasValue
            ? new[] { (gapStart.Value, gapEnd.Value) }
            : Array.Empty<(DateTime, DateTime)>();
        await SeedBarsAsync(inFrom, inTo, tmpGap);
    }

    private async Task SeedBarsAsync(
        DateTime inFrom, DateTime inTo,
        params (DateTime From, DateTime To)[] gaps)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        for (var tmpTs = inFrom; tmpTs <= inTo; tmpTs = tmpTs.AddMinutes(1))
        {
            var tmpInGap = false;
            foreach (var tmpGap in gaps)
            {
                if (tmpTs >= tmpGap.From && tmpTs <= tmpGap.To) { tmpInGap = true; break; }
            }
            if (tmpInGap) continue;

            await tmpConn.ExecuteAsync(
                """
                INSERT INTO historical_bars
                  (symbol, timeframe, timestamp, open, high, low, close, volume, vwap)
                VALUES
                  ('TSLA', '1min', @Ts, 100, 101, 99, 100.5, 1000, 100)
                ON CONFLICT (symbol, timeframe, timestamp) DO NOTHING
                """,
                new { Ts = tmpTs });
        }
    }

    /// <summary>
    /// Stub fetcher that records every (from, to, timeframe) it's called
    /// with and either returns N bars (returnEmpty=false) or an empty
    /// list (returnEmpty=true). For the partial-cache test we generate
    /// bars ONLY for the requested chunk so the cache can fill correctly.
    /// </summary>
    private sealed class RecordingFetcher : IPolygonBarFetcher
    {
        private readonly bool m_ReturnEmpty;
        private readonly int m_FillBars;
        public List<(DateTime From, DateTime To, BarTimeframe TF)> RecordedChunks { get; } = new();

        public RecordingFetcher(bool returnEmpty, int fillBars)
        {
            m_ReturnEmpty = returnEmpty;
            m_FillBars = fillBars;
        }

        public Task<IReadOnlyList<Bar>> FetchBarsAsync(
            string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            BarTimeframe inTimeframe, CancellationToken inCt)
        {
            RecordedChunks.Add((inFromUtc, inToUtc, inTimeframe));
            if (m_ReturnEmpty) return Task.FromResult<IReadOnlyList<Bar>>(Array.Empty<Bar>());

            // Fill exactly the requested range, minute-by-minute. The provider
            // will upsert these and the next gap-detection pass will see them
            // as cached.
            var tmpBars = new List<Bar>();
            for (var tmpTs = inFromUtc; tmpTs <= inToUtc && tmpBars.Count < m_FillBars; tmpTs = tmpTs.AddMinutes(1))
            {
                tmpBars.Add(new Bar(inSymbol, tmpTs, 100, 101, 99, 100.5m, 1000, 100));
            }
            return Task.FromResult<IReadOnlyList<Bar>>(tmpBars);
        }
    }

    private static async Task ApplySchemaAsync(string inConnStr)
    {
        await using var tmpConn = new NpgsqlConnection(inConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            CREATE EXTENSION IF NOT EXISTS timescaledb;

            CREATE TABLE IF NOT EXISTS historical_bars (
              symbol VARCHAR(10) NOT NULL,
              timeframe VARCHAR(10) NOT NULL,
              timestamp TIMESTAMPTZ NOT NULL,
              open DECIMAL(18,4) NOT NULL,
              high DECIMAL(18,4) NOT NULL,
              low DECIMAL(18,4) NOT NULL,
              close DECIMAL(18,4) NOT NULL,
              volume DECIMAL(18,2) NOT NULL,
              vwap DECIMAL(18,4),
              trade_count INT
            );
            SELECT create_hypertable('historical_bars', 'timestamp', if_not_exists => TRUE);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bars_symbol_timeframe_timestamp
              ON historical_bars (symbol, timeframe, timestamp);

            CREATE TABLE IF NOT EXISTS historical_bars_misses (
              symbol      VARCHAR(10)  NOT NULL,
              timeframe   VARCHAR(10)  NOT NULL,
              range_from  TIMESTAMPTZ  NOT NULL,
              range_to    TIMESTAMPTZ  NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (symbol, timeframe, range_from, range_to)
            );
            """);
    }
}
