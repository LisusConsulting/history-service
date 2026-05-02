using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Intra-range gap-detection tests for
/// <see cref="MacroDataProvider.EnsureRangeCachedAsync"/> — the same
/// expected-minus-cached-minus-marked rework PR #19/#20/#21 applied to
/// bars/NBBO/chains, now applied to macro (Lisus brief 2026-05-02 PR
/// #22). Range-shape <c>macro_data_misses</c> + coalesce-on-write via
/// the shared <see cref="RangeMarkerWriter"/>.
///
/// <para>
/// Coverage:
/// <list type="bullet">
///   <item>Empty cache + empty markers → fetches whole range (one FRED call).</item>
///   <item>Partial cache → second run does NOT fetch (cached + future markers cover).</item>
///   <item>Existing range markers covering some boundaries → those skipped.</item>
///   <item>Truncate markers → re-fetch the marked ranges.</item>
///   <item>Empty FRED over a contiguous Daily run → ONE marker row, not N.</item>
///   <item>Empty FRED over a contiguous Monthly run → ONE marker row.</item>
///   <item>Adjacent markers from prior + new run → coalesced (Daily Fri..Mon).</item>
///   <item>Pure-function: CoalesceContiguousBoundaries semantics for both cadences.</item>
/// </list>
/// </para>
/// </summary>
public sealed class MacroIntraRangeGapDetectionTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer m_Pg = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    private string m_ConnStr = null!;

    public async Task InitializeAsync()
    {
        await m_Pg.StartAsync();
        m_ConnStr = m_Pg.GetConnectionString();
        await ApplySchemaAsync(m_ConnStr);
    }

    public Task DisposeAsync() => m_Pg.DisposeAsync().AsTask();

    // ── 1. Empty cache + empty markers → fetches whole range ────────────

    [Fact]
    public async Task EmptyCacheAndMarkers_FetchesWholeRange_OneFredCall()
    {
        // T10Y2Y is Daily. Mon 2024-04-29..Fri 2024-05-03 = 5 weekdays.
        // Stub returns all 5 with real values → no markers, 5 cached rows.
        var stub = new RecordingFredFetcher
        {
            Responses =
            {
                ["T10Y2Y"] = new()
                {
                    new("T10Y2Y", new DateOnly(2024, 4, 29), -0.34m),
                    new("T10Y2Y", new DateOnly(2024, 4, 30), -0.32m),
                    new("T10Y2Y", new DateOnly(2024, 5, 1), -0.30m),
                    new("T10Y2Y", new DateOnly(2024, 5, 2), -0.29m),
                    new("T10Y2Y", new DateOnly(2024, 5, 3), -0.28m),
                },
            },
        };
        var provider = BuildProvider(stub);

        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);

        stub.CallCount.ShouldBe(1, "FRED's range fetch returns the whole window in one call");
        (await CountCachedRowsAsync("T10Y2Y")).ShouldBe(5L);
        (await CountMarkerRowsAsync("T10Y2Y")).ShouldBe(0L,
            "all 5 boundaries had real values — no markers");
    }

    // ── 2. Warm cache → no FRED call on second run ──────────────────────

    [Fact]
    public async Task WarmCache_SecondRun_NoFredCall()
    {
        var stub = new RecordingFredFetcher
        {
            Responses =
            {
                ["T10Y2Y"] = new()
                {
                    new("T10Y2Y", new DateOnly(2024, 4, 29), -0.34m),
                    new("T10Y2Y", new DateOnly(2024, 4, 30), -0.32m),
                    new("T10Y2Y", new DateOnly(2024, 5, 1), -0.30m),
                    new("T10Y2Y", new DateOnly(2024, 5, 2), -0.29m),
                    new("T10Y2Y", new DateOnly(2024, 5, 3), -0.28m),
                },
            },
        };
        var provider = BuildProvider(stub);

        // First run → 1 fetch.
        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);
        stub.CallCount.ShouldBe(1);

        // Second run → cache fully covers expected boundaries → 0 fetches.
        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);
        stub.CallCount.ShouldBe(1, "warm cache must not re-fetch");
    }

    // ── 3. Existing range marker shadows boundaries ─────────────────────

    [Fact]
    public async Task ExistingMarker_ShadowsMissingBoundaries()
    {
        // Marker covers Tue..Thu (4/30..5/2). Window 4/29..5/3 expects
        // Mon..Fri (5 boundaries). Mon (4/29) and Fri (5/3) are NOT in
        // the marker → those should be fetched. The fetcher returns ONLY
        // those two (FRED filters its response to what we asked the
        // provider to look up — modeled here by stub).
        await InsertMarkerAsync("T10Y2Y", new DateOnly(2024, 4, 30), new DateOnly(2024, 5, 2));

        var stub = new RecordingFredFetcher
        {
            Responses =
            {
                ["T10Y2Y"] = new()
                {
                    new("T10Y2Y", new DateOnly(2024, 4, 29), -0.34m),
                    new("T10Y2Y", new DateOnly(2024, 5, 3), -0.28m),
                },
            },
        };
        var provider = BuildProvider(stub);

        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);

        stub.CallCount.ShouldBe(1, "1 fetch for the un-shadowed boundaries");
        var cached = await CountCachedRowsAsync("T10Y2Y");
        cached.ShouldBe(2L, "only Mon + Fri got cached; Tue..Thu shadowed");
    }

    // ── 4. Truncate markers → re-fetch ──────────────────────────────────

    [Fact]
    public async Task TruncatingMarkers_TriggersReFetchOnNextCall()
    {
        var stub = new RecordingFredFetcher
        {
            // Empty response — will produce markers.
            Responses = { ["T10Y2Y"] = new() },
        };
        var provider = BuildProvider(stub);

        // First call: empty FRED over 5 weekdays → markers written.
        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);
        var markersAfter1st = await CountMarkerRowsAsync("T10Y2Y");
        markersAfter1st.ShouldBe(1L, "5 contiguous Daily boundaries → 1 range marker");

        // Second call: marker shadows everything → no FRED call.
        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);
        stub.CallCount.ShouldBe(1, "marker shadows all → no extra fetch");

        // Truncate markers → next call must re-fetch.
        await TruncateMarkersAsync();
        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);
        stub.CallCount.ShouldBe(2, "after truncate, FRED is called again");
    }

    // ── 5. Empty FRED over 5 contiguous Daily boundaries → 1 marker row ─

    [Fact]
    public async Task EmptyFred_FiveContiguousWeekdays_OneMarkerRow()
    {
        var stub = new RecordingFredFetcher { Responses = { ["T10Y2Y"] = new() } };
        var provider = BuildProvider(stub);

        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);

        // Crucial: ONE row, not 5. Range spans Mon..Fri.
        (await CountMarkerRowsAsync("T10Y2Y")).ShouldBe(1L,
            "5 contiguous Daily boundaries must collapse into a SINGLE row");
        var (from, to) = await ReadSingleMarkerAsync("T10Y2Y");
        from.ShouldBe(new DateOnly(2024, 4, 29));
        to.ShouldBe(new DateOnly(2024, 5, 3));
    }

    // ── 6. Empty FRED over 3 contiguous Monthly boundaries → 1 marker ───

    [Fact]
    public async Task EmptyFred_ThreeContiguousMonths_OneMarkerRow()
    {
        // CPIAUCSL is Monthly. Window 2024-01-01..2024-03-31 covers Jan,
        // Feb, Mar first-of-month boundaries. Empty FRED → one marker
        // covering all three.
        var stub = new RecordingFredFetcher { Responses = { ["CPIAUCSL"] = new() } };
        var provider = BuildProvider(stub);

        await provider.EnsureRangeCachedAsync(
            "CPIAUCSL", new DateOnly(2024, 1, 1), new DateOnly(2024, 3, 31),
            CancellationToken.None);

        (await CountMarkerRowsAsync("CPIAUCSL")).ShouldBe(1L,
            "3 contiguous Monthly boundaries must collapse into a SINGLE row");
        var (from, to) = await ReadSingleMarkerAsync("CPIAUCSL");
        from.ShouldBe(new DateOnly(2024, 1, 1));
        to.ShouldBe(new DateOnly(2024, 3, 1));
    }

    // ── 7. Adjacent markers across runs → coalesced ─────────────────────

    [Fact]
    public async Task AdjacentMarkersAcrossRuns_AreCoalesced()
    {
        // Pre-seed a marker for Mon..Wed (4/29..5/1). Empty FRED over
        // window Mon..Fri (4/29..5/3) → new gap-set is Thu..Fri (5/2..5/3)
        // which abuts the existing marker. RangeMarkerWriter should
        // produce ONE row 4/29..5/3.
        await InsertMarkerAsync("T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 1));
        (await CountMarkerRowsAsync("T10Y2Y")).ShouldBe(1L);

        var stub = new RecordingFredFetcher { Responses = { ["T10Y2Y"] = new() } };
        var provider = BuildProvider(stub);

        await provider.EnsureRangeCachedAsync(
            "T10Y2Y", new DateOnly(2024, 4, 29), new DateOnly(2024, 5, 3),
            CancellationToken.None);

        (await CountMarkerRowsAsync("T10Y2Y")).ShouldBe(1L,
            "new marker should merge with the existing adjacent marker");
        var (from, to) = await ReadSingleMarkerAsync("T10Y2Y");
        from.ShouldBe(new DateOnly(2024, 4, 29));
        to.ShouldBe(new DateOnly(2024, 5, 3),
            "merged marker should span the full union");
    }

    // ── 8. Pure-function: CoalesceContiguousBoundaries semantics ────────

    [Fact]
    public void CoalesceContiguousBoundaries_DailyContiguous()
    {
        // All 5 weekdays in a row.
        var input = new[]
        {
            new DateOnly(2024, 4, 29),
            new DateOnly(2024, 4, 30),
            new DateOnly(2024, 5, 1),
            new DateOnly(2024, 5, 2),
            new DateOnly(2024, 5, 3),
        };
        var ranges = MacroDataProvider.CoalesceContiguousBoundaries(input, FredSeriesCadence.Daily);
        ranges.Count.ShouldBe(1);
        ranges[0].From.ShouldBe(input[0]);
        ranges[0].To.ShouldBe(input[^1]);
    }

    [Fact]
    public void CoalesceContiguousBoundaries_DailyWithGap_Splits()
    {
        // Mon, Tue, Thu, Fri — Wed missing means Tue..Thu has a boundary
        // (Wed) strictly between → split.
        var input = new[]
        {
            new DateOnly(2024, 4, 29),  // Mon
            new DateOnly(2024, 4, 30),  // Tue
            new DateOnly(2024, 5, 2),   // Thu (Wed 5/1 is missing — that's the gap)
            new DateOnly(2024, 5, 3),   // Fri
        };
        var ranges = MacroDataProvider.CoalesceContiguousBoundaries(input, FredSeriesCadence.Daily);
        ranges.Count.ShouldBe(2);
        ranges[0].From.ShouldBe(input[0]);
        ranges[0].To.ShouldBe(input[1]);
        ranges[1].From.ShouldBe(input[2]);
        ranges[1].To.ShouldBe(input[3]);
    }

    [Fact]
    public void CoalesceContiguousBoundaries_DailyWeekendInBetween_NoSplit()
    {
        // Fri 4/26 + Mon 4/29 — no business days strictly between them
        // (weekend) → coalesce.
        var input = new[]
        {
            new DateOnly(2024, 4, 26),
            new DateOnly(2024, 4, 29),
        };
        var ranges = MacroDataProvider.CoalesceContiguousBoundaries(input, FredSeriesCadence.Daily);
        ranges.Count.ShouldBe(1, "weekend in between is not a Daily boundary — should coalesce");
        ranges[0].From.ShouldBe(input[0]);
        ranges[0].To.ShouldBe(input[1]);
    }

    [Fact]
    public void CoalesceContiguousBoundaries_MonthlyContiguous()
    {
        var input = new[]
        {
            new DateOnly(2024, 1, 1),
            new DateOnly(2024, 2, 1),
            new DateOnly(2024, 3, 1),
        };
        var ranges = MacroDataProvider.CoalesceContiguousBoundaries(input, FredSeriesCadence.Monthly);
        ranges.Count.ShouldBe(1);
        ranges[0].From.ShouldBe(input[0]);
        ranges[0].To.ShouldBe(input[^1]);
    }

    [Fact]
    public void CoalesceContiguousBoundaries_MonthlyWithGap_Splits()
    {
        // Jan + Mar — Feb 1 is an expected boundary strictly between → split.
        var input = new[]
        {
            new DateOnly(2024, 1, 1),
            new DateOnly(2024, 3, 1),
        };
        var ranges = MacroDataProvider.CoalesceContiguousBoundaries(input, FredSeriesCadence.Monthly);
        ranges.Count.ShouldBe(2);
        ranges[0].From.ShouldBe(input[0]);
        ranges[0].To.ShouldBe(input[0]);
        ranges[1].From.ShouldBe(input[1]);
        ranges[1].To.ShouldBe(input[1]);
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private MacroDataProvider BuildProvider(IFredFetcher inFetcher)
    {
        return new MacroDataProvider(
            m_ConnStr,
            NullLogger<MacroDataProvider>.Instance,
            inFetcher);
    }

    private async Task<long> CountCachedRowsAsync(string inSeries)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        return await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM macro_data WHERE series_id = @S",
            new { S = inSeries });
    }

    private async Task<long> CountMarkerRowsAsync(string inSeries)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        return await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM macro_data_misses WHERE series_id = @S",
            new { S = inSeries });
    }

    private async Task<(DateOnly From, DateOnly To)> ReadSingleMarkerAsync(string inSeries)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        var tmp = await tmpConn.QueryFirstAsync<(string From, string To)>(
            """
            SELECT range_from::text AS "From", range_to::text AS "To"
            FROM macro_data_misses
            WHERE series_id = @S
            """,
            new { S = inSeries });
        return (DateOnly.Parse(tmp.From), DateOnly.Parse(tmp.To));
    }

    private async Task TruncateMarkersAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync("TRUNCATE macro_data_misses");
    }

    private async Task InsertMarkerAsync(string inSeries, DateOnly inFrom, DateOnly inTo)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            INSERT INTO macro_data_misses
              (series_id, range_from, range_to, reason, fetched_at)
            VALUES (@S, @F::date, @T::date, 'test-seed', NOW())
            """,
            new
            {
                S = inSeries,
                F = inFrom.ToString("yyyy-MM-dd"),
                T = inTo.ToString("yyyy-MM-dd"),
            });
    }

    private sealed class RecordingFredFetcher : IFredFetcher
    {
        public Dictionary<string, List<FredObservationRow>> Responses { get; } = new();
        public int CallCount { get; private set; }

        public Task<IReadOnlyList<FredObservationRow>> FetchSeriesAsync(
            string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            CallCount++;
            return Task.FromResult<IReadOnlyList<FredObservationRow>>(
                Responses.TryGetValue(seriesId, out var rows)
                    ? rows
                    : Array.Empty<FredObservationRow>());
        }
    }

    private static async Task ApplySchemaAsync(string inConnStr)
    {
        await using var tmpConn = new NpgsqlConnection(inConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            CREATE TABLE IF NOT EXISTS macro_data (
              series_id VARCHAR(20) NOT NULL,
              observation_date DATE NOT NULL,
              value DECIMAL(18,6)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_series_date
              ON macro_data (series_id, observation_date);

            CREATE TABLE IF NOT EXISTS macro_data_misses (
              series_id   VARCHAR(20)  NOT NULL,
              range_from  DATE         NOT NULL,
              range_to    DATE         NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (series_id, range_from, range_to)
            );
            """);
    }
}
