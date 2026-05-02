using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using TreyThomasCodes.Polygon.Models.Options;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Intra-range gap-detection tests for
/// <see cref="OptionChainProvider.EnsureRangeCachedAsync"/> — the same
/// expected-minus-cached-minus-marked rework PR #19 applied to bars and
/// PR #20 applied to NBBO, now applied to chains. (Lisus brief 2026-05-02
/// PR #21.)
///
/// <para>
/// Coverage:
/// <list type="bullet">
///   <item>Empty cache + empty markers → fetches every trading day in window.</item>
///   <item>Partial cache → fetches only missing trading days.</item>
///   <item>Range markers covering some gaps → those skipped.</item>
///   <item>Truncate markers → re-fetches the marked ranges.</item>
///   <item>Empty upstream over a contiguous run of trading days → ONE marker row, not N.</item>
///   <item>Adjacent markers from prior + new run → coalesced.</item>
///   <item>CoalesceContiguousTradingDays handles holiday-only gaps (no split).</item>
/// </list>
/// </para>
///
/// <para>
/// All tests anchor on the trading-week of 2026-04-13..2026-04-17 (Mon-Fri,
/// no holidays, no half-days) so the per-day math doesn't drift. April
/// 17, 2026 is a known full Friday in the calendar.
/// </para>
/// </summary>
public sealed class ChainsIntraRangeGapDetectionTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer m_Pg = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    private string m_ConnStr = null!;
    private const string c_Symbol = "TSLA";

    // Mon-Fri 2026-04-13..2026-04-17 — no holiday, no half-day.
    private static readonly DateOnly s_From = new(2026, 4, 13);
    private static readonly DateOnly s_To = new(2026, 4, 17);

    public async Task InitializeAsync()
    {
        await m_Pg.StartAsync();
        m_ConnStr = m_Pg.GetConnectionString();
        await ApplySchemaAsync(m_ConnStr);
    }

    public Task DisposeAsync() => m_Pg.DisposeAsync().AsTask();

    // ── 1. Empty cache + empty markers → fetches every trading day ───────

    [Fact]
    public async Task EmptyCacheAndMarkers_FetchesEveryTradingDay()
    {
        var tmpStub = new RecordingFetcher(returnEmpty: false);
        var tmpProvider = BuildProvider(tmpStub);

        var tmpUpstream = await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);

        tmpUpstream.ShouldBe(5, "5 trading days in window → 5 fetches");
        tmpStub.RecordedDays.Count.ShouldBe(5);
        tmpStub.RecordedDays.ShouldContain(new DateOnly(2026, 4, 13));
        tmpStub.RecordedDays.ShouldContain(new DateOnly(2026, 4, 17));

        // Cache should now contain rows for all 5 days.
        var tmpCachedDays = await CountCachedDaysAsync();
        tmpCachedDays.ShouldBe(5L);
    }

    // ── 2. Partial cache → fetches only missing days ─────────────────────

    [Fact]
    public async Task PartialCache_FetchesOnlyMissingDays()
    {
        // Pre-seed Mon (4/13), Wed (4/15), Fri (4/17). Tue (4/14) and Thu (4/16)
        // are missing. Expected: 2 fetches (Tue + Thu, but they are NOT
        // contiguous — Wed sits between them as cached, so they are 2 ranges).
        await SeedChainAsync(new DateOnly(2026, 4, 13));
        await SeedChainAsync(new DateOnly(2026, 4, 15));
        await SeedChainAsync(new DateOnly(2026, 4, 17));

        var tmpStub = new RecordingFetcher(returnEmpty: false);
        var tmpProvider = BuildProvider(tmpStub);

        var tmpUpstream = await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);

        tmpUpstream.ShouldBe(2, "Tue + Thu missing → 2 fetches");
        tmpStub.RecordedDays.Count.ShouldBe(2);
        tmpStub.RecordedDays.ShouldContain(new DateOnly(2026, 4, 14));
        tmpStub.RecordedDays.ShouldContain(new DateOnly(2026, 4, 16));
    }

    // ── 3. Existing range marker shadows part of window ──────────────────

    [Fact]
    public async Task ExistingMarkers_ShadowMissingDays()
    {
        // Cache: empty. Marker covers Tue..Thu (4/14..4/16). Expected:
        // fetch the un-shadowed days (Mon 4/13 + Fri 4/17), 2 fetches.
        await InsertMarkerAsync(new DateOnly(2026, 4, 14), new DateOnly(2026, 4, 16));

        var tmpStub = new RecordingFetcher(returnEmpty: false);
        var tmpProvider = BuildProvider(tmpStub);

        var tmpUpstream = await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);

        tmpUpstream.ShouldBe(2, "marker shadows Tue..Thu → only Mon + Fri to fetch");
        tmpStub.RecordedDays.ShouldContain(new DateOnly(2026, 4, 13));
        tmpStub.RecordedDays.ShouldContain(new DateOnly(2026, 4, 17));
        tmpStub.RecordedDays.ShouldNotContain(new DateOnly(2026, 4, 14));
        tmpStub.RecordedDays.ShouldNotContain(new DateOnly(2026, 4, 16));
    }

    // ── 4. Truncate markers → re-fetches their ranges ────────────────────

    [Fact]
    public async Task TruncatingMarkers_TriggersReFetchOnNextCall()
    {
        var tmpStub = new RecordingFetcher(returnEmpty: true);
        var tmpProvider = BuildProvider(tmpStub);

        // First call: empty upstream over 5 trading days → 1 marker covering them.
        await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);
        var tmpMarkerCountAfter1st = await CountMarkerRowsAsync();
        tmpMarkerCountAfter1st.ShouldBe(1L);

        // Second call: marker shadows everything → 0 fetches.
        tmpStub.RecordedDays.Clear();
        var tmpUpstreamWarm = await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);
        tmpUpstreamWarm.ShouldBe(0, "marker should shadow the full range — no upstream call");
        tmpStub.RecordedDays.Count.ShouldBe(0);

        // Truncate the markers → next call must re-fetch.
        await TruncateMarkersAsync();
        tmpStub.RecordedDays.Clear();
        var tmpUpstreamAfter = await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);
        tmpUpstreamAfter.ShouldBe(5, "after truncate, gap is back → 5 fetches");
        tmpStub.RecordedDays.Count.ShouldBe(5);
    }

    // ── 5. Empty upstream over 5 contiguous days → 1 marker row, not 5 ──

    [Fact]
    public async Task EmptyUpstream_FiveContiguousDays_WritesOneRangeMarker()
    {
        var tmpStub = new RecordingFetcher(returnEmpty: true);
        var tmpProvider = BuildProvider(tmpStub);

        await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);

        // Crucial assertion: ONE row, not 5.
        var tmpMarkerCount = await CountMarkerRowsAsync();
        tmpMarkerCount.ShouldBe(1L,
            "5 contiguous missing trading days must collapse into a SINGLE marker row");

        var tmpRow = await ReadSingleMarkerAsync();
        tmpRow.From.ShouldBe(s_From);
        tmpRow.To.ShouldBe(s_To);
    }

    // ── 6. Adjacent markers (prior + new run) → coalesced ────────────────

    [Fact]
    public async Task AdjacentMarkersAcrossRuns_AreCoalesced()
    {
        // Pre-seed a marker for Mon..Wed (4/13..4/15). Then run a new
        // ensure for Mon..Fri with empty upstream — the new "gap" is
        // Thu..Fri (4/16..4/17) which abuts the existing marker. The
        // RangeMarkerWriter should produce ONE row 4/13..4/17, not two.
        await InsertMarkerAsync(new DateOnly(2026, 4, 13), new DateOnly(2026, 4, 15));
        var tmpInitialCount = await CountMarkerRowsAsync();
        tmpInitialCount.ShouldBe(1L);

        var tmpStub = new RecordingFetcher(returnEmpty: true);
        var tmpProvider = BuildProvider(tmpStub);

        await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);

        var tmpFinalCount = await CountMarkerRowsAsync();
        tmpFinalCount.ShouldBe(1L,
            "new marker should merge with the existing adjacent marker");

        var tmpRow = await ReadSingleMarkerAsync();
        tmpRow.From.ShouldBe(new DateOnly(2026, 4, 13));
        tmpRow.To.ShouldBe(new DateOnly(2026, 4, 17),
            "merged marker should span the full union");
    }

    // ── 7. Multiple disjoint gaps in one window → multiple markers ───────

    [Fact]
    public async Task TwoDisjointGaps_BothEmpty_WriteTwoRangeMarkers()
    {
        // Cache covers Wed (4/15) only. Gaps: Mon..Tue (4/13..4/14) and
        // Thu..Fri (4/16..4/17). Both gaps' upstream returns empty → 2
        // distinct markers (the cached Wed separates them).
        await SeedChainAsync(new DateOnly(2026, 4, 15));

        var tmpStub = new RecordingFetcher(returnEmpty: true);
        var tmpProvider = BuildProvider(tmpStub);

        await tmpProvider.EnsureRangeCachedAsync(
            c_Symbol, s_From, s_To, CancellationToken.None);

        var tmpMarkerCount = await CountMarkerRowsAsync();
        tmpMarkerCount.ShouldBe(2L,
            "two disjoint missing ranges → two distinct marker rows");
    }

    // ── 8. Pure-function: CoalesceContiguousTradingDays semantics ────────

    [Fact]
    public void CoalesceContiguousTradingDays_AdjacentDaysGroup()
    {
        var tmpInput = new[]
        {
            new DateOnly(2026, 4, 13),  // Mon
            new DateOnly(2026, 4, 14),  // Tue
            new DateOnly(2026, 4, 15),  // Wed
            // Skip Thu.
            new DateOnly(2026, 4, 17),  // Fri (Thu is a trading day → real gap)
        };
        var tmpRanges = OptionChainProvider.CoalesceContiguousTradingDays(tmpInput);
        tmpRanges.Count.ShouldBe(2);
        tmpRanges[0].From.ShouldBe(new DateOnly(2026, 4, 13));
        tmpRanges[0].To.ShouldBe(new DateOnly(2026, 4, 15));
        tmpRanges[1].From.ShouldBe(new DateOnly(2026, 4, 17));
        tmpRanges[1].To.ShouldBe(new DateOnly(2026, 4, 17));
    }

    [Fact]
    public void CoalesceContiguousTradingDays_WeekendInBetween_NoSplit()
    {
        // Fri 4/17 + Mon 4/20 — calendar gap, but no trading days strictly
        // between (weekend). Should coalesce into a single Fri..Mon range.
        var tmpInput = new[]
        {
            new DateOnly(2026, 4, 17),  // Fri
            new DateOnly(2026, 4, 20),  // Mon (Sat/Sun not trading days)
        };
        var tmpRanges = OptionChainProvider.CoalesceContiguousTradingDays(tmpInput);
        tmpRanges.Count.ShouldBe(1,
            "weekend in between is not a trading-day gap — should coalesce");
        tmpRanges[0].From.ShouldBe(new DateOnly(2026, 4, 17));
        tmpRanges[0].To.ShouldBe(new DateOnly(2026, 4, 20));
    }

    [Fact]
    public void CoalesceContiguousTradingDays_HolidayInBetween_NoSplit()
    {
        // 2026-04-02 (Thu) + 2026-04-06 (Mon) bracket Good Friday
        // (2026-04-03, holiday) and the weekend. No trading days strictly
        // between → coalesce into one range.
        TradingCalendar.IsTradingDay(new DateOnly(2026, 4, 3)).ShouldBeFalse(
            "2026-04-03 is Good Friday — must be a holiday in the calendar");
        var tmpInput = new[]
        {
            new DateOnly(2026, 4, 2),
            new DateOnly(2026, 4, 6),
        };
        var tmpRanges = OptionChainProvider.CoalesceContiguousTradingDays(tmpInput);
        tmpRanges.Count.ShouldBe(1,
            "holiday + weekend in between is not a trading-day gap — should coalesce");
        tmpRanges[0].From.ShouldBe(new DateOnly(2026, 4, 2));
        tmpRanges[0].To.ShouldBe(new DateOnly(2026, 4, 6));
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private OptionChainProvider BuildProvider(IPolygonChainFetcher inFetcher)
    {
        return new OptionChainProvider(
            Options.Create(new HistoryServiceOptions { ConnectionString = m_ConnStr }),
            NullLogger<OptionChainProvider>.Instance,
            inFetcher);
    }

    private async Task<long> CountCachedDaysAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        return await tmpConn.ExecuteScalarAsync<long>(
            """
            SELECT COUNT(DISTINCT as_of_date)
            FROM historical_options_contracts
            WHERE underlying_ticker = @S
            """,
            new { S = c_Symbol });
    }

    private async Task<long> CountMarkerRowsAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        return await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_options_chains_misses WHERE symbol = @S",
            new { S = c_Symbol });
    }

    private async Task<(DateOnly From, DateOnly To)> ReadSingleMarkerAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        // Cast to text + parse so Dapper's tuple binder doesn't fight
        // postgres DATE → DateTime conversion.
        var tmp = await tmpConn.QueryFirstAsync<(string From, string To)>(
            """
            SELECT range_from::text AS "From", range_to::text AS "To"
            FROM historical_options_chains_misses
            WHERE symbol = @S
            """,
            new { S = c_Symbol });
        return (DateOnly.Parse(tmp.From), DateOnly.Parse(tmp.To));
    }

    private async Task TruncateMarkersAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync("TRUNCATE historical_options_chains_misses");
    }

    private async Task InsertMarkerAsync(DateOnly inFrom, DateOnly inTo)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            INSERT INTO historical_options_chains_misses
              (symbol, range_from, range_to, reason, fetched_at)
            VALUES (@S, @F::date, @T::date, 'test-seed', NOW())
            """,
            new
            {
                S = c_Symbol,
                F = inFrom.ToString("yyyy-MM-dd"),
                T = inTo.ToString("yyyy-MM-dd"),
            });
    }

    /// <summary>Seed a single (symbol, as_of_date) row in the contracts cache —
    /// just enough to make the day appear "cached" to the gap detector.</summary>
    private async Task SeedChainAsync(DateOnly inAsOfDate)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            INSERT INTO historical_options_contracts
              (as_of_date, ticker, underlying_ticker, contract_type,
               exercise_style, expiration_date, strike_price,
               shares_per_contract, primary_exchange)
            VALUES
              (@D::date, @Tk, @S, 'call', 'american', @D::date,
               100, 100, 'BATO')
            ON CONFLICT (as_of_date, ticker) DO NOTHING
            """,
            new
            {
                S = c_Symbol,
                Tk = $"O:{c_Symbol}{inAsOfDate:yyMMdd}C00100000",
                D = inAsOfDate.ToString("yyyy-MM-dd"),
            });
    }

    /// <summary>
    /// Stub fetcher that records every (symbol, as_of) it's called with
    /// and returns either a synthetic 1-contract chain (returnEmpty=false)
    /// or an empty list (returnEmpty=true).
    /// </summary>
    private sealed class RecordingFetcher : IPolygonChainFetcher
    {
        private readonly bool m_ReturnEmpty;
        public List<DateOnly> RecordedDays { get; } = new();

        public RecordingFetcher(bool returnEmpty) { m_ReturnEmpty = returnEmpty; }

        public Task<IReadOnlyList<OptionsContract>> FetchChainAsync(
            string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
        {
            RecordedDays.Add(inAsOfDate);
            if (m_ReturnEmpty)
            {
                return Task.FromResult<IReadOnlyList<OptionsContract>>(
                    Array.Empty<OptionsContract>());
            }
            // One synthetic call contract per fetched day. ExpirationDate
            // string format matches what the upsert path expects.
            var tmpContract = new OptionsContract
            {
                Ticker = $"O:{inSymbol}{inAsOfDate:yyMMdd}C00250000",
                UnderlyingTicker = inSymbol,
                ContractType = "call",
                ExerciseStyle = "american",
                ExpirationDate = inAsOfDate.AddDays(7).ToString("yyyy-MM-dd"),
                StrikePrice = 250m,
                SharesPerContract = 100,
                PrimaryExchange = "BATO",
            };
            return Task.FromResult<IReadOnlyList<OptionsContract>>(
                new[] { tmpContract });
        }
    }

    private static async Task ApplySchemaAsync(string inConnStr)
    {
        await using var tmpConn = new NpgsqlConnection(inConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            CREATE TABLE IF NOT EXISTS historical_options_contracts (
              as_of_date DATE NOT NULL,
              ticker VARCHAR(50) NOT NULL,
              underlying_ticker VARCHAR(10) NOT NULL,
              contract_type VARCHAR(10),
              exercise_style VARCHAR(20),
              expiration_date DATE,
              strike_price DECIMAL(18,4),
              shares_per_contract INT,
              primary_exchange VARCHAR(10)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_options_date_ticker
              ON historical_options_contracts (as_of_date, ticker);

            CREATE TABLE IF NOT EXISTS historical_options_chains_misses (
              symbol      VARCHAR(10)  NOT NULL,
              range_from  DATE         NOT NULL,
              range_to    DATE         NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (symbol, range_from, range_to)
            );
            """);
    }
}
