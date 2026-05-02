using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using NSubstitute;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Tests the NBBO miss-marker rework (PR #3 of brief 2026-05-02): the
/// historical_options_quotes_misses table moved from point-shape
/// (ticker, ts) to range-shape (ticker, range_from, range_to). The
/// per-call <see cref="OptionQuotesProvider.GetAtOrBeforeAsync"/> path
/// writes degenerate 1-minute range markers; over many sequential
/// missing-minute fetches for the same contract the
/// <see cref="RangeMarkerWriter"/> coalesce-on-write logic collapses
/// them into ranges.
///
/// <para>
/// Coverage:
/// <list type="bullet">
///   <item>390 sequential missing minutes → 1 row, not 390.</item>
///   <item>Half-day silence (afternoon-only gap) → 1 row.</item>
///   <item>Two disjoint gaps in the same day → 2 rows.</item>
///   <item>Truncate markers → next call re-fetches; re-mark whatever's still empty.</item>
///   <item>IsKnownMissAsync recognises a ts inside an existing range.</item>
/// </list>
/// </para>
/// </summary>
public sealed class NbboRangeMarkerTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer m_Pg = new PostgreSqlBuilder()
        .WithImage("timescale/timescaledb:latest-pg16")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    private string m_ConnStr = null!;
    private const string c_Ticker = "O:TSLA260417C00250000";

    // Apr 17 2026 is a Friday — full-day trading. RTH session in EDT
    // (UTC-4): 13:30..20:00 UTC = 390 minutes. We anchor every test
    // here so the per-minute math doesn't drift.
    private static readonly DateTime s_RthOpenUtc = new(2026, 4, 17, 13, 30, 0, DateTimeKind.Utc);
    private static readonly DateTime s_RthCloseUtc = new(2026, 4, 17, 20, 0, 0, DateTimeKind.Utc);

    public async Task InitializeAsync()
    {
        await m_Pg.StartAsync();
        m_ConnStr = m_Pg.GetConnectionString();
        await ApplySchemaAsync(m_ConnStr);
    }

    public Task DisposeAsync() => m_Pg.DisposeAsync().AsTask();

    // ── 1. 390 sequential missing minutes → 1 marker row ────────────────

    [Fact]
    public async Task FullRthDay_AllMisses_CoalescesTo_OneMarkerRow()
    {
        var tmpProvider = BuildProvider(missAlways: true);

        // 390 sequential per-call requests, each returns a Polygon Miss →
        // each writes a degenerate 1-minute marker. The coalesce-on-write
        // must merge contiguous adjacent markers into a single row.
        for (var tmpTs = s_RthOpenUtc; tmpTs < s_RthCloseUtc; tmpTs = tmpTs.AddMinutes(1))
        {
            await tmpProvider.GetAtOrBeforeAsync(c_Ticker, tmpTs);
        }

        var tmpRowCount = await CountMarkerRowsAsync();
        tmpRowCount.ShouldBe(1L,
            "390 contiguous minute-misses must collapse into ONE range marker, " +
            $"not 390. Actual row count: {tmpRowCount}.");

        // The single row must span 13:30..19:59 UTC (last bar's open).
        var tmpRow = await ReadSingleMarkerAsync();
        tmpRow.From.ShouldBe(s_RthOpenUtc);
        tmpRow.To.ShouldBe(s_RthCloseUtc.AddMinutes(-1));
    }

    // ── 2. Half-day silence (afternoon-only) → 1 marker row ─────────────

    [Fact]
    public async Task AfternoonOnlySilence_CoalescesTo_OneMarkerRow()
    {
        // Morning (13:30..16:30 UTC = 09:30..12:30 ET) returns hits;
        // afternoon (16:30..19:59 UTC = 12:30..15:59 ET) returns misses.
        var tmpFetcher = Substitute.For<IPolygonNbboFetcher>();
        tmpFetcher
            .FetchAsync(Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var tmpTs = call.Arg<DateTime>();
                if (tmpTs < new DateTime(2026, 4, 17, 16, 30, 0, DateTimeKind.Utc))
                {
                    return new PolygonNbboFetch(PolygonNbboOutcome.Hit,
                        new PolygonNbboResult(c_Ticker, tmpTs, tmpTs.AddSeconds(-5),
                            1.20m, 1.25m, 10, 12, 1, 2),
                        null);
                }
                return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "no-data");
            });
        var tmpProvider = BuildProviderWith(tmpFetcher);

        for (var tmpTs = s_RthOpenUtc; tmpTs < s_RthCloseUtc; tmpTs = tmpTs.AddMinutes(1))
        {
            await tmpProvider.GetAtOrBeforeAsync(c_Ticker, tmpTs);
        }

        var tmpRowCount = await CountMarkerRowsAsync();
        tmpRowCount.ShouldBe(1L,
            "All afternoon misses must coalesce into ONE range row.");

        var tmpRow = await ReadSingleMarkerAsync();
        tmpRow.From.ShouldBe(new DateTime(2026, 4, 17, 16, 30, 0, DateTimeKind.Utc));
        tmpRow.To.ShouldBe(s_RthCloseUtc.AddMinutes(-1));
    }

    // ── 3. Two disjoint gaps in same day → 2 marker rows ────────────────

    [Fact]
    public async Task TwoDisjointGaps_WriteTwoMarkerRows()
    {
        // Miss windows: 13:30..13:39 (10 min) and 14:00..14:09 (10 min).
        // 13:40..13:59 (20 min) and 14:10..14:30 (21 min) are hits — the
        // intervening hits separate the two miss runs.
        var tmpFetcher = Substitute.For<IPolygonNbboFetcher>();
        tmpFetcher
            .FetchAsync(Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var tmpTs = call.Arg<DateTime>();
                bool tmpIsMiss =
                    (tmpTs >= new DateTime(2026, 4, 17, 13, 30, 0, DateTimeKind.Utc) &&
                     tmpTs < new DateTime(2026, 4, 17, 13, 40, 0, DateTimeKind.Utc)) ||
                    (tmpTs >= new DateTime(2026, 4, 17, 14, 0, 0, DateTimeKind.Utc) &&
                     tmpTs < new DateTime(2026, 4, 17, 14, 10, 0, DateTimeKind.Utc));
                if (tmpIsMiss)
                    return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "no-data");
                return new PolygonNbboFetch(PolygonNbboOutcome.Hit,
                    new PolygonNbboResult(c_Ticker, tmpTs, tmpTs.AddSeconds(-5),
                        1.20m, 1.25m, 10, 12, 1, 2),
                    null);
            });
        var tmpProvider = BuildProviderWith(tmpFetcher);

        // Walk 13:30..14:30 inclusive (61 minutes).
        var tmpEnd = new DateTime(2026, 4, 17, 14, 30, 0, DateTimeKind.Utc);
        for (var tmpTs = s_RthOpenUtc; tmpTs <= tmpEnd; tmpTs = tmpTs.AddMinutes(1))
        {
            await tmpProvider.GetAtOrBeforeAsync(c_Ticker, tmpTs);
        }

        var tmpRowCount = await CountMarkerRowsAsync();
        tmpRowCount.ShouldBe(2L,
            "Two disjoint miss-runs must produce 2 distinct marker rows.");
    }

    // ── 4. Truncate markers → next call re-fetches ──────────────────────

    [Fact]
    public async Task TruncateMarkers_ReFetchesAndReMarksRanges()
    {
        var tmpFetcher = Substitute.For<IPolygonNbboFetcher>();
        var tmpFetchCount = 0;
        tmpFetcher
            .FetchAsync(Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Interlocked.Increment(ref tmpFetchCount);
                return new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "no-data");
            });
        var tmpProvider = BuildProviderWith(tmpFetcher);

        // First pass: 5 contiguous misses → 1 marker.
        for (var i = 0; i < 5; i++)
        {
            await tmpProvider.GetAtOrBeforeAsync(c_Ticker, s_RthOpenUtc.AddMinutes(i));
        }
        var tmpFetchCountAfter1st = tmpFetchCount;
        tmpFetchCountAfter1st.ShouldBe(5);
        (await CountMarkerRowsAsync()).ShouldBe(1L);

        // Second pass on the SAME range: marker shadows everything →
        // no upstream calls. Note: NbboMemoryCache is shared with the
        // provider for the whole test, so we need a FRESH provider to
        // hit the postgres marker path (the in-memory miss-marker
        // shortcuts the postgres lookup).
        var tmpProvider2 = BuildProviderWith(tmpFetcher);
        for (var i = 0; i < 5; i++)
        {
            await tmpProvider2.GetAtOrBeforeAsync(c_Ticker, s_RthOpenUtc.AddMinutes(i));
        }
        tmpFetchCount.ShouldBe(tmpFetchCountAfter1st,
            "marker should shadow every ts → zero new upstream calls");

        // Truncate markers → next call must re-fetch.
        await TruncateMarkersAsync();
        var tmpProvider3 = BuildProviderWith(tmpFetcher);
        for (var i = 0; i < 5; i++)
        {
            await tmpProvider3.GetAtOrBeforeAsync(c_Ticker, s_RthOpenUtc.AddMinutes(i));
        }
        tmpFetchCount.ShouldBe(tmpFetchCountAfter1st + 5,
            "after truncate, every ts re-issues the upstream call");
        (await CountMarkerRowsAsync()).ShouldBe(1L,
            "and the markers are rebuilt as a coalesced range row");
    }

    // ── 5. IsKnownMissAsync — ts inside existing range ──────────────────

    [Fact]
    public async Task QueryMatchesTsInsideExistingRangeMarker()
    {
        // Hand-write a range marker covering 13:30..13:39 (10 minutes).
        // Provider's per-call lookup at 13:35 (mid-range) must short-circuit
        // to IsMissMarker=true with zero upstream calls.
        await InsertMarkerAsync(s_RthOpenUtc, s_RthOpenUtc.AddMinutes(9));

        var tmpFetcher = Substitute.For<IPolygonNbboFetcher>();
        var tmpProvider = BuildProviderWith(tmpFetcher);

        var tmpResult = await tmpProvider.GetAtOrBeforeAsync(
            c_Ticker, s_RthOpenUtc.AddMinutes(5));
        tmpResult.IsMissMarker.ShouldBeTrue();
        tmpResult.CacheHit.ShouldBeTrue();

        await tmpFetcher.DidNotReceive().FetchAsync(
            Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>());
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private OptionQuotesProvider BuildProvider(bool missAlways)
    {
        var tmpFetcher = Substitute.For<IPolygonNbboFetcher>();
        if (missAlways)
        {
            tmpFetcher
                .FetchAsync(Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                .Returns(new PolygonNbboFetch(PolygonNbboOutcome.Miss, null, "no-data"));
        }
        return BuildProviderWith(tmpFetcher);
    }

    private OptionQuotesProvider BuildProviderWith(IPolygonNbboFetcher inFetcher)
    {
        return new OptionQuotesProvider(
            new NbboMemoryCache(),
            inFetcher,
            Options.Create(new HistoryServiceOptions { ConnectionString = m_ConnStr }),
            NullLogger<OptionQuotesProvider>.Instance);
    }

    private async Task<long> CountMarkerRowsAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        return await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_options_quotes_misses WHERE ticker = @T",
            new { T = c_Ticker });
    }

    private async Task<(DateTime From, DateTime To)> ReadSingleMarkerAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        return await tmpConn.QueryFirstAsync<(DateTime From, DateTime To)>(
            """
            SELECT range_from AS "From", range_to AS "To"
            FROM historical_options_quotes_misses
            WHERE ticker = @T
            """,
            new { T = c_Ticker });
    }

    private async Task TruncateMarkersAsync()
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync("TRUNCATE historical_options_quotes_misses");
    }

    private async Task InsertMarkerAsync(DateTime inFrom, DateTime inTo)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            INSERT INTO historical_options_quotes_misses
              (ticker, range_from, range_to, reason, fetched_at)
            VALUES (@T, @F, @TT, 'test-seed', NOW())
            """,
            new { T = c_Ticker, F = inFrom, TT = inTo });
    }

    private static async Task ApplySchemaAsync(string inConnStr)
    {
        await using var tmpConn = new NpgsqlConnection(inConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            CREATE TABLE IF NOT EXISTS historical_options_quotes (
              ticker        VARCHAR(50)  NOT NULL,
              ts            TIMESTAMPTZ  NOT NULL,
              as_of_ts      TIMESTAMPTZ,
              bid_price     NUMERIC(18,4),
              ask_price     NUMERIC(18,4),
              bid_size      INT,
              ask_size      INT,
              bid_exchange  INT,
              ask_exchange  INT,
              fetched_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (ticker, ts)
            );
            CREATE TABLE IF NOT EXISTS historical_options_quotes_misses (
              ticker      VARCHAR(50)  NOT NULL,
              range_from  TIMESTAMPTZ  NOT NULL,
              range_to    TIMESTAMPTZ  NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (ticker, range_from, range_to)
            );
            """);
    }
}
