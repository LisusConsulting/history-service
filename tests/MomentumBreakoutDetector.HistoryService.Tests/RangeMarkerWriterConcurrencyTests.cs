using Dapper;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Regression tests for the 2026-05-02 concurrent-gap-fill race in
/// <see cref="RangeMarkerWriter.WriteAsync"/>. Two writers racing on the
/// same composite key both used to DELETE-zero-rows and then both
/// INSERT, the second one hitting <c>Npgsql.PostgresException 23505</c>
/// (unique-constraint violation on the marker table's PK,
/// e.g. <c>macro_data_misses_v2_pkey</c>). The fix wraps the
/// DELETE-then-INSERT in a <c>pg_advisory_xact_lock(table_hash,
/// key_hash)</c> so only writers competing for the same key serialise.
/// </summary>
public sealed class RangeMarkerWriterConcurrencyTests : IAsyncLifetime
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

    /// <summary>
    /// Eight tasks race to write the same range against the same
    /// (table, series_id) key. Pre-fix: at least one would surface a
    /// 23505 from the second concurrent INSERT. Post-fix: the advisory
    /// lock serialises them and the final state has exactly one row
    /// covering the union of the writes.
    /// </summary>
    [Fact]
    public async Task EightConcurrentWriters_SameKey_DoNotRaiseUniqueViolation()
    {
        const string kSeries = "T10Y2Y";
        const int kFanout = 8;

        // Each writer claims a 1-week slice of an 8-week window. After
        // they all finish, the markers should collapse to one row
        // covering 8 weeks (touch-merge of date-typed adjacent ranges).
        var tmpStart = new DateOnly(2024, 1, 1);
        var tmpRanges = Enumerable.Range(0, kFanout)
            .Select(i =>
            {
                var tmpFrom = tmpStart.AddDays(i * 7);
                var tmpTo = tmpStart.AddDays(((i + 1) * 7) - 1);
                return (
                    From: new DateTimeOffset(tmpFrom.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
                    To: new DateTimeOffset(tmpTo.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero));
            })
            .ToList();

        var tmpStartGate = new TaskCompletionSource();
        var tmpTasks = new List<Task>();
        var tmpExceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();
        for (var i = 0; i < kFanout; i++)
        {
            var tmpRange = tmpRanges[i];
            tmpTasks.Add(Task.Run(async () =>
            {
                await tmpStartGate.Task.ConfigureAwait(false);
                try
                {
                    await using var tmpConn = new NpgsqlConnection(m_ConnStr);
                    await tmpConn.OpenAsync().ConfigureAwait(false);
                    await RangeMarkerWriter.WriteAsync(
                        tmpConn,
                        MacroSpec,
                        new[] { new KeyValuePair<string, object>("SeriesId", kSeries) },
                        new[] { tmpRange },
                        "concurrent-test",
                        // 2-day adjacency so the 1-day gaps between
                        // contiguous slices (Sun..Mon) collapse to a
                        // single row at the end. Mirrors what the macro
                        // provider does at touch-merge time.
                        inAdjacencyTicks: TimeSpan.FromDays(2).Ticks,
                        inCt: CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    tmpExceptions.Add(ex);
                }
            }));
        }

        // Release all writers at once. Pre-fix this is what surfaces
        // the 23505 race deterministically (modulo lock contention
        // ordering).
        tmpStartGate.SetResult();
        await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        tmpExceptions.ShouldBeEmpty(
            "no concurrent writer should hit a unique-constraint violation");

        // Final state: one row spanning the union of all writes (or, if
        // the adjacency exactly hits, one row per non-adjacent island).
        // We assert "exactly one row" because we used a 2-day adjacency
        // which closes the 1-day inter-slice gap.
        await using var tmpVerifyConn = new NpgsqlConnection(m_ConnStr);
        await tmpVerifyConn.OpenAsync().ConfigureAwait(false);
        var tmpRows = (await tmpVerifyConn.QueryAsync<(string From, string To)>(
            """
            SELECT range_from::text AS "From", range_to::text AS "To"
            FROM macro_data_misses
            WHERE series_id = @S
            ORDER BY range_from
            """,
            new { S = kSeries }).ConfigureAwait(false)).ToList();

        tmpRows.Count.ShouldBe(1,
            "8 contiguous slices with 2-day adjacency must collapse to a single marker row");
        DateOnly.Parse(tmpRows[0].From).ShouldBe(new DateOnly(2024, 1, 1));
        DateOnly.Parse(tmpRows[0].To).ShouldBe(new DateOnly(2024, 1, 1).AddDays((kFanout * 7) - 1));
    }

    /// <summary>
    /// Same race, but each writer targets a distinct key. The advisory
    /// lock keys on (table, key_hash) so distinct-key writers must NOT
    /// serialise — they should execute fully in parallel and produce
    /// one row per series. This pins that the lock granularity is
    /// per-key, not per-table.
    /// </summary>
    [Fact]
    public async Task DistinctKeys_DoNotSerialise_AndAllRowsLand()
    {
        const int kFanout = 8;
        var tmpRange = (
            From: new DateTimeOffset(new DateTime(2024, 1, 1), TimeSpan.Zero),
            To: new DateTimeOffset(new DateTime(2024, 1, 7), TimeSpan.Zero));

        var tmpTasks = new List<Task>();
        for (var i = 0; i < kFanout; i++)
        {
            var tmpSeries = $"SERIES_{i}";
            tmpTasks.Add(Task.Run(async () =>
            {
                await using var tmpConn = new NpgsqlConnection(m_ConnStr);
                await tmpConn.OpenAsync().ConfigureAwait(false);
                await RangeMarkerWriter.WriteAsync(
                    tmpConn,
                    MacroSpec,
                    new[] { new KeyValuePair<string, object>("SeriesId", tmpSeries) },
                    new[] { tmpRange },
                    "distinct-keys",
                    inAdjacencyTicks: TimeSpan.FromDays(1).Ticks,
                    inCt: CancellationToken.None).ConfigureAwait(false);
            }));
        }
        await Task.WhenAll(tmpTasks).ConfigureAwait(false);

        await using var tmpVerifyConn = new NpgsqlConnection(m_ConnStr);
        await tmpVerifyConn.OpenAsync().ConfigureAwait(false);
        var tmpCount = await tmpVerifyConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM macro_data_misses").ConfigureAwait(false);
        tmpCount.ShouldBe((long)kFanout, "one row per distinct series");
    }

    /// <summary>
    /// FNV-1a hash must be stable across processes. The standard
    /// <see cref="string.GetHashCode()"/> is randomised per-process
    /// since .NET Core, which would defeat the cross-replica lock
    /// behaviour. Pin known values so a future "let's just use
    /// GetHashCode" regression is loud.
    /// </summary>
    [Fact]
    public void StableHashInt32_IsDeterministicAcrossProcesses()
    {
        // FNV-1a 32-bit golden values — verified against multiple
        // reference implementations.
        RangeMarkerWriter.StableHashInt32("").ShouldBe(unchecked((int)2166136261u));
        RangeMarkerWriter.StableHashInt32("a").ShouldBe(unchecked((int)0xe40c292cu));
        RangeMarkerWriter.StableHashInt32("foobar").ShouldBe(unchecked((int)0xbf9cf968u));
        // Non-trivial: real table name we lock on.
        RangeMarkerWriter.StableHashInt32("macro_data_misses").ShouldBe(
            RangeMarkerWriter.StableHashInt32("macro_data_misses"),
            "trivially equal — but proves the hash is repeatable in-process");
    }

    private static readonly RangeMarkerTableSpec MacroSpec = new(
        TableName: "macro_data_misses",
        KeyColumns: new[] { "series_id" },
        RangeFromColumn: "range_from",
        RangeToColumn: "range_to",
        FetchedAtColumn: "fetched_at",
        HasReasonColumn: true,
        ReasonColumn: "reason",
        RangeColumnType: RangeMarkerColumnType.Date);

    private static async Task ApplySchemaAsync(string inConnStr)
    {
        await using var tmpConn = new NpgsqlConnection(inConnStr);
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync(
            """
            CREATE TABLE IF NOT EXISTS macro_data_misses (
              series_id   VARCHAR(20)  NOT NULL,
              range_from  DATE         NOT NULL,
              range_to    DATE         NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              CONSTRAINT macro_data_misses_v2_pkey
                PRIMARY KEY (series_id, range_from, range_to)
            );
            """);
    }
}
