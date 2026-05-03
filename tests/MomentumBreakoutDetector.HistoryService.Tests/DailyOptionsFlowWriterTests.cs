using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// PR 2 — write-path tests for <see cref="DailyOptionsFlowProvider"/>.
/// Stands up a Postgres testcontainer with the migration-012 schema
/// inline (no Timescale extension on <c>postgres:16-alpine</c>; the
/// daily_options_flow table works as a plain table for write semantics)
/// and exercises:
///   - UPSERT inserts a fresh row
///   - UPSERT a second time on the same key UPDATEs in place
///   - UPSERT round-trips NULL put_call_ratio + flow_score
///   - RecordMissAsync writes one degenerate range row
///   - RecordMissAsync coalesces adjacent ranges via RangeMarkerWriter
///
/// These tests pair with the read-path tests in
/// <see cref="DailyOptionsFlowProviderTests"/>.
/// </summary>
public class DailyOptionsFlowWriterTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await using var tmpCmd = tmpConn.CreateCommand();
        tmpCmd.CommandText = """
        CREATE TABLE IF NOT EXISTS daily_options_flow (
          underlying_ticker VARCHAR(10)    NOT NULL,
          trade_date        DATE           NOT NULL,
          call_volume       BIGINT         NOT NULL DEFAULT 0,
          put_volume        BIGINT         NOT NULL DEFAULT 0,
          call_oi           BIGINT         NOT NULL DEFAULT 0,
          put_oi            BIGINT         NOT NULL DEFAULT 0,
          put_call_ratio    DECIMAL(10,4),
          flow_score        DECIMAL(6,4),
          contract_count    INT            NOT NULL DEFAULT 0,
          fetched_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
          PRIMARY KEY (underlying_ticker, trade_date)
        );
        CREATE TABLE IF NOT EXISTS daily_options_flow_misses (
          underlying_ticker VARCHAR(10)  NOT NULL,
          range_from        DATE         NOT NULL,
          range_to          DATE         NOT NULL,
          reason            TEXT,
          fetched_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
          PRIMARY KEY (underlying_ticker, range_from, range_to)
        );
        """;
        await tmpCmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task UpsertAsync_FreshKey_InsertsRow()
    {
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(), NullLogger<DailyOptionsFlowProvider>.Instance);

        var tmpRow = new DailyOptionsFlowRow(
            UnderlyingTicker: "TSLA",
            TradeDate: new DateOnly(2024, 1, 2),
            CallVolume: 200_000L,
            PutVolume: 150_000L,
            CallOi: 0L,
            PutOi: 0L,
            PutCallRatio: 0.75m,
            FlowScore: 0.245m,
            ContractCount: 30);

        await tmpProvider.UpsertAsync(new[] { tmpRow });

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 31));
        tmpRows.Count.ShouldBe(1);
        tmpRows[0].CallVolume.ShouldBe(200_000L);
        tmpRows[0].PutVolume.ShouldBe(150_000L);
        tmpRows[0].PutCallRatio.ShouldBe(0.75m);
        tmpRows[0].FlowScore.ShouldBe(0.245m);
        tmpRows[0].ContractCount.ShouldBe(30);
    }

    [Fact]
    public async Task UpsertAsync_ExistingKey_UpdatesInPlace()
    {
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(), NullLogger<DailyOptionsFlowProvider>.Instance);

        // First write — initial row.
        await tmpProvider.UpsertAsync(new[]
        {
            new DailyOptionsFlowRow(
                "TSLA", new DateOnly(2024, 1, 2),
                CallVolume: 100_000L, PutVolume: 80_000L,
                CallOi: 0L, PutOi: 0L,
                PutCallRatio: 0.8m, FlowScore: 0.14m,
                ContractCount: 20),
        });

        // Second write — updated values for the same key.
        await tmpProvider.UpsertAsync(new[]
        {
            new DailyOptionsFlowRow(
                "TSLA", new DateOnly(2024, 1, 2),
                CallVolume: 250_000L, PutVolume: 200_000L,
                CallOi: 0L, PutOi: 0L,
                PutCallRatio: 0.8m, FlowScore: 0.14m,
                ContractCount: 35),
        });

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 31));
        tmpRows.Count.ShouldBe(1);
        tmpRows[0].CallVolume.ShouldBe(250_000L);
        tmpRows[0].PutVolume.ShouldBe(200_000L);
        tmpRows[0].ContractCount.ShouldBe(35);
    }

    [Fact]
    public async Task UpsertAsync_NullRatioAndScore_RoundTripAsNull()
    {
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(), NullLogger<DailyOptionsFlowProvider>.Instance);

        // call_volume=0 → ratio + score undefined ⇒ NULL columns.
        var tmpRow = new DailyOptionsFlowRow(
            "TSLA", new DateOnly(2024, 1, 15),
            CallVolume: 0L, PutVolume: 50_000L,
            CallOi: 0L, PutOi: 0L,
            PutCallRatio: null, FlowScore: null,
            ContractCount: 12);

        await tmpProvider.UpsertAsync(new[] { tmpRow });

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 15), new DateOnly(2024, 1, 15));
        tmpRows.Count.ShouldBe(1);
        tmpRows[0].PutCallRatio.ShouldBeNull();
        tmpRows[0].FlowScore.ShouldBeNull();
    }

    [Fact]
    public async Task RecordMissAsync_FreshSymbol_WritesOneRow()
    {
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(), NullLogger<DailyOptionsFlowProvider>.Instance);

        await tmpProvider.RecordMissAsync(
            "TSLA", new DateOnly(2024, 1, 2), new DateOnly(2024, 1, 2),
            "no-chain-rows-cached");

        var tmpCount = await CountMissesAsync("TSLA");
        tmpCount.ShouldBe(1);
    }

    [Fact]
    public async Task RecordMissAsync_AdjacentRanges_CoalesceOnWrite()
    {
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(), NullLogger<DailyOptionsFlowProvider>.Instance);

        // Two adjacent 1-day markers with one day in between (so the
        // 1-day-tick adjacency window collapses them into a single
        // 2024-01-02..04 row on the second write — same shape as the
        // chains miss-marker pattern).
        await tmpProvider.RecordMissAsync(
            "TSLA", new DateOnly(2024, 1, 2), new DateOnly(2024, 1, 2), "miss-day-1");
        await tmpProvider.RecordMissAsync(
            "TSLA", new DateOnly(2024, 1, 3), new DateOnly(2024, 1, 3), "miss-day-2");

        var tmpCount = await CountMissesAsync("TSLA");
        // Two writes within 1 calendar day collapse to 1 merged row.
        tmpCount.ShouldBe(1);
    }

    private async Task<int> CountMissesAsync(string inSymbol)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await using var tmpCmd = tmpConn.CreateCommand();
        tmpCmd.CommandText = "SELECT COUNT(*) FROM daily_options_flow_misses WHERE underlying_ticker = @s";
        tmpCmd.Parameters.AddWithValue("s", inSymbol);
        var tmpScalar = await tmpCmd.ExecuteScalarAsync();
        return Convert.ToInt32(tmpScalar);
    }
}
