using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Provider-level test for the read path on <c>daily_options_flow</c>.
/// Stands up a Postgres testcontainer with the migration-012 schema (sans
/// hypertable — no Timescale extension on the `postgres:16-alpine` image
/// the rest of the suite uses; the table works as a plain table for
/// query semantics) and verifies:
///   - empty cache returns an empty list
///   - rows in [from, to] return ordered ascending by trade_date
///   - rows outside the range are filtered
///   - NULL put_call_ratio / flow_score round-trip cleanly
/// PR 1 is read-only; PR 2 will add a write-path test.
/// </summary>
public class DailyOptionsFlowProviderTests : IAsyncLifetime
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
        // Plain CREATE TABLE — no hypertable here; Testcontainers' postgres
        // image is vanilla, and the read SQL is hypertable-agnostic.
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
        """;
        await tmpCmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task GetRangeAsync_EmptyCache_ReturnsEmpty()
    {
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyOptionsFlowProvider>.Instance);

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 31));

        tmpRows.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetRangeAsync_ReturnsOnlyRowsInRange_OrderedAscending()
    {
        await SeedAsync(
            ("TSLA", new DateOnly(2023, 12, 28), 100_000L, 80_000L, 0L, 0L, 0.8m, 0.21m, 25),
            ("TSLA", new DateOnly(2024, 1, 2),   200_000L, 150_000L, 0L, 0L, 0.75m, 0.245m, 30),
            ("TSLA", new DateOnly(2024, 1, 3),   180_000L, 220_000L, 0L, 0L, 1.222m, -0.156m, 28),
            ("TSLA", new DateOnly(2024, 1, 8),   150_000L, 100_000L, 0L, 0L, 0.667m, 0.350m, 32),
            ("AAPL", new DateOnly(2024, 1, 3),   500_000L, 300_000L, 0L, 0L, 0.6m, 0.42m, 50)); // different symbol

        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyOptionsFlowProvider>.Instance);

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 5));

        tmpRows.Count.ShouldBe(2);
        tmpRows[0].UnderlyingTicker.ShouldBe("TSLA");
        tmpRows[0].TradeDate.ShouldBe(new DateOnly(2024, 1, 2));
        tmpRows[0].CallVolume.ShouldBe(200_000L);
        tmpRows[0].PutVolume.ShouldBe(150_000L);
        tmpRows[0].PutCallRatio.ShouldBe(0.75m);
        tmpRows[0].FlowScore.ShouldBe(0.245m);
        tmpRows[0].ContractCount.ShouldBe(30);
        tmpRows[1].TradeDate.ShouldBe(new DateOnly(2024, 1, 3));
        tmpRows[1].PutVolume.ShouldBe(220_000L);
        tmpRows[1].FlowScore.ShouldBe(-0.156m);

        // Out-of-range and other-symbol rows are filtered.
        tmpRows.ShouldNotContain(r => r.TradeDate == new DateOnly(2023, 12, 28));
        tmpRows.ShouldNotContain(r => r.TradeDate == new DateOnly(2024, 1, 8));
        tmpRows.ShouldAllBe(r => r.UnderlyingTicker == "TSLA");
    }

    [Fact]
    public async Task GetRangeAsync_NullRatioAndScore_RoundTripAsNull()
    {
        // call_volume = 0 → the formula leaves put_call_ratio + flow_score
        // NULL. Verify the read maps NULL → C# null on both columns.
        await SeedNullableAsync(
            "TSLA", new DateOnly(2024, 1, 15),
            callVolume: 0L, putVolume: 50_000L,
            callOi: 0L, putOi: 0L,
            putCallRatio: null, flowScore: null,
            contractCount: 12);

        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyOptionsFlowProvider>.Instance);

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 15), new DateOnly(2024, 1, 15));

        tmpRows.Count.ShouldBe(1);
        tmpRows[0].PutCallRatio.ShouldBeNull();
        tmpRows[0].FlowScore.ShouldBeNull();
        tmpRows[0].CallVolume.ShouldBe(0L);
        tmpRows[0].PutVolume.ShouldBe(50_000L);
    }

    [Fact]
    public async Task GetRangeAsync_InvertedRange_ReturnsEmpty()
    {
        await SeedAsync(("TSLA", new DateOnly(2024, 1, 2), 100L, 80L, 0L, 0L, 0.8m, 0.14m, 5));

        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyOptionsFlowProvider>.Instance);

        // from > to — provider returns empty without throwing.
        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 31), new DateOnly(2024, 1, 1));

        tmpRows.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetRangeAsync_EmptySymbol_ReturnsEmpty()
    {
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyOptionsFlowProvider>.Instance);

        var tmpRows = await tmpProvider.GetRangeAsync(
            "  ", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 31));

        tmpRows.ShouldBeEmpty();
    }

    private async Task SeedAsync(params (string Sym, DateOnly Date, long CallV, long PutV, long CallOi, long PutOi, decimal? Ratio, decimal? Score, int Count)[] inRows)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        foreach (var tmpR in inRows)
        {
            await using var tmpCmd = tmpConn.CreateCommand();
            tmpCmd.CommandText = """
            INSERT INTO daily_options_flow
              (underlying_ticker, trade_date, call_volume, put_volume,
               call_oi, put_oi, put_call_ratio, flow_score, contract_count)
            VALUES (@s, @d, @cv, @pv, @co, @po, @r, @sc, @cnt)
            """;
            tmpCmd.Parameters.AddWithValue("s", tmpR.Sym);
            tmpCmd.Parameters.AddWithValue("d", tmpR.Date);
            tmpCmd.Parameters.AddWithValue("cv", tmpR.CallV);
            tmpCmd.Parameters.AddWithValue("pv", tmpR.PutV);
            tmpCmd.Parameters.AddWithValue("co", tmpR.CallOi);
            tmpCmd.Parameters.AddWithValue("po", tmpR.PutOi);
            tmpCmd.Parameters.AddWithValue("r", (object?)tmpR.Ratio ?? DBNull.Value);
            tmpCmd.Parameters.AddWithValue("sc", (object?)tmpR.Score ?? DBNull.Value);
            tmpCmd.Parameters.AddWithValue("cnt", tmpR.Count);
            await tmpCmd.ExecuteNonQueryAsync();
        }
    }

    private async Task SeedNullableAsync(
        string inSym, DateOnly inDate,
        long callVolume, long putVolume, long callOi, long putOi,
        decimal? putCallRatio, decimal? flowScore, int contractCount)
    {
        await SeedAsync((inSym, inDate, callVolume, putVolume, callOi, putOi, putCallRatio, flowScore, contractCount));
    }
}
