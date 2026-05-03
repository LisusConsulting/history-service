using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Provider-level test for <c>daily_atm_iv</c> read + write paths
/// (Wave B / PR 5 of the ATM-IV plan). Mirrors the structure of
/// <see cref="DailyOptionsFlowProviderTests"/>: applies the migration-014
/// schema (sans hypertable — vanilla testcontainer image), exercises the
/// read path's edge cases, and the write path's idempotency.
/// </summary>
public class DailyAtmIvProviderTests : IAsyncLifetime
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
        await tmpConn.ExecuteAsync("""
            CREATE TABLE IF NOT EXISTS daily_atm_iv (
              underlying_ticker VARCHAR(10) NOT NULL,
              trade_date        DATE        NOT NULL,
              atm_iv            NUMERIC(10,6),
              contract_count    INT,
              fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              PRIMARY KEY (underlying_ticker, trade_date)
            );

            CREATE TABLE IF NOT EXISTS daily_atm_iv_misses (
              underlying_ticker VARCHAR(10) NOT NULL,
              range_from        DATE        NOT NULL,
              range_to          DATE        NOT NULL,
              reason            TEXT,
              fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              PRIMARY KEY (underlying_ticker, range_from, range_to)
            );
            """);
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    // ── Read path ──────────────────────────────────────────────────────

    [Fact]
    public async Task GetRangeAsync_EmptyCache_ReturnsEmpty()
    {
        var tmpProvider = new DailyAtmIvProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvProvider>.Instance);

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 31));

        tmpRows.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetRangeAsync_FiltersAndOrdersAscending()
    {
        await SeedAsync(
            ("TSLA", new DateOnly(2023, 12, 28), 0.55m, 30),
            ("TSLA", new DateOnly(2024, 1, 2),   0.62m, 35),
            ("TSLA", new DateOnly(2024, 1, 3),   0.58m, 32),
            ("TSLA", new DateOnly(2024, 1, 8),   0.70m, 40),
            ("AAPL", new DateOnly(2024, 1, 3),   0.30m, 50)); // different symbol

        var tmpProvider = new DailyAtmIvProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvProvider>.Instance);

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 5));

        tmpRows.Count.ShouldBe(2);
        tmpRows[0].UnderlyingTicker.ShouldBe("TSLA");
        tmpRows[0].TradeDate.ShouldBe(new DateOnly(2024, 1, 2));
        tmpRows[0].AtmIv.ShouldBe(0.62m);
        tmpRows[0].ContractCount.ShouldBe(35);
        tmpRows[1].TradeDate.ShouldBe(new DateOnly(2024, 1, 3));
    }

    [Fact]
    public async Task GetRangeAsync_NullValues_RoundTripAsNull()
    {
        // Aggregator may write NULL when every contributing snapshot has
        // NULL IV. Verify the read maps NULL → C# null cleanly on both
        // columns.
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync("""
            INSERT INTO daily_atm_iv (underlying_ticker, trade_date, atm_iv, contract_count)
            VALUES ('TSLA', '2024-01-15', NULL, NULL)
            """);

        var tmpProvider = new DailyAtmIvProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvProvider>.Instance);

        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 15), new DateOnly(2024, 1, 15));

        tmpRows.Count.ShouldBe(1);
        tmpRows[0].AtmIv.ShouldBeNull();
        tmpRows[0].ContractCount.ShouldBeNull();
    }

    [Fact]
    public async Task GetRangeAsync_InvertedRange_ReturnsEmpty()
    {
        await SeedAsync(("TSLA", new DateOnly(2024, 1, 2), 0.5m, 10));
        var tmpProvider = new DailyAtmIvProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvProvider>.Instance);
        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 31), new DateOnly(2024, 1, 1));
        tmpRows.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetRangeAsync_EmptySymbol_ReturnsEmpty()
    {
        var tmpProvider = new DailyAtmIvProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvProvider>.Instance);
        var tmpRows = await tmpProvider.GetRangeAsync(
            "", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 5));
        tmpRows.ShouldBeEmpty();
    }

    // ── Write path ─────────────────────────────────────────────────────

    [Fact]
    public async Task UpsertAsync_InsertsThenUpdatesIdempotently()
    {
        var tmpProvider = new DailyAtmIvProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvProvider>.Instance);

        var tmpFirst = new[]
        {
            new DailyAtmIvRow("TSLA", new DateOnly(2024, 1, 2), 0.55m, 30),
            new DailyAtmIvRow("TSLA", new DateOnly(2024, 1, 3), 0.60m, 32),
        };
        await tmpProvider.UpsertAsync(tmpFirst);

        // Second write with new values for the same keys overwrites.
        var tmpSecond = new[]
        {
            new DailyAtmIvRow("TSLA", new DateOnly(2024, 1, 2), 0.65m, 31),
        };
        await tmpProvider.UpsertAsync(tmpSecond);

        var tmpRead = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 5));
        tmpRead.Count.ShouldBe(2);
        var tmpUpdated = tmpRead.First(r => r.TradeDate == new DateOnly(2024, 1, 2));
        tmpUpdated.AtmIv.ShouldBe(0.65m);
        tmpUpdated.ContractCount.ShouldBe(31);
    }

    [Fact]
    public async Task UpsertAsync_NullValues_PersistAsNull()
    {
        var tmpProvider = new DailyAtmIvProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvProvider>.Instance);
        await tmpProvider.UpsertAsync(new[]
        {
            new DailyAtmIvRow("TSLA", new DateOnly(2024, 1, 2), null, null),
        });

        var tmpRead = await tmpProvider.GetRangeAsync(
            "TSLA", new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 5));
        tmpRead.Count.ShouldBe(1);
        tmpRead[0].AtmIv.ShouldBeNull();
        tmpRead[0].ContractCount.ShouldBeNull();
    }

    [Fact]
    public async Task RecordMissAsync_WritesMarkerRow()
    {
        var tmpProvider = new DailyAtmIvProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvProvider>.Instance);
        await tmpProvider.RecordMissAsync(
            "TSLA", new DateOnly(2024, 1, 2), new DateOnly(2024, 1, 5),
            "no-aggregate", CancellationToken.None);

        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        var tmpCount = await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM daily_atm_iv_misses WHERE underlying_ticker = 'TSLA'");
        tmpCount.ShouldBe(1);
    }

    private async Task SeedAsync(
        params (string Underlying, DateOnly Date, decimal? Iv, int? Count)[] inRows)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        foreach (var tmpRow in inRows)
        {
            await tmpConn.ExecuteAsync("""
                INSERT INTO daily_atm_iv
                  (underlying_ticker, trade_date, atm_iv, contract_count)
                VALUES (@U, @D::date, @I, @C)
                """, new
            {
                U = tmpRow.Underlying,
                D = tmpRow.Date.ToString("yyyy-MM-dd"),
                I = (object?)tmpRow.Iv ?? DBNull.Value,
                C = (object?)tmpRow.Count ?? DBNull.Value,
            });
        }
    }
}
