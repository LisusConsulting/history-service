using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Wave C / PR 6 — integration tests for
/// <see cref="DailyAtmIvAggregator"/>. Validates the DISTINCT-ON +
/// strike-band + non-NULL-IV filter logic against a real Postgres
/// hypertable (testcontainers, vanilla pg image — no Timescale needed
/// since the aggregator is pure SQL).
/// </summary>
public class DailyAtmIvAggregatorTests : IAsyncLifetime
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
        // Minimal historical_options_snapshots schema — only the columns
        // the aggregator reads. Real prod table has more columns + a
        // hypertable wrapper, but the aggregator's SQL is column-stable.
        await tmpConn.ExecuteAsync("""
            CREATE TABLE IF NOT EXISTS historical_options_snapshots (
              ticker             VARCHAR(40) NOT NULL,
              snapshot_date      TIMESTAMPTZ NOT NULL,
              underlying_ticker  VARCHAR(10),
              strike_price       NUMERIC(12,4),
              underlying_price   NUMERIC(12,4),
              implied_volatility NUMERIC(10,6),
              source             VARCHAR(20),
              PRIMARY KEY (ticker, snapshot_date)
            );
            CREATE INDEX idx_hos_underlying_ts
              ON historical_options_snapshots(underlying_ticker, snapshot_date DESC);
            """);
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task AggregateAsync_NoRows_ReturnsNull()
    {
        var tmpAgg = new DailyAtmIvAggregator(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvAggregator>.Instance);

        var tmpResult = await tmpAgg.AggregateAsync("TSLA", new DateOnly(2024, 6, 3));

        tmpResult.ShouldBeNull();
    }

    [Fact]
    public async Task AggregateAsync_ComputesAvgIvAndContractCount_AcrossEodPerContract()
    {
        // 3 contracts on 2024-06-03 (TSLA, S=200):
        //   O:TSLA240705C00200000  EOD IV=0.50 (and an earlier intraday row at 0.40 same day → ignored by EOD pick)
        //   O:TSLA240705P00200000  EOD IV=0.60
        //   O:TSLA240705C00210000  EOD IV=0.55 (within 5% band: |210-200|/200=5%)
        // Expected: AVG = (0.50 + 0.60 + 0.55) / 3 = 0.55, count=3.
        await SeedSnapshotsAsync(
            ("O:TSLA240705C00200000", new DateTime(2024, 6, 3, 10, 0, 0, DateTimeKind.Utc), "TSLA", 200m, 200m, 0.40m),
            ("O:TSLA240705C00200000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "TSLA", 200m, 200m, 0.50m),
            ("O:TSLA240705P00200000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "TSLA", 200m, 200m, 0.60m),
            ("O:TSLA240705C00210000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "TSLA", 210m, 200m, 0.55m));

        var tmpAgg = new DailyAtmIvAggregator(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvAggregator>.Instance);

        var tmpResult = await tmpAgg.AggregateAsync("TSLA", new DateOnly(2024, 6, 3));

        tmpResult.ShouldNotBeNull();
        tmpResult!.ContractCount.ShouldBe(3);
        tmpResult.AtmIv.ShouldNotBeNull();
        Math.Round(tmpResult.AtmIv!.Value, 4).ShouldBe(0.5500m);
    }

    [Fact]
    public async Task AggregateAsync_FiltersOutOfBandStrikes()
    {
        // S=100, band = ±5%, so [95, 105] inclusive.
        // 90 → out-of-band (|−10|/100=10%) skipped.
        // 100 → in-band.
        // 110 → out-of-band skipped.
        await SeedSnapshotsAsync(
            ("O:T240705C00090000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "T", 90m, 100m, 0.30m),
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "T", 100m, 100m, 0.40m),
            ("O:T240705C00110000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "T", 110m, 100m, 0.50m));

        var tmpAgg = new DailyAtmIvAggregator(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvAggregator>.Instance);

        var tmpResult = await tmpAgg.AggregateAsync("T", new DateOnly(2024, 6, 3));

        tmpResult.ShouldNotBeNull();
        tmpResult!.ContractCount.ShouldBe(1);
        tmpResult.AtmIv.ShouldBe(0.40m);
    }

    [Fact]
    public async Task AggregateAsync_SkipsNullIvRows()
    {
        await SeedSnapshotsAsync(
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "T", 100m, 100m, null),
            ("O:T240705C00102000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "T", 102m, 100m, 0.42m));

        var tmpAgg = new DailyAtmIvAggregator(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvAggregator>.Instance);

        var tmpResult = await tmpAgg.AggregateAsync("T", new DateOnly(2024, 6, 3));

        tmpResult.ShouldNotBeNull();
        tmpResult!.ContractCount.ShouldBe(1);
        tmpResult.AtmIv.ShouldBe(0.42m);
    }

    [Fact]
    public async Task AggregateAsync_OnlyNullIv_ReturnsNull()
    {
        await SeedSnapshotsAsync(
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "T", 100m, 100m, null));

        var tmpAgg = new DailyAtmIvAggregator(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvAggregator>.Instance);

        var tmpResult = await tmpAgg.AggregateAsync("T", new DateOnly(2024, 6, 3));

        tmpResult.ShouldBeNull();
    }

    [Fact]
    public async Task AggregateAsync_DistinctOnPicksLatestSnapshotPerContract()
    {
        // Two intraday rows + one EOD row for the same contract. The
        // DISTINCT ON ... ORDER BY snapshot_date DESC must pick 19:30
        // (the EOD row).
        await SeedSnapshotsAsync(
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 14, 0, 0, DateTimeKind.Utc), "T", 100m, 100m, 0.20m),
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 18, 0, 0, DateTimeKind.Utc), "T", 100m, 100m, 0.30m),
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "T", 100m, 100m, 0.45m));

        var tmpAgg = new DailyAtmIvAggregator(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvAggregator>.Instance);

        var tmpResult = await tmpAgg.AggregateAsync("T", new DateOnly(2024, 6, 3));

        tmpResult.ShouldNotBeNull();
        tmpResult!.ContractCount.ShouldBe(1);
        // EOD row's IV = 0.45 — NOT 0.30 from the prior intraday row.
        tmpResult.AtmIv.ShouldBe(0.45m);
    }

    [Fact]
    public async Task AggregateRangeAsync_GroupsByDay()
    {
        // 3 days, each with one in-band contract.
        await SeedSnapshotsAsync(
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), "T", 100m, 100m, 0.40m),
            ("O:T240705C00100000", new DateTime(2024, 6, 4, 19, 30, 0, DateTimeKind.Utc), "T", 100m, 100m, 0.45m),
            ("O:T240705C00100000", new DateTime(2024, 6, 5, 19, 30, 0, DateTimeKind.Utc), "T", 100m, 100m, 0.50m));

        var tmpAgg = new DailyAtmIvAggregator(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvAggregator>.Instance);

        var tmpRows = await tmpAgg.AggregateRangeAsync(
            "T", new DateOnly(2024, 6, 3), new DateOnly(2024, 6, 5));

        tmpRows.Count.ShouldBe(3);
        tmpRows[0].TradeDate.ShouldBe(new DateOnly(2024, 6, 3));
        tmpRows[0].AtmIv.ShouldBe(0.40m);
        tmpRows[1].TradeDate.ShouldBe(new DateOnly(2024, 6, 4));
        tmpRows[1].AtmIv.ShouldBe(0.45m);
        tmpRows[2].TradeDate.ShouldBe(new DateOnly(2024, 6, 5));
        tmpRows[2].AtmIv.ShouldBe(0.50m);
    }

    private async Task SeedSnapshotsAsync(
        params (string Ticker, DateTime Ts, string Underlying, decimal Strike, decimal UnderlyingPrice, decimal? Iv)[] inRows)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        foreach (var tmpRow in inRows)
        {
            await tmpConn.ExecuteAsync(
                """
                INSERT INTO historical_options_snapshots
                  (ticker, snapshot_date, underlying_ticker, strike_price, underlying_price,
                   implied_volatility, source)
                VALUES (@T, @Ts, @U, @K, @S, @Iv, 'computed_bs')
                """,
                new
                {
                    T = tmpRow.Ticker,
                    Ts = tmpRow.Ts,
                    U = tmpRow.Underlying,
                    K = tmpRow.Strike,
                    S = tmpRow.UnderlyingPrice,
                    Iv = (object?)tmpRow.Iv ?? DBNull.Value,
                });
        }
    }
}
