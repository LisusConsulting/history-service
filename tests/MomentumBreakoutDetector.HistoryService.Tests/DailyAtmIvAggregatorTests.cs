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
/// <see cref="DailyAtmIvAggregator"/>. Validates the JOIN +
/// DISTINCT-ON + strike-band + non-NULL-IV filter logic against a real
/// Postgres pair (snapshots + contracts) via testcontainers (vanilla
/// pg image — no Timescale needed since the aggregator is pure SQL).
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
        // Minimal schemas — only the columns the aggregator reads. Real
        // prod tables have more columns + Timescale hypertable wrappers,
        // but the aggregator's SQL is column-stable.
        // Production schema: historical_options_snapshots (migration 013)
        // does NOT have underlying_ticker or strike_price; those come
        // from historical_options_contracts (migration 003) via JOIN on
        // the option ticker.
        await tmpConn.ExecuteAsync("""
            CREATE TABLE IF NOT EXISTS historical_options_snapshots (
              ticker             VARCHAR(50) NOT NULL,
              snapshot_date      TIMESTAMPTZ NOT NULL,
              implied_volatility NUMERIC(10,6),
              underlying_price   NUMERIC(18,4),
              source             VARCHAR(20),
              PRIMARY KEY (ticker, snapshot_date)
            );
            CREATE INDEX idx_hos_ticker_ts
              ON historical_options_snapshots(ticker, snapshot_date DESC);

            CREATE TABLE IF NOT EXISTS historical_options_contracts (
              ticker            VARCHAR(50) NOT NULL,
              underlying_ticker VARCHAR(10) NOT NULL,
              as_of_date        DATE NOT NULL,
              strike_price      NUMERIC(18,4),
              contract_type     VARCHAR(10),
              expiration_date   DATE,
              PRIMARY KEY (as_of_date, ticker)
            );
            CREATE INDEX idx_hoc_ticker
              ON historical_options_contracts(ticker, underlying_ticker);
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
        await SeedContractsAsync("TSLA",
            ("O:TSLA240705C00200000", 200m),
            ("O:TSLA240705P00200000", 200m),
            ("O:TSLA240705C00210000", 210m));
        await SeedSnapshotsAsync(
            ("O:TSLA240705C00200000", new DateTime(2024, 6, 3, 10, 0, 0, DateTimeKind.Utc), 200m, 0.40m),
            ("O:TSLA240705C00200000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 200m, 0.50m),
            ("O:TSLA240705P00200000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 200m, 0.60m),
            ("O:TSLA240705C00210000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 200m, 0.55m));

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
        await SeedContractsAsync("T",
            ("O:T240705C00090000", 90m),
            ("O:T240705C00100000", 100m),
            ("O:T240705C00110000", 110m));
        await SeedSnapshotsAsync(
            ("O:T240705C00090000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 100m, 0.30m),
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 100m, 0.40m),
            ("O:T240705C00110000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 100m, 0.50m));

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
        await SeedContractsAsync("T",
            ("O:T240705C00100000", 100m),
            ("O:T240705C00102000", 102m));
        await SeedSnapshotsAsync(
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 100m, null),
            ("O:T240705C00102000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 100m, 0.42m));

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
        await SeedContractsAsync("T", ("O:T240705C00100000", 100m));
        await SeedSnapshotsAsync(
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 100m, null));

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
        // DISTINCT ON ... ORDER BY snapshot_date DESC must pick 19:30.
        await SeedContractsAsync("T", ("O:T240705C00100000", 100m));
        await SeedSnapshotsAsync(
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 14, 0, 0, DateTimeKind.Utc), 100m, 0.20m),
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 18, 0, 0, DateTimeKind.Utc), 100m, 0.30m),
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 100m, 0.45m));

        var tmpAgg = new DailyAtmIvAggregator(
            _postgres.GetConnectionString(),
            NullLogger<DailyAtmIvAggregator>.Instance);

        var tmpResult = await tmpAgg.AggregateAsync("T", new DateOnly(2024, 6, 3));

        tmpResult.ShouldNotBeNull();
        tmpResult!.ContractCount.ShouldBe(1);
        tmpResult.AtmIv.ShouldBe(0.45m);
    }

    [Fact]
    public async Task AggregateRangeAsync_GroupsByDay()
    {
        await SeedContractsAsync("T", ("O:T240705C00100000", 100m));
        await SeedSnapshotsAsync(
            ("O:T240705C00100000", new DateTime(2024, 6, 3, 19, 30, 0, DateTimeKind.Utc), 100m, 0.40m),
            ("O:T240705C00100000", new DateTime(2024, 6, 4, 19, 30, 0, DateTimeKind.Utc), 100m, 0.45m),
            ("O:T240705C00100000", new DateTime(2024, 6, 5, 19, 30, 0, DateTimeKind.Utc), 100m, 0.50m));

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

    /// <summary>
    /// Seed historical_options_contracts. Each tuple = (option ticker,
    /// strike). The contract universe is keyed on (as_of_date, ticker)
    /// in production but the aggregator only joins on (ticker,
    /// underlying_ticker) so a single placeholder as_of_date row per
    /// contract is enough to satisfy the join.
    /// </summary>
    private async Task SeedContractsAsync(
        string inUnderlying, params (string Ticker, decimal Strike)[] inRows)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        foreach (var tmpRow in inRows)
        {
            await tmpConn.ExecuteAsync(
                """
                INSERT INTO historical_options_contracts
                  (ticker, underlying_ticker, as_of_date, strike_price, contract_type, expiration_date)
                VALUES (@T, @U, @AsOf::date, @K, 'call', @Exp::date)
                ON CONFLICT (as_of_date, ticker) DO NOTHING
                """,
                new
                {
                    T = tmpRow.Ticker,
                    U = inUnderlying,
                    AsOf = "2024-06-03",
                    K = tmpRow.Strike,
                    Exp = "2024-07-05",
                });
        }
    }

    private async Task SeedSnapshotsAsync(
        params (string Ticker, DateTime Ts, decimal UnderlyingPrice, decimal? Iv)[] inRows)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        foreach (var tmpRow in inRows)
        {
            await tmpConn.ExecuteAsync(
                """
                INSERT INTO historical_options_snapshots
                  (ticker, snapshot_date, underlying_price, implied_volatility, source)
                VALUES (@T, @Ts, @S, @Iv, 'computed_bs')
                """,
                new
                {
                    T = tmpRow.Ticker,
                    Ts = tmpRow.Ts,
                    S = tmpRow.UnderlyingPrice,
                    Iv = (object?)tmpRow.Iv ?? DBNull.Value,
                });
        }
    }
}
