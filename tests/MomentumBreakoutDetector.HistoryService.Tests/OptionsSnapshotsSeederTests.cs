using Dapper;
using MomentumBreakoutDetector.HistoryService.Pricing.BlackScholes;
using MomentumBreakoutDetector.HistoryService.Seeder;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Integration test for the Wave B / PR 3 OptionsSnapshots seeder. Stands
/// up a Postgres testcontainer, applies the relevant migration shapes
/// (with create_hypertable stripped — vanilla postgres image), seeds a
/// tiny synthetic dataset (3 contracts, 2 days), runs the seeder end-to-end,
/// and asserts:
/// <list type="bullet">
///   <item>Per-contract snapshot rows are persisted to <c>historical_options_snapshots</c>.</item>
///   <item>Source column = <c>computed_bs</c> on every row.</item>
///   <item>The Black-Scholes solver successfully converges on liquid
///         ATM contracts (IV not null) — exercises the full
///         NBBO → bars → DGS3MO → BS pipeline through the DB.</item>
///   <item>Idempotent re-run: a second invocation rewrites the same
///         rows without duplicating.</item>
/// </list>
/// </summary>
public class OptionsSnapshotsSeederTests : IAsyncLifetime
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
        // Apply schema for the tables the seeder reads + writes.
        await tmpConn.ExecuteAsync("""
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
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bars_symbol_timeframe_timestamp
              ON historical_bars (symbol, timeframe, timestamp);

            CREATE TABLE IF NOT EXISTS historical_options_quotes (
              ticker            VARCHAR(50)  NOT NULL,
              ts                TIMESTAMPTZ  NOT NULL,
              as_of_ts          TIMESTAMPTZ,
              bid_price         DECIMAL(18,4),
              ask_price         DECIMAL(18,4),
              bid_size          INT,
              ask_size          INT,
              bid_exchange      INT,
              ask_exchange      INT,
              underlying_price  DECIMAL(18,4),
              fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_options_quotes_ticker_ts
              ON historical_options_quotes (ticker, ts);

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

            CREATE TABLE IF NOT EXISTS macro_data (
              series_id VARCHAR(20) NOT NULL,
              observation_date DATE NOT NULL,
              value DECIMAL(18,6)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_series_date
              ON macro_data (series_id, observation_date);

            CREATE TABLE IF NOT EXISTS historical_options_snapshots (
              ticker             VARCHAR(50)   NOT NULL,
              snapshot_date      TIMESTAMPTZ   NOT NULL,
              bid_price          NUMERIC(18,4),
              ask_price          NUMERIC(18,4),
              volume             BIGINT,
              open_interest      BIGINT,
              implied_volatility NUMERIC(10,6),
              delta              NUMERIC(10,6),
              gamma              NUMERIC(10,6),
              theta              NUMERIC(10,6),
              vega               NUMERIC(10,6),
              underlying_price   NUMERIC(18,4),
              source             VARCHAR(16)   NOT NULL CHECK (source IN ('polygon_live', 'computed_bs')),
              PRIMARY KEY (ticker, snapshot_date)
            );

            CREATE TABLE IF NOT EXISTS historical_options_snapshots_misses (
              ticker      VARCHAR(50)  NOT NULL,
              range_from  TIMESTAMPTZ  NOT NULL,
              range_to    TIMESTAMPTZ  NOT NULL,
              reason      TEXT,
              fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (ticker, range_from, range_to)
            );
            """);
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task Run_TinyDataset_PersistsBsRowsWithIvAndGreeks()
    {
        // Seed 2 trading days (must be Mon-Fri non-holiday). 2024-01-08
        // (Mon) and 2024-01-09 (Tue) — January 2024 has no holidays in
        // the trading-calendar's range until 1/15 (MLK Day).
        var tmpDay1 = new DateOnly(2024, 1, 8);
        var tmpDay2 = new DateOnly(2024, 1, 9);
        var tmpExpiry = new DateOnly(2024, 1, 19); // ~9 trading days out

        // Underlying daily bars.
        await SeedBarsAsync(tmpDay1, inClose: 200m);
        await SeedBarsAsync(tmpDay2, inClose: 205m);

        // DGS3MO at the start of the window (caller's at-or-before lookup
        // picks this).
        await SeedMacroAsync(new DateOnly(2024, 1, 5), 5.25m);

        // 3 ATM-band contracts on the 2024-01-08 chain (call ATM, call
        // slightly OTM, put ATM). Only the ATM call passes the band.
        // Pick strikes that are inside ±5% of close=200 (i.e. [190, 210]).
        await SeedContractAsync(tmpDay1, "O:TSLA240119C00200000", "call", 200m, tmpExpiry);
        await SeedContractAsync(tmpDay1, "O:TSLA240119C00205000", "call", 205m, tmpExpiry);
        await SeedContractAsync(tmpDay1, "O:TSLA240119P00200000", "put", 200m, tmpExpiry);
        // Outside the band — should be skipped.
        await SeedContractAsync(tmpDay1, "O:TSLA240119C00250000", "call", 250m, tmpExpiry);

        // Same chain on day 2.
        await SeedContractAsync(tmpDay2, "O:TSLA240119C00200000", "call", 200m, tmpExpiry);
        await SeedContractAsync(tmpDay2, "O:TSLA240119C00205000", "call", 205m, tmpExpiry);
        await SeedContractAsync(tmpDay2, "O:TSLA240119P00200000", "put", 200m, tmpExpiry);

        // NBBO before the EOD timestamp on each day, for each in-band
        // contract. Use plausible mid prices (synthetic but BS-solvable).
        var tmpEod1 = OptionsSnapshotsSeederEngine.ComputeRthCloseUtc(tmpDay1);
        var tmpEod2 = OptionsSnapshotsSeederEngine.ComputeRthCloseUtc(tmpDay2);
        await SeedNbboAsync("O:TSLA240119C00200000", tmpEod1.AddMinutes(-1), 8.5m, 8.7m);
        await SeedNbboAsync("O:TSLA240119C00205000", tmpEod1.AddMinutes(-1), 6.4m, 6.6m);
        await SeedNbboAsync("O:TSLA240119P00200000", tmpEod1.AddMinutes(-1), 7.8m, 8.0m);
        await SeedNbboAsync("O:TSLA240119C00200000", tmpEod2.AddMinutes(-1), 11.4m, 11.6m);
        await SeedNbboAsync("O:TSLA240119C00205000", tmpEod2.AddMinutes(-1), 9.0m, 9.2m);
        await SeedNbboAsync("O:TSLA240119P00200000", tmpEod2.AddMinutes(-1), 5.4m, 5.6m);

        var tmpCheckpointPath = Path.Combine(Path.GetTempPath(),
            $"checkpoint-{Guid.NewGuid():N}.json");
        try
        {
            var tmpOpts = new SeedOptions
            {
                Surface = Surface.OptionsSnapshots,
                ComputeMethod = SnapshotComputeMethod.Bs,
                Symbol = "TSLA",
                From = tmpDay1,
                To = tmpDay2,
                CheckpointFile = tmpCheckpointPath,
                StrikeBandPct = 0.05,
                SnapshotDteMaxDays = 60,
                PostgresConn = _postgres.GetConnectionString(),
            };
            var tmpCp = await Checkpoint.LoadOrCreateAsync(
                tmpCheckpointPath, tmpOpts.Symbol, Surface.OptionsSnapshots, CancellationToken.None);
            var tmpSolver = new BlackScholesSolver();
            var tmpEngine = new OptionsSnapshotsSeederEngine(
                tmpOpts, tmpCp, tmpSolver, _postgres.GetConnectionString(), inLogWriter: null);

            await tmpEngine.RunAsync(CancellationToken.None);

            // 3 in-band contracts × 2 days = 6 rows. Out-of-band contract
            // (strike=250) should be skipped.
            await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
            await tmpConn.OpenAsync();
            var tmpCount = await tmpConn.ExecuteScalarAsync<long>(
                "SELECT COUNT(*) FROM historical_options_snapshots");
            tmpCount.ShouldBe(6);

            // Source = computed_bs everywhere.
            var tmpDistinctSource = (await tmpConn.QueryAsync<string>(
                "SELECT DISTINCT source FROM historical_options_snapshots")).ToList();
            tmpDistinctSource.Count.ShouldBe(1);
            tmpDistinctSource[0].ShouldBe("computed_bs");

            // Solver should converge on every liquid contract → 0 NULL IV.
            var tmpNullIv = await tmpConn.ExecuteScalarAsync<long>(
                "SELECT COUNT(*) FROM historical_options_snapshots WHERE implied_volatility IS NULL");
            tmpNullIv.ShouldBe(0);

            // IV should be in a sane range across all rows.
            var tmpIvs = (await tmpConn.QueryAsync<decimal>(
                "SELECT implied_volatility FROM historical_options_snapshots")).ToList();
            tmpIvs.ShouldAllBe(iv => iv > 0.05m && iv < 3.0m);

            // Greeks: gamma > 0, vega > 0 (strict for non-degenerate calls/puts).
            var tmpGreeks = (await tmpConn.QueryAsync<(decimal Gamma, decimal Vega)>(
                "SELECT gamma AS Gamma, vega AS Vega FROM historical_options_snapshots")).ToList();
            tmpGreeks.ShouldAllBe(g => g.Gamma > 0m);
            tmpGreeks.ShouldAllBe(g => g.Vega > 0m);

            // Out-of-band contract should NOT have a snapshot row.
            var tmpOutOfBandCount = await tmpConn.ExecuteScalarAsync<long>(
                "SELECT COUNT(*) FROM historical_options_snapshots WHERE ticker = 'O:TSLA240119C00250000'");
            tmpOutOfBandCount.ShouldBe(0);
        }
        finally
        {
            if (File.Exists(tmpCheckpointPath)) File.Delete(tmpCheckpointPath);
        }
    }

    [Fact]
    public async Task Run_NoBars_RecordsMissMarker()
    {
        // No bars seeded → seeder should write a per-day miss marker
        // and produce zero snapshot rows.
        var tmpDay1 = new DateOnly(2024, 1, 8);
        var tmpDay2 = new DateOnly(2024, 1, 9);

        var tmpCheckpointPath = Path.Combine(Path.GetTempPath(), $"checkpoint-{Guid.NewGuid():N}.json");
        try
        {
            var tmpOpts = new SeedOptions
            {
                Surface = Surface.OptionsSnapshots,
                ComputeMethod = SnapshotComputeMethod.Bs,
                Symbol = "TSLA",
                From = tmpDay1,
                To = tmpDay2,
                CheckpointFile = tmpCheckpointPath,
                StrikeBandPct = 0.05,
                SnapshotDteMaxDays = 60,
                PostgresConn = _postgres.GetConnectionString(),
            };
            var tmpCp = await Checkpoint.LoadOrCreateAsync(
                tmpCheckpointPath, tmpOpts.Symbol, Surface.OptionsSnapshots, CancellationToken.None);
            var tmpEngine = new OptionsSnapshotsSeederEngine(
                tmpOpts, tmpCp, new BlackScholesSolver(), _postgres.GetConnectionString(), null);

            await tmpEngine.RunAsync(CancellationToken.None);

            await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
            await tmpConn.OpenAsync();
            (await tmpConn.ExecuteScalarAsync<long>(
                "SELECT COUNT(*) FROM historical_options_snapshots")).ShouldBe(0);
            (await tmpConn.ExecuteScalarAsync<long>(
                "SELECT COUNT(*) FROM historical_options_snapshots_misses")).ShouldBe(2);
        }
        finally
        {
            if (File.Exists(tmpCheckpointPath)) File.Delete(tmpCheckpointPath);
        }
    }

    [Fact]
    public async Task Run_IsIdempotent_SecondRunDoesNotDuplicate()
    {
        var tmpDay = new DateOnly(2024, 1, 8);
        var tmpExpiry = new DateOnly(2024, 1, 19);
        await SeedBarsAsync(tmpDay, 200m);
        await SeedMacroAsync(new DateOnly(2024, 1, 5), 5.25m);
        await SeedContractAsync(tmpDay, "O:TSLA240119C00200000", "call", 200m, tmpExpiry);
        var tmpEod = OptionsSnapshotsSeederEngine.ComputeRthCloseUtc(tmpDay);
        await SeedNbboAsync("O:TSLA240119C00200000", tmpEod.AddMinutes(-1), 8.5m, 8.7m);

        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();

        // First run.
        await RunOnceAsync(tmpDay, tmpDay);
        (await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_options_snapshots")).ShouldBe(1);

        // Second run on the same window — checkpoint advanced past the
        // day, so no new work happens. We pass a fresh checkpoint to
        // force a re-process; UPSERT rewrites the same row.
        await RunOnceAsync(tmpDay, tmpDay);
        (await tmpConn.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM historical_options_snapshots")).ShouldBe(1);
    }

    private async Task RunOnceAsync(DateOnly inFrom, DateOnly inTo)
    {
        var tmpCp = Path.Combine(Path.GetTempPath(), $"checkpoint-{Guid.NewGuid():N}.json");
        try
        {
            var tmpOpts = new SeedOptions
            {
                Surface = Surface.OptionsSnapshots,
                ComputeMethod = SnapshotComputeMethod.Bs,
                Symbol = "TSLA",
                From = inFrom, To = inTo,
                CheckpointFile = tmpCp,
                StrikeBandPct = 0.05,
                SnapshotDteMaxDays = 60,
                PostgresConn = _postgres.GetConnectionString(),
            };
            var tmpChk = await Checkpoint.LoadOrCreateAsync(
                tmpCp, tmpOpts.Symbol, Surface.OptionsSnapshots, CancellationToken.None);
            var tmpEngine = new OptionsSnapshotsSeederEngine(
                tmpOpts, tmpChk, new BlackScholesSolver(), _postgres.GetConnectionString(), null);
            await tmpEngine.RunAsync(CancellationToken.None);
        }
        finally
        {
            if (File.Exists(tmpCp)) File.Delete(tmpCp);
        }
    }

    private async Task SeedBarsAsync(DateOnly inDate, decimal inClose)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        var tmpTs = new DateTime(inDate.Year, inDate.Month, inDate.Day, 0, 0, 0, DateTimeKind.Utc);
        await tmpConn.ExecuteAsync("""
            INSERT INTO historical_bars
              (symbol, timeframe, timestamp, open, high, low, close, volume)
            VALUES ('TSLA', 'day', @Ts, @C, @C, @C, @C, 1000000)
            ON CONFLICT DO NOTHING
            """, new { Ts = tmpTs, C = inClose });
    }

    private async Task SeedMacroAsync(DateOnly inDate, decimal inValuePct)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync("""
            INSERT INTO macro_data (series_id, observation_date, value)
            VALUES ('DGS3MO', @D::date, @V)
            ON CONFLICT DO NOTHING
            """, new { D = inDate.ToString("yyyy-MM-dd"), V = inValuePct });
    }

    private async Task SeedContractAsync(
        DateOnly inAsOf, string inTicker, string inType, decimal inStrike, DateOnly inExpiry)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync("""
            INSERT INTO historical_options_contracts
              (as_of_date, ticker, underlying_ticker, contract_type, exercise_style,
               expiration_date, strike_price, shares_per_contract, primary_exchange)
            VALUES (@AsOf::date, @Ticker, 'TSLA', @Type, 'american',
                    @Exp::date, @Strike, 100, 'OPRA')
            ON CONFLICT DO NOTHING
            """, new
        {
            AsOf = inAsOf.ToString("yyyy-MM-dd"),
            Ticker = inTicker,
            Type = inType,
            Exp = inExpiry.ToString("yyyy-MM-dd"),
            Strike = inStrike,
        });
    }

    private async Task SeedNbboAsync(string inTicker, DateTime inTsUtc, decimal inBid, decimal inAsk)
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await tmpConn.ExecuteAsync("""
            INSERT INTO historical_options_quotes
              (ticker, ts, as_of_ts, bid_price, ask_price, bid_size, ask_size)
            VALUES (@Ticker, @Ts, @Ts, @Bid, @Ask, 10, 10)
            ON CONFLICT DO NOTHING
            """, new { Ticker = inTicker, Ts = inTsUtc, Bid = inBid, Ask = inAsk });
    }
}
