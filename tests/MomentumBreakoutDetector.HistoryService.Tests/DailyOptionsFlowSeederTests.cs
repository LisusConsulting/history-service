using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Providers;
using MomentumBreakoutDetector.HistoryService.Seeder;
using NSubstitute;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using TreyThomasCodes.Polygon.Models.Common;
using TreyThomasCodes.Polygon.Models.Options;
using TreyThomasCodes.Polygon.RestClient.Requests.Options;
using TreyThomasCodes.Polygon.RestClient.Services;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// PR 2 — integration test for <see cref="DailyOptionsFlowSeederEngine"/>.
/// Stands up a Postgres testcontainer with the migration-012 schema
/// inline (sans Timescale; the read/write SQL is hypertable-agnostic),
/// seeds a tiny 5-contract chain over 2 trade days, stubs
/// <see cref="IOptionsService.GetBarsAsync"/> to return known per-contract
/// daily volumes, and verifies the resulting <c>daily_options_flow</c>
/// rows match the documented aggregate formula.
/// </summary>
public class DailyOptionsFlowSeederTests : IAsyncLifetime
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
        CREATE TABLE IF NOT EXISTS historical_options_contracts (
          as_of_date DATE NOT NULL,
          ticker VARCHAR(50) NOT NULL,
          underlying_ticker VARCHAR(10) NOT NULL,
          contract_type VARCHAR(10),
          exercise_style VARCHAR(20),
          expiration_date DATE,
          strike_price DECIMAL(18,4),
          shares_per_contract INT,
          primary_exchange VARCHAR(10),
          PRIMARY KEY (as_of_date, ticker)
        );
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
    public async Task RunAsync_TwoTradeDays_AggregatesAndUpserts()
    {
        // Pick two consecutive weekdays known to be trading days
        // (Tue 2024-01-02 and Wed 2024-01-03; Mon 2024-01-01 was a market
        // holiday so this is the first two consecutive trading days of 2024).
        var tmpDay1 = new DateOnly(2024, 1, 2);
        var tmpDay2 = new DateOnly(2024, 1, 3);

        // 5 contracts per day: 3 calls + 2 puts. as_of_date == trade_date
        // because that's how the seeder reads chain rows.
        await SeedChainAsync(tmpDay1);
        await SeedChainAsync(tmpDay2);

        // Stub IOptionsService.GetBarsAsync — return a deterministic daily
        // volume per contract. Volumes are picked so the math is easy to
        // verify by hand.
        var tmpStub = Substitute.For<IOptionsService>();
        tmpStub
            .GetBarsAsync(Arg.Any<GetBarsRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var tmpReq = call.Arg<GetBarsRequest>();
                var tmpVolume = VolumeForTicker(tmpReq.OptionsTicker);
                return Task.FromResult(new PolygonResponse<List<OptionBar>>
                {
                    Status = "OK",
                    Results = new List<OptionBar>
                    {
                        new() { Volume = tmpVolume, Close = 1.5m, Timestamp = 0UL },
                    },
                });
            });

        var tmpOpts = new SeedOptions
        {
            Surface = Surface.DailyOptionsFlow,
            Symbol = "TSLA",
            From = tmpDay1,
            To = tmpDay2,
            Concurrency = 4,
            CheckpointFile = Path.Combine(Path.GetTempPath(), $"seeder-test-{Guid.NewGuid():N}.json"),
            FlowMaxDte = 60,
            PostgresConn = _postgres.GetConnectionString(),
        };
        var tmpCp = new Checkpoint { Symbol = "TSLA", Surface = Surface.DailyOptionsFlow };

        var tmpEngine = new DailyOptionsFlowSeederEngine(
            tmpOpts, tmpCp, tmpStub, _postgres.GetConnectionString(), inLogWriter: null);

        await tmpEngine.RunAsync(CancellationToken.None);

        // ── Assert ──
        // call_volume = 300 + 200 + 100 = 600
        // put_volume  = 400 + 150       = 550
        // call_side   = 600 + 0.1 * 0   = 600
        // put_side    = 550 + 0.1 * 0   = 550
        // ratio       = 550 / 600       ≈ 0.9167
        // raw_score   = (1 - 0.9167) * 0.7 ≈ 0.0583
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(), NullLogger<DailyOptionsFlowProvider>.Instance);

        var tmpRows = await tmpProvider.GetRangeAsync("TSLA", tmpDay1, tmpDay2);
        tmpRows.Count.ShouldBe(2);
        foreach (var tmpRow in tmpRows)
        {
            tmpRow.UnderlyingTicker.ShouldBe("TSLA");
            tmpRow.CallVolume.ShouldBe(600L);
            tmpRow.PutVolume.ShouldBe(550L);
            tmpRow.ContractCount.ShouldBe(5);
            tmpRow.PutCallRatio.ShouldNotBeNull();
            tmpRow.PutCallRatio!.Value.ShouldBe(0.9167m, tolerance: 0.0005m);
            tmpRow.FlowScore.ShouldNotBeNull();
            tmpRow.FlowScore!.Value.ShouldBe(0.0583m, tolerance: 0.0005m);
            tmpRow.CallOi.ShouldBe(0L); // OI not in /v2/aggs response.
            tmpRow.PutOi.ShouldBe(0L);
        }

        // Both checkpoint state + day-loop progressed.
        tmpCp.LastCompletedDate.ShouldBe(tmpDay2);
        tmpCp.TotalDaysFetched.ShouldBe(2);

        // Cleanup checkpoint file.
        if (File.Exists(tmpOpts.CheckpointFile)) File.Delete(tmpOpts.CheckpointFile);
    }

    [Fact]
    public async Task RunAsync_NoChainRows_WritesMissMarker()
    {
        // No chain rows seeded for this day → seeder should write a
        // miss-marker rather than UPSERT a bogus zero-volume row.
        var tmpDay = new DateOnly(2024, 1, 2);

        var tmpStub = Substitute.For<IOptionsService>();
        // Stub never invoked because the chain-cache pre-read returns empty.

        var tmpOpts = new SeedOptions
        {
            Surface = Surface.DailyOptionsFlow,
            Symbol = "TSLA",
            From = tmpDay,
            To = tmpDay,
            Concurrency = 4,
            CheckpointFile = Path.Combine(Path.GetTempPath(), $"seeder-test-{Guid.NewGuid():N}.json"),
            FlowMaxDte = 60,
            PostgresConn = _postgres.GetConnectionString(),
        };
        var tmpCp = new Checkpoint { Symbol = "TSLA", Surface = Surface.DailyOptionsFlow };

        var tmpEngine = new DailyOptionsFlowSeederEngine(
            tmpOpts, tmpCp, tmpStub, _postgres.GetConnectionString(), inLogWriter: null);

        await tmpEngine.RunAsync(CancellationToken.None);

        // Assert: 0 flow rows + 1 miss-marker row.
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await using var tmpFlowCount = tmpConn.CreateCommand();
        tmpFlowCount.CommandText = "SELECT COUNT(*) FROM daily_options_flow WHERE underlying_ticker='TSLA'";
        var tmpFlow = Convert.ToInt32(await tmpFlowCount.ExecuteScalarAsync());
        tmpFlow.ShouldBe(0);

        await using var tmpMissCount = tmpConn.CreateCommand();
        tmpMissCount.CommandText = "SELECT COUNT(*) FROM daily_options_flow_misses WHERE underlying_ticker='TSLA'";
        var tmpMiss = Convert.ToInt32(await tmpMissCount.ExecuteScalarAsync());
        tmpMiss.ShouldBe(1);

        await tmpStub.DidNotReceive()
            .GetBarsAsync(Arg.Any<GetBarsRequest>(), Arg.Any<CancellationToken>());

        if (File.Exists(tmpOpts.CheckpointFile)) File.Delete(tmpOpts.CheckpointFile);
    }

    [Fact]
    public void ComputeFlowScore_KnownVolumes_ProducesDocumentedFormula()
    {
        // Pin the exact algorithm against numbers from the migration-012
        // documentation.
        // call_volume=600, put_volume=550 → ratio=0.9167, score=0.0583
        var tmpResult = DailyOptionsFlowSeederEngine.ComputeFlowScore(600UL, 550UL);
        tmpResult.PutCallRatio.ShouldNotBeNull();
        tmpResult.PutCallRatio!.Value.ShouldBe(0.9167m, tolerance: 0.0005m);
        tmpResult.FlowScore.ShouldNotBeNull();
        tmpResult.FlowScore!.Value.ShouldBe(0.0583m, tolerance: 0.0005m);
    }

    [Fact]
    public void ComputeFlowScore_CallSideZero_ReturnsNullRatioAndScore()
    {
        var tmpResult = DailyOptionsFlowSeederEngine.ComputeFlowScore(0UL, 50_000UL);
        tmpResult.PutCallRatio.ShouldBeNull();
        tmpResult.FlowScore.ShouldBeNull();
    }

    [Fact]
    public void ComputeFlowScore_ExtremeImbalance_ClampsToBounds()
    {
        // Heavy put-skew: ratio >> 1 → raw score < -1 → clamped to -1.
        var tmpHeavyPut = DailyOptionsFlowSeederEngine.ComputeFlowScore(100UL, 100_000UL);
        tmpHeavyPut.FlowScore.ShouldNotBeNull();
        tmpHeavyPut.FlowScore!.Value.ShouldBe(-1m);

        // Heavy call-skew: ratio = 0 → raw score = 0.7 (within bounds).
        var tmpHeavyCall = DailyOptionsFlowSeederEngine.ComputeFlowScore(100_000UL, 0UL);
        tmpHeavyCall.FlowScore.ShouldNotBeNull();
        tmpHeavyCall.FlowScore!.Value.ShouldBe(0.7m);
    }

    /// <summary>
    /// Insert 5 contracts (3 calls, 2 puts) for the given trade date.
    /// All within DTE 0..60 (expiration two weeks out).
    /// </summary>
    private async Task SeedChainAsync(DateOnly inAsOf)
    {
        var tmpExp = inAsOf.AddDays(14);
        var tmpRows = new[]
        {
            ("O:TSLA240117C00250000", "call", 250m),
            ("O:TSLA240117C00255000", "call", 255m),
            ("O:TSLA240117C00260000", "call", 260m),
            ("O:TSLA240117P00245000", "put",  245m),
            ("O:TSLA240117P00240000", "put",  240m),
        };

        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        foreach (var tmpRow in tmpRows)
        {
            await using var tmpCmd = tmpConn.CreateCommand();
            tmpCmd.CommandText = """
            INSERT INTO historical_options_contracts
              (as_of_date, ticker, underlying_ticker, contract_type, exercise_style,
               expiration_date, strike_price, shares_per_contract, primary_exchange)
            VALUES (@asof, @ticker, 'TSLA', @ctype, 'american', @exp, @strike, 100, 'BATO')
            ON CONFLICT (as_of_date, ticker) DO NOTHING
            """;
            tmpCmd.Parameters.AddWithValue("asof", inAsOf);
            tmpCmd.Parameters.AddWithValue("ticker", tmpRow.Item1);
            tmpCmd.Parameters.AddWithValue("ctype", tmpRow.Item2);
            tmpCmd.Parameters.AddWithValue("exp", tmpExp);
            tmpCmd.Parameters.AddWithValue("strike", tmpRow.Item3);
            await tmpCmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Map ticker → daily volume. Same numbers used by the assertion math
    /// in <see cref="RunAsync_TwoTradeDays_AggregatesAndUpserts"/>.
    ///   calls: 300 + 200 + 100 = 600
    ///   puts:  400 + 150       = 550
    /// </summary>
    private static ulong VolumeForTicker(string inTicker) => inTicker switch
    {
        "O:TSLA240117C00250000" => 300UL,
        "O:TSLA240117C00255000" => 200UL,
        "O:TSLA240117C00260000" => 100UL,
        "O:TSLA240117P00245000" => 400UL,
        "O:TSLA240117P00240000" => 150UL,
        _ => 0UL,
    };
}
