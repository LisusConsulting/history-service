using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using MomentumBreakoutDetector.HistoryService.HostedServices;
using MomentumBreakoutDetector.HistoryService.Providers;
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
/// PR 3 — full end-to-end integration test for the daily-flow refresh
/// cron. Stands up a Postgres testcontainer with the migration-012 +
/// historical_options_contracts schemas inline, seeds a 5-contract chain
/// for the previous trading day, stubs <see cref="IOptionsService.GetBarsAsync"/>
/// with deterministic per-contract volumes, and verifies one
/// <c>RunOnceAsync</c> cycle yields one row in <c>daily_options_flow</c>
/// matching the documented formula.
/// </summary>
public class DailyOptionsFlowRefreshIntegrationTests : IAsyncLifetime
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
    public async Task RunOnceAsync_HappyPath_WritesFlowRowForPreviousTradingDay()
    {
        // Tuesday 2026-01-06 14:00 UTC = 09:00 ET — cron has just fired
        // for the day. Previous trading day is Mon 2026-01-05 (no
        // intervening NYSE holiday).
        var tmpNowUtc = new DateTimeOffset(2026, 1, 6, 14, 0, 0, TimeSpan.Zero);
        var tmpYesterday = new DateOnly(2026, 1, 5);

        await SeedChainAsync(tmpYesterday);

        var tmpStub = Substitute.For<IOptionsService>();
        tmpStub
            .GetBarsAsync(Arg.Any<GetBarsRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var tmpReq = call.Arg<GetBarsRequest>();
                return Task.FromResult(new PolygonResponse<List<OptionBar>>
                {
                    Status = "OK",
                    Results = new List<OptionBar>
                    {
                        new() { Volume = VolumeForTicker(tmpReq.OptionsTicker), Close = 1.5m, Timestamp = 0UL },
                    },
                });
            });

        // Real computer + real provider against the test container.
        var tmpComputer = new DailyOptionsFlowComputer(
            _postgres.GetConnectionString(), tmpStub,
            NullLogger<DailyOptionsFlowComputer>.Instance);
        var tmpProvider = new DailyOptionsFlowProvider(
            _postgres.GetConnectionString(),
            NullLogger<DailyOptionsFlowProvider>.Instance);

        var tmpSvc = new DailyOptionsFlowRefreshService(
            tmpComputer, tmpProvider,
            new FakeTimeProvider(tmpNowUtc),
            NullLogger<DailyOptionsFlowRefreshService>.Instance,
            Options.Create(new DailyOptionsFlowRefreshOptions
            {
                Symbols = new List<string> { "TSLA" },
                FireHourEt = 8,
                FireMinuteEt = 0,
                MaxDte = 60,
                Concurrency = 4,
            }));

        await tmpSvc.RunOnceAsync(CancellationToken.None);

        // Assert exactly one row, for yesterday, matching the canonical
        // formula (see DailyOptionsFlowSeederTests for the same numbers).
        var tmpRows = await tmpProvider.GetRangeAsync(
            "TSLA", tmpYesterday, tmpYesterday);
        tmpRows.Count.ShouldBe(1);
        tmpRows[0].UnderlyingTicker.ShouldBe("TSLA");
        tmpRows[0].TradeDate.ShouldBe(tmpYesterday);
        tmpRows[0].CallVolume.ShouldBe(600L);
        tmpRows[0].PutVolume.ShouldBe(550L);
        tmpRows[0].ContractCount.ShouldBe(5);
        tmpRows[0].PutCallRatio.ShouldNotBeNull();
        tmpRows[0].PutCallRatio!.Value.ShouldBe(0.9167m, tolerance: 0.0005m);
        tmpRows[0].FlowScore.ShouldNotBeNull();
        tmpRows[0].FlowScore!.Value.ShouldBe(0.0583m, tolerance: 0.0005m);
    }

    private async Task SeedChainAsync(DateOnly inAsOf)
    {
        var tmpExp = inAsOf.AddDays(14);
        var tmpRows = new[]
        {
            ("O:TSLA260119C00250000", "call", 250m),
            ("O:TSLA260119C00255000", "call", 255m),
            ("O:TSLA260119C00260000", "call", 260m),
            ("O:TSLA260119P00245000", "put",  245m),
            ("O:TSLA260119P00240000", "put",  240m),
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

    private static ulong VolumeForTicker(string inTicker) => inTicker switch
    {
        "O:TSLA260119C00250000" => 300UL,
        "O:TSLA260119C00255000" => 200UL,
        "O:TSLA260119C00260000" => 100UL,
        "O:TSLA260119P00245000" => 400UL,
        "O:TSLA260119P00240000" => 150UL,
        _ => 0UL,
    };
}
