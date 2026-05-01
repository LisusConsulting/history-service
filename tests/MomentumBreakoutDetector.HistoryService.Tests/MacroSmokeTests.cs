using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Phase 1, micro-PR #5 smoke test. Boots a Postgres container, applies
/// the macro_data + macro_data_misses DDL, points a real
/// <see cref="MacroDataProvider"/> at it with a stubbed
/// <see cref="IFredFetcher"/>, and asserts:
///   1. T10Y2Y warmup populates rows + boundary markers (cold-start path)
///   2. A second warmup over the same window issues zero FRED calls (warm-cache path)
///
/// Comprehensive coverage (gRPC end-to-end, all four series cadences,
/// 4xx/5xx/timeout matrix) lands in micro-PR #8.
/// </summary>
public class MacroSmokeTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _pg = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("mbd_history")
        .WithUsername("mbd")
        .WithPassword("mbd")
        .Build();

    public async Task InitializeAsync()
    {
        await _pg.StartAsync();
        await ApplyDdlAsync();
    }

    public Task DisposeAsync() => _pg.DisposeAsync().AsTask();

    [Fact]
    public async Task EnsureRangeCachedAsync_T10Y2Y_WarmsCacheThenShortCircuits()
    {
        var stub = new StubFredFetcher();
        // A single weekday window so the boundary count is small + deterministic.
        // 2024-04-29 (Mon) → 2024-05-03 (Fri) = 5 weekdays. Stub returns 5 rows
        // (4 real values + 1 ".") to exercise both upsert and miss-marker writes.
        var from = new DateOnly(2024, 4, 29);
        var to = new DateOnly(2024, 5, 3);
        stub.Responses["T10Y2Y"] = new List<FredObservationRow>
        {
            new("T10Y2Y", new DateOnly(2024, 4, 29), -0.34m),
            new("T10Y2Y", new DateOnly(2024, 4, 30), -0.32m),
            new("T10Y2Y", new DateOnly(2024, 5, 1), -0.30m),
            new("T10Y2Y", new DateOnly(2024, 5, 2), null),  // FRED "." sentinel
            new("T10Y2Y", new DateOnly(2024, 5, 3), -0.28m),
        };

        var provider = new MacroDataProvider(
            _pg.GetConnectionString(),
            NullLogger<MacroDataProvider>.Instance,
            stub);

        // First call → cold-start, hits FRED once.
        await provider.EnsureRangeCachedAsync("T10Y2Y", from, to, CancellationToken.None);
        stub.CallCount.ShouldBe(1);

        var rows = await provider.GetSeriesAsync("T10Y2Y", from, to, CancellationToken.None);
        // 4 real values upserted; the null-value row is in misses, not in macro_data.
        rows.Count.ShouldBe(4);
        rows.ShouldContain(r => r.ObservationDate == new DateOnly(2024, 4, 29) && r.Value == -0.34m);
        rows.ShouldNotContain(r => r.ObservationDate == new DateOnly(2024, 5, 2));

        // Verify the null-value date got a miss-marker so the cache view is complete.
        await using var conn = new NpgsqlConnection(_pg.GetConnectionString());
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT COUNT(*) FROM macro_data_misses WHERE series_id = @sid AND observation_date = @d",
            conn);
        cmd.Parameters.AddWithValue("sid", "T10Y2Y");
        cmd.Parameters.AddWithValue("d", new DateTime(2024, 5, 2));
        var markerCount = (long)(await cmd.ExecuteScalarAsync())!;
        markerCount.ShouldBe(1L);

        // Second call → fully covered (4 rows + 1 marker == 5 weekdays). No FRED.
        await provider.EnsureRangeCachedAsync("T10Y2Y", from, to, CancellationToken.None);
        stub.CallCount.ShouldBe(1, "warm-cache path must not re-issue a FRED fetch");
    }

    private async Task ApplyDdlAsync()
    {
        // Mirror tools/migrations/004 + 005 (macro tables only).
        const string ddl = """
            CREATE TABLE IF NOT EXISTS macro_data (
              series_id VARCHAR(20) NOT NULL,
              observation_date DATE NOT NULL,
              value DECIMAL(18,6)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_series_date
              ON macro_data (series_id, observation_date);

            CREATE TABLE IF NOT EXISTS macro_data_misses (
              series_id        VARCHAR(20)  NOT NULL,
              observation_date DATE         NOT NULL,
              reason           TEXT,
              fetched_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (series_id, observation_date)
            );
            """;
        await using var conn = new NpgsqlConnection(_pg.GetConnectionString());
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(ddl, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private sealed class StubFredFetcher : IFredFetcher
    {
        public Dictionary<string, List<FredObservationRow>> Responses { get; } = new();
        public int CallCount { get; private set; }

        public Task<IReadOnlyList<FredObservationRow>> FetchSeriesAsync(
            string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            CallCount++;
            return Task.FromResult<IReadOnlyList<FredObservationRow>>(
                Responses.TryGetValue(seriesId, out var rows)
                    ? rows
                    : Array.Empty<FredObservationRow>());
        }
    }
}
