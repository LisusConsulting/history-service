using Dapper;
using Npgsql;
using Shouldly;
using Testcontainers.PostgreSql;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Schema-presence verification for migrations 013 and 014 — Wave A / PR 1
/// of the ATM-IV full historical coverage plan
/// (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md).
///
/// <para>
/// Stands up a vanilla postgres testcontainer and applies the migration
/// SQL (with the TimescaleDB <c>create_hypertable</c> calls stripped out
/// since the test image is plain postgres) — then asserts:
/// <list type="bullet">
///   <item><c>historical_options_snapshots</c> + <c>historical_options_snapshots_misses</c> exist with the columns called out in the plan.</item>
///   <item><c>daily_atm_iv</c> + <c>daily_atm_iv_misses</c> exist with the columns called out in the plan.</item>
///   <item>The <c>source</c> CHECK constraint rejects values outside <c>('polygon_live','computed_bs')</c>.</item>
///   <item>Primary keys are <c>(ticker, snapshot_date)</c> + <c>(underlying_ticker, trade_date)</c> respectively.</item>
/// </list>
/// </para>
///
/// <para>
/// We do NOT exercise the hypertable behaviour here — that requires the
/// timescaledb extension which the rest of the test suite chooses not
/// to bring up (per <see cref="DailyOptionsFlowProviderTests"/>'s
/// "plain table for query semantics" comment). Hypertable correctness is
/// validated in the dev/paper deployment apply step and by the
/// idempotency of <c>create_hypertable(if_not_exists =&gt; TRUE)</c>.
/// </para>
/// </summary>
public class AtmIvSchemaPresenceTests : IAsyncLifetime
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

        // Apply migrations 013 + 014 against the testcontainer. The
        // migrations call create_hypertable() which the vanilla postgres
        // image doesn't have — strip those statements before applying.
        // The remaining DDL (CREATE TABLE / CREATE INDEX) is what the
        // schema-presence assertions need.
        await ApplyMigrationAsync("013-historical-options-snapshots.sql");
        await ApplyMigrationAsync("014-daily-atm-iv.sql");
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task Migration013_HistoricalOptionsSnapshots_HasExpectedColumns()
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();

        var tmpColumns = (await tmpConn.QueryAsync<(string ColumnName, string DataType, string IsNullable)>(
            """
            SELECT column_name AS ColumnName, data_type AS DataType, is_nullable AS IsNullable
            FROM information_schema.columns
            WHERE table_name = 'historical_options_snapshots'
              AND table_schema = current_schema()
            ORDER BY ordinal_position
            """)).ToList();

        var tmpByName = tmpColumns.ToDictionary(c => c.ColumnName, c => c, StringComparer.OrdinalIgnoreCase);

        // Plan-locked column set (PR 1 brief). Spot-check shape rather
        // than every column type — the migration SQL is the source of
        // truth, this test guards against accidental column drops.
        tmpByName.ShouldContainKey("ticker");
        tmpByName.ShouldContainKey("snapshot_date");
        tmpByName.ShouldContainKey("bid_price");
        tmpByName.ShouldContainKey("ask_price");
        tmpByName.ShouldContainKey("volume");
        tmpByName.ShouldContainKey("open_interest");
        tmpByName.ShouldContainKey("implied_volatility");
        tmpByName.ShouldContainKey("delta");
        tmpByName.ShouldContainKey("gamma");
        tmpByName.ShouldContainKey("theta");
        tmpByName.ShouldContainKey("vega");
        tmpByName.ShouldContainKey("underlying_price");
        tmpByName.ShouldContainKey("source");

        // ticker + snapshot_date + source are NOT NULL per the plan.
        tmpByName["ticker"].IsNullable.ShouldBe("NO");
        tmpByName["snapshot_date"].IsNullable.ShouldBe("NO");
        tmpByName["source"].IsNullable.ShouldBe("NO");
        // Greeks/IV/bid/ask are nullable (solver failure → NULL).
        tmpByName["implied_volatility"].IsNullable.ShouldBe("YES");
        tmpByName["delta"].IsNullable.ShouldBe("YES");
        tmpByName["gamma"].IsNullable.ShouldBe("YES");
        tmpByName["theta"].IsNullable.ShouldBe("YES");
        tmpByName["vega"].IsNullable.ShouldBe("YES");
    }

    [Fact]
    public async Task Migration013_PrimaryKey_IsTickerSnapshotDate()
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();

        var tmpPk = (await tmpConn.QueryAsync<string>(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'historical_options_snapshots'::regclass
              AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            """)).ToList();

        tmpPk.ShouldBe(new[] { "ticker", "snapshot_date" });
    }

    [Fact]
    public async Task Migration013_SourceCheckConstraint_RejectsInvalidValues()
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();

        // Valid sources insert OK.
        await tmpConn.ExecuteAsync(
            """
            INSERT INTO historical_options_snapshots
              (ticker, snapshot_date, source)
            VALUES ('O:TSLA240101C00100000', '2024-01-01 16:00:00+00', 'polygon_live'),
                   ('O:TSLA240101C00100001', '2024-01-01 16:00:00+00', 'computed_bs')
            """);

        // Invalid source rejected by CHECK.
        var tmpEx = await Should.ThrowAsync<PostgresException>(async () =>
        {
            await tmpConn.ExecuteAsync(
                """
                INSERT INTO historical_options_snapshots
                  (ticker, snapshot_date, source)
                VALUES ('O:TSLA240101C00100002', '2024-01-01 16:00:00+00', 'unknown_source')
                """);
        });

        // Postgres CHECK violation is sqlstate 23514. Both the SQLSTATE
        // and the constraint name are stable identifiers.
        tmpEx.SqlState.ShouldBe("23514");
    }

    [Fact]
    public async Task Migration013_Misses_TableShape()
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();

        var tmpColumns = (await tmpConn.QueryAsync<string>(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'historical_options_snapshots_misses'
              AND table_schema = current_schema()
            """)).ToHashSet(StringComparer.OrdinalIgnoreCase);

        tmpColumns.ShouldContain("ticker");
        tmpColumns.ShouldContain("range_from");
        tmpColumns.ShouldContain("range_to");
        tmpColumns.ShouldContain("reason");
        tmpColumns.ShouldContain("fetched_at");
    }

    [Fact]
    public async Task Migration014_DailyAtmIv_HasExpectedColumns()
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();

        var tmpColumns = (await tmpConn.QueryAsync<(string ColumnName, string IsNullable)>(
            """
            SELECT column_name AS ColumnName, is_nullable AS IsNullable
            FROM information_schema.columns
            WHERE table_name = 'daily_atm_iv'
              AND table_schema = current_schema()
            """)).ToDictionary(c => c.ColumnName, c => c, StringComparer.OrdinalIgnoreCase);

        tmpColumns.ShouldContainKey("underlying_ticker");
        tmpColumns.ShouldContainKey("trade_date");
        tmpColumns.ShouldContainKey("atm_iv");
        tmpColumns.ShouldContainKey("contract_count");
        tmpColumns.ShouldContainKey("fetched_at");

        tmpColumns["underlying_ticker"].IsNullable.ShouldBe("NO");
        tmpColumns["trade_date"].IsNullable.ShouldBe("NO");
        // atm_iv and contract_count nullable — empty days produce a row
        // with NULL aggregates (per plan Step 3 Concern G).
        tmpColumns["atm_iv"].IsNullable.ShouldBe("YES");
        tmpColumns["contract_count"].IsNullable.ShouldBe("YES");
    }

    [Fact]
    public async Task Migration014_PrimaryKey_IsUnderlyingTradeDate()
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();

        var tmpPk = (await tmpConn.QueryAsync<string>(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'daily_atm_iv'::regclass
              AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            """)).ToList();

        tmpPk.ShouldBe(new[] { "underlying_ticker", "trade_date" });
    }

    [Fact]
    public async Task Migration014_Misses_TableShape()
    {
        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();

        var tmpColumns = (await tmpConn.QueryAsync<string>(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'daily_atm_iv_misses'
              AND table_schema = current_schema()
            """)).ToHashSet(StringComparer.OrdinalIgnoreCase);

        tmpColumns.ShouldContain("underlying_ticker");
        tmpColumns.ShouldContain("range_from");
        tmpColumns.ShouldContain("range_to");
        tmpColumns.ShouldContain("reason");
        tmpColumns.ShouldContain("fetched_at");
    }

    [Fact]
    public void MacroProvider_KnownSeriesCadence_IncludesDgs3moAsDaily()
    {
        // Wave A / PR 1 also extends MacroDataProvider.KnownSeriesCadence to
        // include DGS3MO (3-month T-bill) as the risk-free-rate input
        // for the Black-Scholes solver added in PR 2. FRED publishes
        // DGS3MO Mon-Fri, so Daily cadence is the correct gap-detection
        // model.
        Providers.MacroDataProvider.KnownSeriesCadence
            .ShouldContainKey("DGS3MO");
        Providers.MacroDataProvider.KnownSeriesCadence["DGS3MO"]
            .ShouldBe(Providers.FredSeriesCadence.Daily);
    }

    /// <summary>
    /// Apply a migration .sql file, stripping the lines that call
    /// TimescaleDB's <c>create_hypertable</c> (the testcontainer's
    /// vanilla postgres image lacks the extension). The remaining DDL
    /// is independent of the hypertable status and captures everything
    /// the schema-presence assertions need.
    /// </summary>
    private async Task ApplyMigrationAsync(string inMigrationFileName)
    {
        var tmpRepoRoot = LocateRepoRoot();
        var tmpPath = Path.Combine(tmpRepoRoot, "tools", "migrations", inMigrationFileName);
        var tmpSql = await File.ReadAllTextAsync(tmpPath);

        // Strip create_hypertable(...) statements — they span SELECT
        // ... ; with parens. Naive approach: cut from "SELECT create_hypertable"
        // through the next ");\n" sequence. The migrations all follow
        // the same shape so this is sufficient.
        tmpSql = StripHypertableCalls(tmpSql);

        await using var tmpConn = new NpgsqlConnection(_postgres.GetConnectionString());
        await tmpConn.OpenAsync();
        await using var tmpCmd = tmpConn.CreateCommand();
        tmpCmd.CommandText = tmpSql;
        await tmpCmd.ExecuteNonQueryAsync();
    }

    internal static string StripHypertableCalls(string inSql)
    {
        // Remove every "SELECT create_hypertable(...)" statement up to
        // and including the trailing semicolon. The migrations format
        // it across multiple lines; we look for the start marker and
        // rewind to the next ';' character.
        var tmpResult = inSql;
        const string tmpMarker = "SELECT create_hypertable";
        while (true)
        {
            var tmpIdx = tmpResult.IndexOf(tmpMarker, StringComparison.OrdinalIgnoreCase);
            if (tmpIdx < 0) break;
            var tmpEnd = tmpResult.IndexOf(';', tmpIdx);
            if (tmpEnd < 0) break;
            tmpResult = tmpResult.Remove(tmpIdx, tmpEnd - tmpIdx + 1);
        }
        return tmpResult;
    }

    /// <summary>Walk up from the assembly location until we find the repo root
    /// (the directory containing <c>tools/migrations/</c>).</summary>
    private static string LocateRepoRoot()
    {
        var tmpDir = AppContext.BaseDirectory;
        for (var i = 0; i < 10; i++)
        {
            if (Directory.Exists(Path.Combine(tmpDir, "tools", "migrations")))
            {
                return tmpDir;
            }
            var tmpParent = Directory.GetParent(tmpDir);
            if (tmpParent is null) break;
            tmpDir = tmpParent.FullName;
        }
        throw new InvalidOperationException("Could not locate repo root from " + AppContext.BaseDirectory);
    }
}
