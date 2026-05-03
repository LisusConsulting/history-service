using System.Diagnostics;
using System.Globalization;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.ImportMbdSnapshots;

// ──────────────────────────────────────────────────────────────────────
// Wave C / PR 9 — bootstrap import of MBD dev's historical
// `options_snapshots` rows into history-service's
// `historical_options_snapshots` (+ companion `historical_options_contracts`).
//
// Per the ATM-IV full historical coverage plan
// (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md):
//
//   * Source: MBD dev `options_snapshots` (~30M rows full-chain).
//   * Filter: ATM ± 5% × DTE [0, 60] days (matches the live-capture cron's
//     band + DTE window so historical/forward-going coverage is symmetric).
//     Expected kept count: ~600k rows for a 6-month live-write window.
//   * Destination: history-service `historical_options_snapshots` with
//     source='polygon_live' (these were captured by MBD's live engine
//     against Polygon /v3/snapshot/options, identical provenance to the
//     forward-going live-capture cron).
//   * Contracts table population: history-service stores strike +
//     contract_type in `historical_options_contracts` (keyed by ticker).
//     The MBD source carries those columns in-line on each snapshot row,
//     so we de-dupe to one contract row per (ticker, as_of_date) and
//     INSERT ... ON CONFLICT DO NOTHING.
//   * Idempotent: ON CONFLICT (ticker, snapshot_date) DO NOTHING on the
//     snapshots table so a re-run skips already-imported rows.
//
// Operator runbook (long-running, ~10-30 min for 600k rows):
//
//   dotnet run --project tools/import-mbd-snapshots -- \
//     --source-conn "Host=localhost;Port=15432;Database=momentum_breakout;Username=mbd_user;Password=..." \
//     --dest-conn "Host=localhost;Port=35432;Database=mbd_history;Username=mbd;Password=mbd" \
//     --underlying TSLA \
//     [--batch-size 5000] \
//     [--strike-band-pct 0.05] \
//     [--dte-max 60]
//
// Detached pattern (PowerShell):
//   Start-Process -FilePath dotnet -ArgumentList @(...) -WindowStyle Hidden -PassThru

public static class Program
{
    public static async Task<int> Main(string[] inArgs)
    {
        try
        {
            var tmpOpts = ParseArgs(inArgs);

            using var tmpCts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                Console.WriteLine("\n[import] Ctrl+C — flushing in-flight batch then stopping...");
                e.Cancel = true;
                tmpCts.Cancel();
            };

            using var tmpLoggerFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Information));
            var tmpLogger = tmpLoggerFactory.CreateLogger<ImportRunner>();

            var tmpRunner = new ImportRunner(tmpOpts, tmpLogger);
            return await tmpRunner.RunAsync(tmpCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[import] cancelled.");
            return 130;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[import] FATAL: {ex.GetType().Name}: {ex.Message}");
            Console.Error.WriteLine(ex.StackTrace);
            return 1;
        }
    }

    private static ImportOptions ParseArgs(string[] inArgs)
    {
        string? tmpSrc = null;
        string? tmpDest = null;
        string tmpUnderlying = "TSLA";
        int tmpBatch = 5000;
        decimal tmpBand = 0.05m;
        int tmpDteMax = 60;

        for (int i = 0; i < inArgs.Length; i++)
        {
            var tmpKey = inArgs[i];
            var tmpVal = i + 1 < inArgs.Length ? inArgs[i + 1] : null;
            switch (tmpKey)
            {
                case "--source-conn":      tmpSrc = Require(tmpKey, tmpVal); i++; break;
                case "--dest-conn":        tmpDest = Require(tmpKey, tmpVal); i++; break;
                case "--underlying":       tmpUnderlying = Require(tmpKey, tmpVal); i++; break;
                case "--batch-size":       tmpBatch = int.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
                case "--strike-band-pct":  tmpBand = decimal.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
                case "--dte-max":          tmpDteMax = int.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
                case "-h":
                case "--help":
                    PrintUsage();
                    Environment.Exit(0);
                    break;
                default:
                    throw new ArgumentException($"unknown argument: {tmpKey}");
            }
        }

        if (string.IsNullOrWhiteSpace(tmpSrc))
            throw new ArgumentException("--source-conn is required (MBD dev postgres connection string)");
        if (string.IsNullOrWhiteSpace(tmpDest))
            throw new ArgumentException("--dest-conn is required (history-service postgres connection string)");

        return new ImportOptions(tmpSrc!, tmpDest!, tmpUnderlying, tmpBatch, tmpBand, tmpDteMax);
    }

    private static string Require(string inKey, string? inVal)
        => inVal ?? throw new ArgumentException($"{inKey} requires a value");

    private static void PrintUsage()
    {
        Console.WriteLine(
            """
            Usage:
              dotnet run --project tools/import-mbd-snapshots -- \
                --source-conn "Host=localhost;Port=15432;Database=momentum_breakout;Username=mbd_user;Password=..." \
                --dest-conn   "Host=localhost;Port=35432;Database=mbd_history;Username=mbd;Password=mbd" \
                --underlying TSLA \
                [--batch-size 5000] \
                [--strike-band-pct 0.05] \
                [--dte-max 60]
            """);
    }
}

public sealed record ImportOptions(
    string SourceConn,
    string DestConn,
    string Underlying,
    int BatchSize,
    decimal StrikeBandPct,
    int DteMaxDays);

/// <summary>Streaming row read from MBD source `options_snapshots`.</summary>
public sealed record SourceSnapshotRow(
    string Ticker,
    DateTime SnapshotDate,
    string? UnderlyingTicker,
    string? ContractType,
    decimal? StrikePrice,
    DateTime? ExpirationDate,
    decimal? BidPrice,
    decimal? AskPrice,
    long? Volume,
    int? OpenInterest,
    decimal? ImpliedVolatility,
    decimal? Delta,
    decimal? Gamma,
    decimal? Theta,
    decimal? Vega,
    decimal? UnderlyingPrice);

public sealed class ImportRunner
{
    private readonly ImportOptions m_Opts;
    private readonly ILogger m_Logger;

    public ImportRunner(ImportOptions inOpts, ILogger inLogger)
    {
        m_Opts = inOpts;
        m_Logger = inLogger;
    }

    public async Task<int> RunAsync(CancellationToken inCt)
    {
        m_Logger.LogInformation(
            "import: src={Src} → dest={Dest} underlying={Under} band=±{Band:P0} dte-max={Dte} batch={Batch}",
            ExtractDbHost(m_Opts.SourceConn), ExtractDbHost(m_Opts.DestConn),
            m_Opts.Underlying, m_Opts.StrikeBandPct, m_Opts.DteMaxDays, m_Opts.BatchSize);

        var tmpSw = Stopwatch.StartNew();
        long tmpReadCount = 0;
        long tmpKeptCount = 0;
        long tmpDroppedCount = 0;
        long tmpInsertedSnapshots = 0;
        long tmpInsertedContracts = 0;

        await using var tmpDestConn = new NpgsqlConnection(m_Opts.DestConn);
        await tmpDestConn.OpenAsync(inCt).ConfigureAwait(false);

        await using var tmpSrcConn = new NpgsqlConnection(m_Opts.SourceConn);
        await tmpSrcConn.OpenAsync(inCt).ConfigureAwait(false);

        // Stream-read source rows ordered by snapshot_date so resume after
        // a crash is roughly time-bounded.
        var tmpReader = await tmpSrcConn.ExecuteReaderAsync(
            new CommandDefinition(SourceSelectSql,
                new { Underlying = m_Opts.Underlying },
                commandTimeout: 0, // no timeout — operator-detached, may run hours
                cancellationToken: inCt)).ConfigureAwait(false);

        var tmpBatch = new List<SourceSnapshotRow>(m_Opts.BatchSize);
        while (await tmpReader.ReadAsync(inCt).ConfigureAwait(false))
        {
            tmpReadCount++;
            var tmpRow = MapRow(tmpReader);

            if (FilterPredicate(tmpRow, m_Opts.StrikeBandPct, m_Opts.DteMaxDays))
            {
                tmpBatch.Add(tmpRow);
                tmpKeptCount++;
            }
            else
            {
                tmpDroppedCount++;
            }

            if (tmpBatch.Count >= m_Opts.BatchSize)
            {
                var (tmpS, tmpC) = await PersistBatchAsync(tmpDestConn, tmpBatch, inCt).ConfigureAwait(false);
                tmpInsertedSnapshots += tmpS;
                tmpInsertedContracts += tmpC;
                tmpBatch.Clear();

                if (tmpReadCount % 100_000 == 0)
                {
                    m_Logger.LogInformation(
                        "progress: read={Read:N0} kept={Kept:N0} dropped={Drop:N0} ins-snap={Snap:N0} ins-contracts={Ctr:N0} elapsed={Elapsed}",
                        tmpReadCount, tmpKeptCount, tmpDroppedCount, tmpInsertedSnapshots, tmpInsertedContracts, tmpSw.Elapsed);
                }
            }
        }

        if (tmpBatch.Count > 0)
        {
            var (tmpS, tmpC) = await PersistBatchAsync(tmpDestConn, tmpBatch, inCt).ConfigureAwait(false);
            tmpInsertedSnapshots += tmpS;
            tmpInsertedContracts += tmpC;
        }

        tmpSw.Stop();
        m_Logger.LogInformation(
            "DONE: read={Read:N0} kept={Kept:N0} dropped={Drop:N0} new-snapshots={Snap:N0} new-contracts={Ctr:N0} wall={Elapsed}",
            tmpReadCount, tmpKeptCount, tmpDroppedCount, tmpInsertedSnapshots, tmpInsertedContracts, tmpSw.Elapsed);

        return 0;
    }

    /// <summary>
    /// Per-row filter — ATM ± Band of underlying_price × DTE [0, MaxDte].
    /// Skips rows with NULL underlying_price, NULL strike_price, or
    /// NULL expiration_date because the band/DTE math is undefined.
    /// Returns true to keep, false to drop.
    /// </summary>
    internal static bool FilterPredicate(
        SourceSnapshotRow inRow, decimal inStrikeBandPct, int inDteMaxDays)
    {
        if (inRow.UnderlyingPrice is null or <= 0m) return false;
        if (inRow.StrikePrice is null or <= 0m) return false;
        if (inRow.ExpirationDate is null) return false;

        var tmpBand = Math.Abs(
            (inRow.StrikePrice.Value - inRow.UnderlyingPrice.Value) / inRow.UnderlyingPrice.Value);
        if (tmpBand > inStrikeBandPct) return false;

        var tmpAsOf = DateOnly.FromDateTime(inRow.SnapshotDate);
        var tmpExp = DateOnly.FromDateTime(inRow.ExpirationDate.Value);
        var tmpDte = tmpExp.DayNumber - tmpAsOf.DayNumber;
        if (tmpDte < 0 || tmpDte > inDteMaxDays) return false;

        return true;
    }

    private async Task<(long InsertedSnapshots, long InsertedContracts)> PersistBatchAsync(
        NpgsqlConnection inDestConn, IReadOnlyList<SourceSnapshotRow> inBatch, CancellationToken inCt)
    {
        // Phase 1 — INSERT distinct contracts (one row per ticker per
        // as_of_date present in the batch). The contracts table is keyed
        // (as_of_date, ticker) so duplicates within a batch collapse
        // server-side via the ON CONFLICT clause.
        var tmpDistinctContracts = inBatch
            .Where(r => !string.IsNullOrEmpty(r.UnderlyingTicker))
            .Select(r => new
            {
                Ticker = r.Ticker,
                Underlying = r.UnderlyingTicker!,
                AsOf = DateOnly.FromDateTime(r.SnapshotDate),
                Strike = (object?)r.StrikePrice ?? DBNull.Value,
                ContractType = r.ContractType?.ToLowerInvariant() ?? "unknown",
                Exp = (object?)(r.ExpirationDate.HasValue
                    ? DateOnly.FromDateTime(r.ExpirationDate.Value)
                    : (DateOnly?)null) ?? DBNull.Value,
            })
            .DistinctBy(c => (c.AsOf, c.Ticker))
            .ToList();

        long tmpInsertedContracts = 0;
        if (tmpDistinctContracts.Count > 0)
        {
            tmpInsertedContracts = await inDestConn.ExecuteAsync(
                new CommandDefinition(ContractInsertSql, tmpDistinctContracts,
                    commandTimeout: 600, cancellationToken: inCt)).ConfigureAwait(false);
        }

        // Phase 2 — INSERT snapshots. ON CONFLICT (ticker, snapshot_date)
        // DO NOTHING; idempotent re-run.
        var tmpSnapshotParams = inBatch.Select(r => new
        {
            Ticker = r.Ticker,
            Ts = DateTime.SpecifyKind(r.SnapshotDate, DateTimeKind.Utc),
            Bid = (object?)r.BidPrice ?? DBNull.Value,
            Ask = (object?)r.AskPrice ?? DBNull.Value,
            Vol = (object?)(r.Volume.HasValue ? (long?)r.Volume.Value : null) ?? DBNull.Value,
            OI = (object?)(r.OpenInterest.HasValue ? (long?)r.OpenInterest.Value : null) ?? DBNull.Value,
            Iv = (object?)r.ImpliedVolatility ?? DBNull.Value,
            Delta = (object?)r.Delta ?? DBNull.Value,
            Gamma = (object?)r.Gamma ?? DBNull.Value,
            Theta = (object?)r.Theta ?? DBNull.Value,
            Vega = (object?)r.Vega ?? DBNull.Value,
            Underlying = (object?)r.UnderlyingPrice ?? DBNull.Value,
        }).ToList();

        var tmpInsertedSnapshots = await inDestConn.ExecuteAsync(
            new CommandDefinition(SnapshotInsertSql, tmpSnapshotParams,
                commandTimeout: 600, cancellationToken: inCt)).ConfigureAwait(false);

        return (tmpInsertedSnapshots, tmpInsertedContracts);
    }

    private static SourceSnapshotRow MapRow(System.Data.IDataReader inRdr)
    {
        return new SourceSnapshotRow(
            Ticker: inRdr.GetString(inRdr.GetOrdinal("ticker")),
            SnapshotDate: inRdr.GetDateTime(inRdr.GetOrdinal("snapshot_date")),
            UnderlyingTicker: GetNullableString(inRdr, "underlying_ticker"),
            ContractType: GetNullableString(inRdr, "contract_type"),
            StrikePrice: GetNullableDecimal(inRdr, "strike_price"),
            ExpirationDate: GetNullableDateTime(inRdr, "expiration_date"),
            BidPrice: GetNullableDecimal(inRdr, "bid_price"),
            AskPrice: GetNullableDecimal(inRdr, "ask_price"),
            Volume: GetNullableInt64(inRdr, "volume"),
            OpenInterest: GetNullableInt32(inRdr, "open_interest"),
            ImpliedVolatility: GetNullableDecimal(inRdr, "implied_volatility"),
            Delta: GetNullableDecimal(inRdr, "delta"),
            Gamma: GetNullableDecimal(inRdr, "gamma"),
            Theta: GetNullableDecimal(inRdr, "theta"),
            Vega: GetNullableDecimal(inRdr, "vega"),
            UnderlyingPrice: GetNullableDecimal(inRdr, "underlying_price"));
    }

    private static string? GetNullableString(System.Data.IDataReader r, string n)
        => r.IsDBNull(r.GetOrdinal(n)) ? null : r.GetString(r.GetOrdinal(n));
    private static decimal? GetNullableDecimal(System.Data.IDataReader r, string n)
        => r.IsDBNull(r.GetOrdinal(n)) ? null : r.GetDecimal(r.GetOrdinal(n));
    private static DateTime? GetNullableDateTime(System.Data.IDataReader r, string n)
        => r.IsDBNull(r.GetOrdinal(n)) ? null : r.GetDateTime(r.GetOrdinal(n));
    private static long? GetNullableInt64(System.Data.IDataReader r, string n)
        => r.IsDBNull(r.GetOrdinal(n)) ? null : Convert.ToInt64(r.GetValue(r.GetOrdinal(n)));
    private static int? GetNullableInt32(System.Data.IDataReader r, string n)
        => r.IsDBNull(r.GetOrdinal(n)) ? null : Convert.ToInt32(r.GetValue(r.GetOrdinal(n)));

    private static string ExtractDbHost(string inConn)
    {
        foreach (var tmpPart in inConn.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var tmpKv = tmpPart.Split('=', 2);
            if (tmpKv.Length == 2 && tmpKv[0].Trim().Equals("Host", StringComparison.OrdinalIgnoreCase))
                return tmpKv[1].Trim();
        }
        return "(unknown)";
    }

    /// <summary>
    /// Streaming SELECT against MBD's `options_snapshots`. Filters to the
    /// requested underlying and orders by snapshot_date so resume after a
    /// crash is roughly time-bounded (operator can resume from the last
    /// fully-imported timestamp). Server-side pre-filter on
    /// strike/underlying_price/DTE is intentionally skipped: the C#
    /// FilterPredicate is the canonical filter so it can be unit-tested
    /// in isolation, and the SELECT runs a single sequential scan
    /// regardless of WHERE complexity.
    /// </summary>
    internal const string SourceSelectSql = """
        SELECT ticker, snapshot_date, underlying_ticker, contract_type,
               strike_price, expiration_date, bid_price, ask_price,
               last_price, volume, open_interest, implied_volatility,
               delta, gamma, theta, vega, underlying_price
        FROM options_snapshots
        WHERE underlying_ticker = @Underlying
        ORDER BY snapshot_date
        """;

    /// <summary>
    /// Destination snapshot UPSERT. ON CONFLICT (ticker, snapshot_date)
    /// DO NOTHING — re-runs are idempotent. source='polygon_live' marks
    /// these rows as captured-from-Polygon (matches the live-capture
    /// cron's writes for forward-going dates).
    /// </summary>
    internal const string SnapshotInsertSql = """
        INSERT INTO historical_options_snapshots
          (ticker, snapshot_date, bid_price, ask_price, volume, open_interest,
           implied_volatility, delta, gamma, theta, vega, underlying_price, source)
        VALUES
          (@Ticker, @Ts, @Bid, @Ask, @Vol, @OI,
           @Iv, @Delta, @Gamma, @Theta, @Vega, @Underlying, 'polygon_live')
        ON CONFLICT (ticker, snapshot_date) DO NOTHING
        """;

    /// <summary>
    /// Destination contracts UPSERT. ON CONFLICT (as_of_date, ticker)
    /// DO NOTHING — many MBD snapshots share the same (ticker, day),
    /// only one contract row per (day, ticker) is needed.
    /// </summary>
    internal const string ContractInsertSql = """
        INSERT INTO historical_options_contracts
          (ticker, underlying_ticker, as_of_date, contract_type, strike_price, expiration_date)
        VALUES (@Ticker, @Underlying, @AsOf, @ContractType, @Strike, @Exp)
        ON CONFLICT (as_of_date, ticker) DO NOTHING
        """;
}
