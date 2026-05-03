using System.Globalization;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;
using MomentumBreakoutDetector.HistoryService.Seeder;
using TreyThomasCodes.Polygon.RestClient.Extensions;
using TreyThomasCodes.Polygon.RestClient.Services;
using HistoryServiceContainer = MomentumBreakoutDetector.HistoryService.Contracts.V1.HistoryService;

// ──────────────────────────────────────────────────────────────────────
// History-service one-shot seeder.
// ──────────────────────────────────────────────────────────────────────
//
// Two surfaces (PR 2):
//   • bars (default): drives the running history-service via gRPC to
//     backfill bars / chains / macro / minute-NBBO over a date window.
//   • daily_options_flow: per-(symbol, day) aggregate of Polygon /v2/aggs
//     daily volume across short-DTE contracts, written through a direct
//     Postgres connection to the daily_options_flow table. Bypasses gRPC
//     because the write path is intentionally not exposed (consumers
//     READ via GetDailyOptionsFlow only).
//
// Idempotent: re-runs against the same checkpoint resume at the next
// trading day. Safe to abort with Ctrl+C — the in-flight day's partial
// fetch is discarded but the cache layer's UNIQUE constraints
// deduplicate any half-written rows on resume.
//
// See README.md in this directory for the full operator runbook.

try
{
    var tmpOpts = ParseArgs(args);

    using var tmpCts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        Console.WriteLine("\n[seeder] Ctrl+C received — finishing current task and saving checkpoint...");
        e.Cancel = true;
        tmpCts.Cancel();
    };

    StreamWriter? tmpLogWriter = null;
    if (!string.IsNullOrWhiteSpace(tmpOpts.LogFile))
    {
        var tmpDir = Path.GetDirectoryName(Path.GetFullPath(tmpOpts.LogFile));
        if (!string.IsNullOrEmpty(tmpDir)) Directory.CreateDirectory(tmpDir);
        tmpLogWriter = new StreamWriter(File.Open(tmpOpts.LogFile, FileMode.Append, FileAccess.Write, FileShare.Read));
    }

    try
    {
        switch (tmpOpts.Surface)
        {
            case Surface.Bars:
                return await RunBarsSurfaceAsync(tmpOpts, tmpLogWriter, tmpCts.Token);
            case Surface.DailyOptionsFlow:
                return await RunDailyOptionsFlowSurfaceAsync(tmpOpts, tmpLogWriter, tmpCts.Token);
            case Surface.OptionsSnapshots:
                return await RunOptionsSnapshotsSurfaceAsync(tmpOpts, tmpLogWriter, tmpCts.Token);
            case Surface.DailyAtmIv:
                return await RunDailyAtmIvSurfaceAsync(tmpOpts, tmpLogWriter, tmpCts.Token);
            default:
                throw new ArgumentException($"Unsupported surface: {tmpOpts.Surface}");
        }
    }
    finally
    {
        tmpLogWriter?.Dispose();
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("[seeder] cancelled — checkpoint preserved, safe to resume.");
    return 130;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[seeder] FATAL: {ex.GetType().Name}: {ex.Message}");
    Console.Error.WriteLine(ex.StackTrace);
    return 1;
}

static async Task<int> RunBarsSurfaceAsync(SeedOptions inOpts, StreamWriter? inLogWriter, CancellationToken inCt)
{
    var tmpAddress = $"http://{inOpts.HistoryGrpcHost}:{inOpts.HistoryGrpcPort}";
    Console.WriteLine($"[seeder] connecting to {tmpAddress}");

    // The history-service uses h2c (HTTP/2 cleartext) — match the
    // service's compose port mapping (30005). For h2c GrpcChannel
    // needs HttpHandler with no TLS and HTTP/2 explicitly.
    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    using var tmpChannel = GrpcChannel.ForAddress(tmpAddress, new GrpcChannelOptions
    {
        // 6h hard cap on streamed warmup. Per-call deadlines on point
        // fetches are managed inside SeedEngine.
        MaxReceiveMessageSize = 64 * 1024 * 1024,
        MaxSendMessageSize = 4 * 1024 * 1024,
    });
    var tmpClient = new HistoryServiceContainer.HistoryServiceClient(tmpChannel);

    var tmpCp = await Checkpoint.LoadOrCreateAsync(inOpts.CheckpointFile, inOpts.Symbol, Surface.Bars, inCt);
    Console.WriteLine($"[seeder] checkpoint: lastCompleted={tmpCp.LastCompletedDate?.ToString("yyyy-MM-dd") ?? "<none>"} " +
                      $"daysFetched={tmpCp.TotalDaysFetched} keysFetched={tmpCp.TotalKeysFetched}");

    var tmpEngine = new SeedEngine(tmpClient, inOpts, tmpCp, inLogWriter);
    await tmpEngine.RunAsync(inCt);
    return 0;
}

static async Task<int> RunDailyOptionsFlowSurfaceAsync(SeedOptions inOpts, StreamWriter? inLogWriter, CancellationToken inCt)
{
    var tmpConn = ResolvePostgresConnection(inOpts);
    var tmpApiKey = ResolvePolygonApiKey();
    Console.WriteLine($"[seeder] surface=daily_options_flow symbol={inOpts.Symbol} " +
                      $"db-host={ExtractDbHost(tmpConn)} polygon-key-set={!string.IsNullOrEmpty(tmpApiKey)}");

    // Build a minimal DI container for the polygon-net-client SDK. The
    // SDK registers IOptionsService + Refit + FluentValidation — it
    // requires DI. We don't use the rest of the seeder under DI; the
    // engine takes the resolved IOptionsService by ctor.
    var tmpServices = new ServiceCollection();
    tmpServices.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning).AddSimpleConsole());
    tmpServices.AddPolygonClient(o =>
    {
        o.ApiKey = tmpApiKey ?? string.Empty;
    });
    using var tmpSp = tmpServices.BuildServiceProvider();
    var tmpPolygonOptions = tmpSp.GetRequiredService<IOptionsService>();

    var tmpCp = await Checkpoint.LoadOrCreateAsync(
        inOpts.CheckpointFile, inOpts.Symbol, Surface.DailyOptionsFlow, inCt);
    Console.WriteLine($"[seeder] checkpoint: lastCompleted={tmpCp.LastCompletedDate?.ToString("yyyy-MM-dd") ?? "<none>"} " +
                      $"daysFetched={tmpCp.TotalDaysFetched} surface={tmpCp.Surface}");

    var tmpEngine = new DailyOptionsFlowSeederEngine(inOpts, tmpCp, tmpPolygonOptions, tmpConn, inLogWriter);
    await tmpEngine.RunAsync(inCt);
    return 0;
}

static async Task<int> RunOptionsSnapshotsSurfaceAsync(SeedOptions inOpts, StreamWriter? inLogWriter, CancellationToken inCt)
{
    if (inOpts.ComputeMethod != SnapshotComputeMethod.Bs)
    {
        throw new ArgumentException(
            $"--compute-method {inOpts.ComputeMethod.ToString().ToLowerInvariant()} is reserved; only 'bs' is implemented in PR 3.");
    }

    var tmpConn = ResolvePostgresConnection(inOpts);
    Console.WriteLine($"[seeder] surface=options_snapshots compute=bs symbol={inOpts.Symbol} " +
                      $"db-host={ExtractDbHost(tmpConn)} " +
                      $"strike-band=±{inOpts.StrikeBandPct:P0} dte-max={inOpts.SnapshotDteMaxDays}");

    // Solver is stateless — just `new` it. No DI container needed for a
    // one-shot CLI tool.
    var tmpSolver = new MomentumBreakoutDetector.HistoryService.Pricing.BlackScholes.BlackScholesSolver();

    var tmpCp = await Checkpoint.LoadOrCreateAsync(
        inOpts.CheckpointFile, inOpts.Symbol, Surface.OptionsSnapshots, inCt);
    Console.WriteLine($"[seeder] checkpoint: lastCompleted={tmpCp.LastCompletedDate?.ToString("yyyy-MM-dd") ?? "<none>"} " +
                      $"daysFetched={tmpCp.TotalDaysFetched} surface={tmpCp.Surface}");

    var tmpEngine = new OptionsSnapshotsSeederEngine(inOpts, tmpCp, tmpSolver, tmpConn, inLogWriter);
    await tmpEngine.RunAsync(inCt);
    return 0;
}

static async Task<int> RunDailyAtmIvSurfaceAsync(SeedOptions inOpts, StreamWriter? inLogWriter, CancellationToken inCt)
{
    var tmpConn = ResolvePostgresConnection(inOpts);
    Console.WriteLine($"[seeder] surface=daily_atm_iv symbol={inOpts.Symbol} " +
                      $"db-host={ExtractDbHost(tmpConn)} window={inOpts.From:yyyy-MM-dd}..{inOpts.To:yyyy-MM-dd}");

    var tmpCp = await Checkpoint.LoadOrCreateAsync(
        inOpts.CheckpointFile, inOpts.Symbol, Surface.DailyAtmIv, inCt);
    Console.WriteLine($"[seeder] checkpoint: lastCompleted={tmpCp.LastCompletedDate?.ToString("yyyy-MM-dd") ?? "<none>"} " +
                      $"daysFetched={tmpCp.TotalDaysFetched} surface={tmpCp.Surface}");

    var tmpEngine = DailyAtmIvSeederEngine.Create(inOpts, tmpCp, tmpConn, inLogWriter);
    await tmpEngine.RunAsync(inCt);
    return 0;
}

static string ResolvePostgresConnection(SeedOptions inOpts)
{
    if (!string.IsNullOrWhiteSpace(inOpts.PostgresConn)) return inOpts.PostgresConn!;
    var tmpEnv = Environment.GetEnvironmentVariable("HISTORY__CONNECTIONSTRING");
    if (!string.IsNullOrWhiteSpace(tmpEnv)) return tmpEnv;
    // Default for the local-dev mbd-history-postgres on host port 35432.
    // Match init.sql's username/password (mbd/mbd, db=mbd_history).
    return "Host=localhost;Port=35432;Database=mbd_history;Username=mbd;Password=mbd";
}

static string? ResolvePolygonApiKey()
{
    return Environment.GetEnvironmentVariable("HISTORY__POLYGONAPIKEY")
        ?? Environment.GetEnvironmentVariable("Polygon__ApiKey")
        ?? Environment.GetEnvironmentVariable("POLYGON_API_KEY");
}

static string ExtractDbHost(string inConn)
{
    foreach (var tmpPart in inConn.Split(';', StringSplitOptions.RemoveEmptyEntries))
    {
        var tmpKv = tmpPart.Split('=', 2);
        if (tmpKv.Length == 2 && tmpKv[0].Trim().Equals("Host", StringComparison.OrdinalIgnoreCase))
        {
            return tmpKv[1].Trim();
        }
    }
    return "(unknown)";
}

static SeedOptions ParseArgs(string[] inArgs)
{
    string? tmpSymbol = null;
    DateOnly? tmpFrom = null, tmpTo = null;
    int tmpConcurrency = 32;
    string tmpCp = "./checkpoint.json";
    string tmpHost = "localhost";
    int tmpPort = 30005;
    string? tmpLogFile = null;
    double tmpStrikeBand = 0.05;
    int tmpDte = 10;
    Surface tmpSurface = Surface.Bars;
    string? tmpPostgresConn = null;
    int tmpFlowMaxDte = 60;
    SnapshotComputeMethod tmpComputeMethod = SnapshotComputeMethod.Bs;
    int tmpSnapshotDte = 60;

    for (int i = 0; i < inArgs.Length; i++)
    {
        var tmpKey = inArgs[i];
        var tmpVal = i + 1 < inArgs.Length ? inArgs[i + 1] : null;
        switch (tmpKey)
        {
            case "--surface":            tmpSurface = ParseSurface(Require(tmpKey, tmpVal)); i++; break;
            case "--symbol":             tmpSymbol = Require(tmpKey, tmpVal); i++; break;
            case "--from":               tmpFrom = DateOnly.ParseExact(Require(tmpKey, tmpVal), "yyyy-MM-dd", CultureInfo.InvariantCulture); i++; break;
            case "--to":                 tmpTo = DateOnly.ParseExact(Require(tmpKey, tmpVal), "yyyy-MM-dd", CultureInfo.InvariantCulture); i++; break;
            case "--concurrency":        tmpConcurrency = int.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
            case "--checkpoint-file":    tmpCp = Require(tmpKey, tmpVal); i++; break;
            case "--history-grpc-host":  tmpHost = Require(tmpKey, tmpVal); i++; break;
            case "--history-grpc-port":  tmpPort = int.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
            case "--log-file":           tmpLogFile = Require(tmpKey, tmpVal); i++; break;
            case "--strike-band-pct":    tmpStrikeBand = double.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
            case "--dte-max-days":       tmpDte = int.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
            case "--postgres-conn":      tmpPostgresConn = Require(tmpKey, tmpVal); i++; break;
            case "--flow-max-dte":       tmpFlowMaxDte = int.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
            case "--compute-method":     tmpComputeMethod = ParseComputeMethod(Require(tmpKey, tmpVal)); i++; break;
            case "--snapshot-dte-max-days": tmpSnapshotDte = int.Parse(Require(tmpKey, tmpVal), CultureInfo.InvariantCulture); i++; break;
            case "-h":
            case "--help":
                PrintUsage();
                Environment.Exit(0);
                break;
            default:
                throw new ArgumentException($"unknown argument: {tmpKey}");
        }
    }

    if (tmpSymbol is null) throw new ArgumentException("--symbol is required");
    if (tmpFrom is null) throw new ArgumentException("--from is required (YYYY-MM-DD)");
    if (tmpTo is null) throw new ArgumentException("--to is required (YYYY-MM-DD)");
    if (tmpFrom > tmpTo) throw new ArgumentException("--from must be <= --to");

    return new SeedOptions
    {
        Surface = tmpSurface,
        Symbol = tmpSymbol!,
        From = tmpFrom!.Value,
        To = tmpTo!.Value,
        Concurrency = tmpConcurrency,
        CheckpointFile = tmpCp,
        HistoryGrpcHost = tmpHost,
        HistoryGrpcPort = tmpPort,
        LogFile = tmpLogFile,
        StrikeBandPct = tmpStrikeBand,
        DteMaxDays = tmpDte,
        PostgresConn = tmpPostgresConn,
        FlowMaxDte = tmpFlowMaxDte,
        ComputeMethod = tmpComputeMethod,
        SnapshotDteMaxDays = tmpSnapshotDte,
    };
}

static Surface ParseSurface(string inValue) => inValue.ToLowerInvariant() switch
{
    "bars" => Surface.Bars,
    "daily_options_flow" or "daily-options-flow" or "dailyoptionsflow" => Surface.DailyOptionsFlow,
    "options_snapshots" or "options-snapshots" or "optionssnapshots" => Surface.OptionsSnapshots,
    "daily_atm_iv" or "daily-atm-iv" or "dailyatmiv" => Surface.DailyAtmIv,
    _ => throw new ArgumentException(
        $"unknown --surface value: {inValue} (expected: bars | daily_options_flow | options_snapshots | daily_atm_iv)"),
};

static SnapshotComputeMethod ParseComputeMethod(string inValue) => inValue.ToLowerInvariant() switch
{
    "bs" or "black-scholes" or "black_scholes" => SnapshotComputeMethod.Bs,
    "polygon" => SnapshotComputeMethod.Polygon,
    _ => throw new ArgumentException(
        $"unknown --compute-method value: {inValue} (expected: bs | polygon)"),
};

static string Require(string inKey, string? inVal)
    => inVal ?? throw new ArgumentException($"{inKey} requires a value");

static void PrintUsage()
{
    Console.WriteLine(
        """
        Usage:
          # Bars surface (default — drives gRPC):
          dotnet run --project tools/seed/MomentumBreakoutDetector.HistoryService.Seeder -- \
            --symbol TSLA \
            --from 2025-11-02 \
            --to   2026-05-02 \
            --concurrency 32 \
            --checkpoint-file ./checkpoint.tsla.json \
            --history-grpc-host localhost \
            --history-grpc-port 30005 \
            [--log-file ./seed.log] \
            [--strike-band-pct 0.05] \
            [--dte-max-days 10]

          # Daily-options-flow surface (writes via direct Postgres):
          dotnet run --project tools/seed/MomentumBreakoutDetector.HistoryService.Seeder -- \
            --surface daily_options_flow \
            --symbol TSLA \
            --from 2025-11-02 \
            --to   2026-05-02 \
            --concurrency 32 \
            --checkpoint-file ./checkpoint.tsla-flow.json \
            --postgres-conn "Host=localhost;Port=35432;Database=mbd_history;Username=mbd;Password=mbd" \
            [--flow-max-dte 60] \
            [--log-file ./seed.flow.log]

          # Options-snapshots surface, Black-Scholes compute (Wave B / PR 3 of ATM-IV plan):
          dotnet run --project tools/seed/MomentumBreakoutDetector.HistoryService.Seeder -- \
            --surface options_snapshots \
            --compute-method bs \
            --symbol TSLA \
            --from 2022-08-25 \
            --to   2026-04-13 \
            --checkpoint-file ./checkpoint.tsla-snapshots-bs.json \
            --postgres-conn "Host=localhost;Port=35432;Database=mbd_history;Username=mbd;Password=mbd" \
            [--strike-band-pct 0.05] \
            [--snapshot-dte-max-days 60] \
            [--log-file ./seed.snapshots-bs.log]

        Env vars consumed by the daily-options-flow surface:
          HISTORY__CONNECTIONSTRING — fallback for --postgres-conn
          HISTORY__POLYGONAPIKEY    — Polygon API key (required)
        Env var consumed by the options-snapshots surface:
          HISTORY__CONNECTIONSTRING — fallback for --postgres-conn
          (no Polygon key — solver reads from local DB only)

        See tools/seed/README.md for the full operator runbook.
        """);
}
