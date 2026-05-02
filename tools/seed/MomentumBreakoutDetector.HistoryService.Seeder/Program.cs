using System.Globalization;
using Grpc.Net.Client;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;
using MomentumBreakoutDetector.HistoryService.Seeder;
using HistoryServiceContainer = MomentumBreakoutDetector.HistoryService.Contracts.V1.HistoryService;

// ──────────────────────────────────────────────────────────────────────
// History-service one-shot seeder.
// ──────────────────────────────────────────────────────────────────────
//
// Drives the running history-service via its gRPC client to backfill a
// symbol's bars / chains / macro / minute-NBBO over a date window.
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
        var tmpAddress = $"http://{tmpOpts.HistoryGrpcHost}:{tmpOpts.HistoryGrpcPort}";
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

        var tmpCp = await Checkpoint.LoadOrCreateAsync(tmpOpts.CheckpointFile, tmpOpts.Symbol, tmpCts.Token);
        Console.WriteLine($"[seeder] checkpoint: lastCompleted={tmpCp.LastCompletedDate?.ToString("yyyy-MM-dd") ?? "<none>"} " +
                          $"daysFetched={tmpCp.TotalDaysFetched} keysFetched={tmpCp.TotalKeysFetched}");

        var tmpEngine = new SeedEngine(tmpClient, tmpOpts, tmpCp, tmpLogWriter);
        await tmpEngine.RunAsync(tmpCts.Token);
        return 0;
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

    for (int i = 0; i < inArgs.Length; i++)
    {
        var tmpKey = inArgs[i];
        var tmpVal = i + 1 < inArgs.Length ? inArgs[i + 1] : null;
        switch (tmpKey)
        {
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
    };
}

static string Require(string inKey, string? inVal)
    => inVal ?? throw new ArgumentException($"{inKey} requires a value");

static void PrintUsage()
{
    Console.WriteLine(
        """
        Usage:
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

        See tools/seed/README.md for the full operator runbook.
        """);
}
