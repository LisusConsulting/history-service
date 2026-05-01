// Phase 1, micro-PR #1 — deployable shell only.
//
// Bootstraps the ASP.NET host with gRPC + reflection. Wires up the stub
// HistoryServiceImpl whose RPCs all throw Unimplemented (lifted in PRs
// #2-#7) and a real /health endpoint that proves the process is alive
// and the postgres connection string is resolvable.

using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// --- Logging --------------------------------------------------------------
// Serilog console sink. JSON formatter is overkill for Phase 1; a
// structured-text formatter is fine until we plug in a log shipper.
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();
builder.Host.UseSerilog();

// --- Configuration --------------------------------------------------------
// History:* options bound from env vars and appsettings. Connection string
// has a sensible compose-network default; CI / local dev will override.
builder.Services.Configure<HistoryServiceOptions>(
    builder.Configuration.GetSection(HistoryServiceOptions.SectionName));

// --- Providers / fetchers (micro-PR #3 — NBBO quotes) --------------------
// In-memory NBBO cache + Polygon HTTP fetcher are singletons (process-wide
// concurrency cap + connection pooling). The provider is scoped so each
// gRPC call gets a fresh NpgsqlConnection.
builder.Services.AddSingleton<NbboMemoryCache>();
// Named HttpClient so the IHttpClientFactory infra (handler pooling /
// rotation) is in play, while we keep the fetcher as a true process-wide
// singleton — its SemaphoreSlim must not be re-created per call.
builder.Services.AddHttpClient(nameof(PolygonNbboFetcher));
builder.Services.AddSingleton<IPolygonNbboFetcher>(sp =>
{
    var httpFactory = sp.GetRequiredService<IHttpClientFactory>();
    var http = httpFactory.CreateClient(nameof(PolygonNbboFetcher));
    var opts = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<HistoryServiceOptions>>();
    var logger = sp.GetRequiredService<ILogger<PolygonNbboFetcher>>();
    return new PolygonNbboFetcher(http, opts, logger);
});
builder.Services.AddScoped<IOptionQuotesProvider, OptionQuotesProvider>();

// --- FRED / Macro (micro-PR #5) ------------------------------------------
// Named HttpClient for FredFetcher so it picks up DI'd handlers + lifetime.
builder.Services.AddHttpClient(FredFetcher.HttpClientName, c =>
{
    c.Timeout = TimeSpan.FromMilliseconds(FredFetcher.DefaultPerCallTimeoutMs * 2);
});
builder.Services.AddSingleton<IFredFetcher>(sp =>
{
    var logger = sp.GetRequiredService<ILogger<FredFetcher>>();
    var httpClientFactory = sp.GetRequiredService<IHttpClientFactory>();
    var opts = sp.GetRequiredService<IOptions<HistoryServiceOptions>>().Value;
    // Resolution order: History:FredApiKey config (env-var overridable as
    // History__FredApiKey) → process-level FRED_API_KEY env var (handled
    // by the fetcher's own fallback). FRED key is optional at startup —
    // calls fail-quiet with a Warning if it's missing.
    return new FredFetcher(
        logger: logger,
        httpClientFactory: httpClientFactory,
        apiKey: string.IsNullOrWhiteSpace(opts.FredApiKey) ? null : opts.FredApiKey);
});
builder.Services.AddScoped<IMacroDataProvider>(sp =>
{
    var logger = sp.GetRequiredService<ILogger<MacroDataProvider>>();
    var opts = sp.GetRequiredService<IOptions<HistoryServiceOptions>>().Value;
    var fred = sp.GetRequiredService<IFredFetcher>();
    return new MacroDataProvider(opts.ConnectionString, logger, fred);
});

// --- gRPC -----------------------------------------------------------------
builder.Services.AddGrpc(options =>
{
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});
builder.Services.AddGrpcReflection();

// --- App ------------------------------------------------------------------
var app = builder.Build();

// Map the gRPC stub (every RPC throws Unimplemented).
app.MapGrpcService<HistoryServiceImpl>();

// Reflection so `grpcurl -plaintext localhost:30005 list` works without
// hand-feeding a .proto file.
if (app.Environment.IsDevelopment())
{
    app.MapGrpcReflectionService();
}

// Plain-HTTP health endpoint for the docker healthcheck and quick curl
// probes. Returns version info so we can confirm the container picked up
// the right git sha.
app.MapGet("/health", (IServiceProvider sp) =>
{
    var opts = sp.GetService<Microsoft.Extensions.Options.IOptions<HistoryServiceOptions>>()?.Value
               ?? new HistoryServiceOptions();
    return Results.Ok(new
    {
        status = "ok",
        service = "mbd-history",
        phase = 1,
        microPr = 1,
        gitCommit = opts.GitCommit,
        gitBranch = opts.GitBranch,
        buildTime = opts.BuildTime,
        serverTimeUtc = DateTime.UtcNow.ToString("O"),
        // Phase 1 doesn't actually connect to postgres yet — that lands in
        // PR #2 with the first provider lift. We just echo back the
        // configured connection string host for sanity (no creds).
        configuredDbHost = ExtractDbHost(opts.ConnectionString),
    });
});

app.MapGet("/", () => Results.Ok(new
{
    service = "mbd-history",
    docs = "see README.md and Protos/history.proto",
    health = "/health",
    grpc = "every RPC currently returns Unimplemented (Phase 1, micro-PR #1)"
}));

Log.Information("mbd-history starting on {Urls}", string.Join(", ", app.Urls));
app.Run();

static string ExtractDbHost(string? connStr)
{
    if (string.IsNullOrWhiteSpace(connStr)) return "(unset)";
    foreach (var part in connStr.Split(';', StringSplitOptions.RemoveEmptyEntries))
    {
        var kv = part.Split('=', 2);
        if (kv.Length == 2 && kv[0].Trim().Equals("Host", StringComparison.OrdinalIgnoreCase))
        {
            return kv[1].Trim();
        }
    }
    return "(no Host= in connection string)";
}
