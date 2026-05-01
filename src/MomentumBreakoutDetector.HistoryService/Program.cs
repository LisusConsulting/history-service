// Phase 1, micro-PR #1 — deployable shell.
// Phase E — refactored Polygon plumbing onto polygon-net-client SDK 0.10.0
// (pluggable handler chain + Raw ApiResponse variants). The 3 Polygon
// fetchers (bars, NBBO, chain) now consume IStocksService /
// IOptionsService instead of raw HttpClient; SemaphoreSlim concurrency +
// per-call timeout live in DelegatingHandlers layered into the SDK's
// AddPolygonClient pipeline.

using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.MessageHandlers;
using MomentumBreakoutDetector.HistoryService.Providers;
using Serilog;
using TreyThomasCodes.Polygon.RestClient.Extensions;

var builder = WebApplication.CreateBuilder(args);

// --- Logging --------------------------------------------------------------
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();
builder.Host.UseSerilog();

// --- Configuration --------------------------------------------------------
builder.Services.Configure<HistoryServiceOptions>(
    builder.Configuration.GetSection(HistoryServiceOptions.SectionName));

// --- Polygon SDK (Phase E) ------------------------------------------------
// Replaces three previously-separate IHttpClientFactory bindings (the
// "polygon" named client for bars + the typed PolygonChainFetcher /
// PolygonNbboFetcher HttpClients) with one centralized SDK registration.
//
// Pipeline order (auth handler is added by AddPolygonClient first; our
// handlers layer AFTER auth so they see authed requests + observe the
// transport response):
//   request  → auth → retry → timeout → concurrency → wire
//   response ← auth ← retry ← timeout ← concurrency ← wire
// Concurrency must be the innermost handler so the gate is held only
// while the request is actually in flight (retries inside it count
// against the same gate slot, which is fine — that's the per-fetch
// budget the original SemaphoreSlim enforced).
builder.Services.AddTransient<PolygonRetryHandler>();
builder.Services.AddTransient<PerCallTimeoutHandler>();
builder.Services.AddTransient<ConcurrencyLimitingHandler>();

builder.Services.AddPolygonClient(o =>
{
    var opts = builder.Configuration
        .GetSection(HistoryServiceOptions.SectionName)
        .Get<HistoryServiceOptions>() ?? new HistoryServiceOptions();
    o.ApiKey = opts.PolygonApiKey ?? string.Empty;
    o.BaseUrl = string.IsNullOrWhiteSpace(opts.PolygonBaseUrl)
        ? "https://api.polygon.io"
        : opts.PolygonBaseUrl;
}, b => b
    .AddHttpMessageHandler<PolygonRetryHandler>()
    .AddHttpMessageHandler<PerCallTimeoutHandler>()
    .AddHttpMessageHandler<ConcurrencyLimitingHandler>());

// --- Providers / fetchers (NBBO quotes — micro-PR #3) --------------------
builder.Services.AddSingleton<NbboMemoryCache>();
// Fetcher takes IOptionsService from the SDK; singleton because the
// fetcher itself is stateless (concurrency state lives in the handler
// chain, which is process-wide via the static gate in
// ConcurrencyLimitingHandler).
builder.Services.AddSingleton<IPolygonNbboFetcher, PolygonNbboFetcher>();
builder.Services.AddScoped<IOptionQuotesProvider, OptionQuotesProvider>();

// --- FRED / Macro (micro-PR #5) ------------------------------------------
// FRED is NOT a Polygon endpoint — it stays on the named HttpClient path.
builder.Services.AddHttpClient(FredFetcher.HttpClientName, c =>
{
    c.Timeout = TimeSpan.FromMilliseconds(FredFetcher.DefaultPerCallTimeoutMs * 2);
});
builder.Services.AddSingleton<IFredFetcher>(sp =>
{
    var logger = sp.GetRequiredService<ILogger<FredFetcher>>();
    var httpClientFactory = sp.GetRequiredService<IHttpClientFactory>();
    var opts = sp.GetRequiredService<IOptions<HistoryServiceOptions>>().Value;
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

// --- Option chains (micro-PR #4) -----------------------------------------
// PolygonOptions still binds the Polygon:* configuration section because
// existing deploys + smoke tests reference it; the SDK ignores it (its
// ApiKey/BaseUrl come from AddPolygonClient above) but the section is
// preserved for backward-compat with `Polygon__ApiKey` env var consumers.
builder.Services.Configure<PolygonOptions>(
    builder.Configuration.GetSection(PolygonOptions.SectionName));
builder.Services.AddSingleton<IPolygonChainFetcher, PolygonChainFetcher>();
builder.Services.AddScoped<IOptionChainProvider, OptionChainProvider>();

// --- gRPC -----------------------------------------------------------------
builder.Services.AddGrpc(options =>
{
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});
builder.Services.AddGrpcReflection();

// --- Bars (micro-PR #2) ---------------------------------------------------
builder.Services.AddSingleton<IPolygonBarFetcher, PolygonBarFetcher>();
builder.Services.AddScoped<IHistoricalBarsProvider, HistoricalBarsProvider>();

// --- App ------------------------------------------------------------------
var app = builder.Build();

app.MapGrpcService<HistoryServiceImpl>();

if (app.Environment.IsDevelopment())
{
    app.MapGrpcReflectionService();
}

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
        configuredDbHost = ExtractDbHost(opts.ConnectionString),
    });
});

app.MapGet("/", () => Results.Ok(new
{
    service = "mbd-history",
    docs = "see README.md and Protos/history.proto",
    health = "/health",
    grpc = "GetBars / GetNbbo / GetOptionChain / GetMacro implemented; remaining RPCs Unimplemented"
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
