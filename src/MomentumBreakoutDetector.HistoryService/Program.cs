// Phase 1, micro-PR #1 — deployable shell.
// Phase E — refactored Polygon plumbing onto polygon-net-client SDK 0.10.0
// (pluggable handler chain + Raw ApiResponse variants). The 3 Polygon
// fetchers (bars, NBBO, chain) now consume IStocksService /
// IOptionsService instead of raw HttpClient; SemaphoreSlim concurrency +
// per-call timeout live in DelegatingHandlers layered into the SDK's
// AddPolygonClient pipeline.

using Alpaca.Markets;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.HostedServices;
using MomentumBreakoutDetector.HistoryService.MessageHandlers;
using MomentumBreakoutDetector.HistoryService.Observability;
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

// --- Observability (micro-PR #8) ------------------------------------------
// MetricsCollector is process-wide: counters + per-kind ring buffer for
// p50/p95/p99 + in-flight probe registry. Fetchers self-register their
// SingleFlight in-flight count at construction time; providers + fetchers
// inject the collector via constructor and call RecordCacheHit /
// RecordUpstreamFetch / RecordMissMarker on the appropriate paths. The
// gRPC GetCacheStats RPC reads a snapshot at request time.
builder.Services.AddSingleton<MetricsCollector>();

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
    var metrics = sp.GetRequiredService<MetricsCollector>();
    return new FredFetcher(
        logger: logger,
        httpClientFactory: httpClientFactory,
        apiKey: string.IsNullOrWhiteSpace(opts.FredApiKey) ? null : opts.FredApiKey,
        metrics: metrics);
});
builder.Services.AddScoped<IMacroDataProvider>(sp =>
{
    var logger = sp.GetRequiredService<ILogger<MacroDataProvider>>();
    var opts = sp.GetRequiredService<IOptions<HistoryServiceOptions>>().Value;
    var fred = sp.GetRequiredService<IFredFetcher>();
    var metrics = sp.GetRequiredService<MetricsCollector>();
    return new MacroDataProvider(opts.ConnectionString, logger, fred, metrics);
});

// --- Daily options flow (PRs 1, 2, 3, daily_options_flow surface) -------
// PR 1 added the read provider; PR 2 added the write surface (UpsertAsync
// + RecordMissAsync) used by the seeder; PR 3 adds the daily 08:00 ET
// cron that maintains the trailing edge automatically. The provider is
// scoped because it only touches Postgres and holds no per-request state
// — same scoping as the other read-mostly providers.
builder.Services.AddScoped<IDailyOptionsFlowProvider, DailyOptionsFlowProvider>();

// PR 3 — per-day computer used by both the cron and (transitively) the
// seeder via the same algorithm. Scoped because IOptionsService from the
// SDK is registered transient — taking it scoped keeps the resolution
// graph consistent.
builder.Services.AddScoped<IDailyOptionsFlowComputer>(sp =>
{
    var tmpOpts = sp.GetRequiredService<IOptions<HistoryServiceOptions>>().Value;
    var tmpPolygon = sp.GetRequiredService<TreyThomasCodes.Polygon.RestClient.Services.IOptionsService>();
    var tmpLogger = sp.GetRequiredService<ILogger<DailyOptionsFlowComputer>>();
    return new DailyOptionsFlowComputer(tmpOpts.ConnectionString, tmpPolygon, tmpLogger);
});

// PR 3 — TimeProvider injection so the cron can be unit-tested with
// FakeTimeProvider. .NET 8+ has a built-in TimeProvider.System singleton.
builder.Services.AddSingleton(TimeProvider.System);

// --- Pricing / Black-Scholes IV solver (Wave A / PR 2 of the ATM-IV
// full historical coverage plan) -----------------------------------------
// Stateless solver — pure function of inputs — registered as Singleton.
// Consumed by:
//   * PR 3 — backfill seeder for the historical_options_snapshots
//     surface (--surface options_snapshots --compute-method bs).
//   * PR 4 — live-capture cron does NOT call the solver (Polygon
//     supplies IV/greeks on /v3/snapshot/options); the solver is
//     dormant on the live path.
//   * PR 6 (Wave C) — daily aggregate cron invokes via the seeder
//     mode --surface daily_atm_iv when reaggregating computed_bs rows.
builder.Services.AddSingleton<
    MomentumBreakoutDetector.HistoryService.Pricing.BlackScholes.IBlackScholesSolver,
    MomentumBreakoutDetector.HistoryService.Pricing.BlackScholes.BlackScholesSolver>();

// PR 3 — daily refresh cron. Bind options from History:DailyFlowRefresh.
builder.Services.Configure<DailyOptionsFlowRefreshOptions>(
    builder.Configuration.GetSection(DailyOptionsFlowRefreshOptions.SectionName));
builder.Services.AddHostedService<DailyOptionsFlowRefreshService>();

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

// --- Bars (Phase 2c — Alpaca for stocks, Polygon kept for options) -------
// Stock bars route to Alpaca because Polygon's plan caps 1-min stock-bar
// history at ~2 years (the original TSLA backfill hit 800 NOT_AUTHORIZED
// misses for 2022-08-25..2025-08 dates). Alpaca's paid SIP feed has
// uncapped history. Options stay on Polygon — Alpaca's options surface
// (chains, NBBO) is incomplete.
//
// IPolygonBarFetcher is the bars-fetcher interface; the implementation
// is now AlpacaBarFetcher. The interface name is kept for now to minimize
// churn in HistoricalBarsProvider's signature; rename to a vendor-neutral
// name (IHistoricalBarsFetcher) is a follow-up cleanup.
builder.Services.AddSingleton<IAlpacaDataClient>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<HistoryServiceOptions>>().Value;
    var key = opts.AlpacaApiKey ?? string.Empty;
    var secret = opts.AlpacaApiSecret ?? string.Empty;
    // Alpaca data endpoints always use Environments.Live — the data feed
    // is independent of the paper/live trading split. Empty creds yield
    // a client that 401s on first call; the fetcher fail-quiets and
    // surfaces a warn log.
    var tmpSecretKey = new SecretKey(key, secret);
    // Fully-qualified to avoid ambiguity with Microsoft.Extensions.Hosting.Environments.
    return Alpaca.Markets.Environments.Live.GetAlpacaDataClient(tmpSecretKey);
});
builder.Services.AddSingleton<IPolygonBarFetcher>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<HistoryServiceOptions>>().Value;
    var dataClient = sp.GetRequiredService<IAlpacaDataClient>();
    var logger = sp.GetRequiredService<ILogger<AlpacaBarFetcher>>();
    var metrics = sp.GetRequiredService<MetricsCollector>();
    var feed = AlpacaBarFetcher.ParseFeed(opts.AlpacaDataFeed);
    return new AlpacaBarFetcher(dataClient, feed, logger, metrics);
});
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
