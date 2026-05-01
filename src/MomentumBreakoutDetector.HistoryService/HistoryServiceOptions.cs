namespace MomentumBreakoutDetector.HistoryService;

/// <summary>
/// Strongly-typed bind for the <c>History:</c> configuration section.
/// </summary>
/// <remarks>
/// Phase 1 only uses <see cref="ConnectionString"/> for the /health probe
/// to echo the configured DB host. The provider lifts in PRs #2-#5 will
/// consume it for real. Git/build metadata fields are baked in at image
/// build time via Dockerfile ARGs and surfaced on /health.
/// </remarks>
public sealed class HistoryServiceOptions
{
    public const string SectionName = "History";

    public string ConnectionString { get; set; } =
        "Host=mbd-history-postgres;Port=5432;Database=mbd_history;Username=mbd;Password=mbd";

    /// <summary>
    /// Optional FRED API key used by <c>FredFetcher</c> (micro-PR #5).
    /// Resolved from <c>History:FredApiKey</c> in config (envvar
    /// <c>History__FredApiKey</c> on Linux). When empty, the fetcher
    /// falls back to the process-level <c>FRED_API_KEY</c> env var, then
    /// fail-quiets with a warning if neither is set.
    /// </summary>
    public string? FredApiKey { get; set; }

    public string GitCommit { get; set; } = "unknown";
    public string GitBranch { get; set; } = "unknown";
    public string BuildTime { get; set; } = "unknown";

    // ── Polygon NBBO fetcher (micro-PR #3) ────────────────────────────────
    /// <summary>Polygon API key. Read from <c>History__PolygonApiKey</c> env var.</summary>
    public string? PolygonApiKey { get; set; }

    /// <summary>Override the Polygon base URL. Tests inject a stub URL here.</summary>
    public string? PolygonBaseUrl { get; set; }

    /// <summary>Per-call Polygon timeout in ms. Default 3000 (mirrors MBD PR #110).</summary>
    public int PolygonPerCallTimeoutMs { get; set; } = 3000;

    /// <summary>Process-wide concurrency cap on in-flight Polygon NBBO calls. Default 8.</summary>
    public int PolygonMaxConcurrentFetches { get; set; } = 8;

    /// <summary>
    /// Freshness window (seconds) for the at-or-before fuzzy NBBO match.
    /// Default 300 (5 minutes), as per MBD PR #98 rationale.
    /// </summary>
    public int NbboStaleQuoteToleranceSeconds { get; set; } = 300;
}
