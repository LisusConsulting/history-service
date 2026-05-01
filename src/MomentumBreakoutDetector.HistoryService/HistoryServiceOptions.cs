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

    public string GitCommit { get; set; } = "unknown";
    public string GitBranch { get; set; } = "unknown";
    public string BuildTime { get; set; } = "unknown";
}
