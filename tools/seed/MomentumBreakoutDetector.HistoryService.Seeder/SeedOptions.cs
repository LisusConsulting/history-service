namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// Strongly-typed seed-driver invocation. Built from CLI args in <see cref="Program"/>.
/// </summary>
public sealed class SeedOptions
{
    public string Symbol { get; init; } = "TSLA";
    public DateOnly From { get; init; }
    public DateOnly To { get; init; }
    public int Concurrency { get; init; } = 32;
    public string CheckpointFile { get; init; } = "./checkpoint.json";
    public string HistoryGrpcHost { get; init; } = "localhost";
    public int HistoryGrpcPort { get; init; } = 30005;
    public string? LogFile { get; init; }

    /// <summary>Strike band as a fraction of underlying close. 0.05 = ATM ± 5%.</summary>
    public double StrikeBandPct { get; init; } = 0.05;

    /// <summary>Maximum days-to-expiry from the as-of date. Default 10.</summary>
    public int DteMaxDays { get; init; } = 10;
}
