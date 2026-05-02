using System.Text.Json;
using System.Text.Json.Serialization;

namespace MomentumBreakoutDetector.HistoryService.Seeder;

/// <summary>
/// Durable checkpoint for resumable seeds.
/// </summary>
/// <remarks>
/// Written to disk after every successfully-completed trading day so an
/// abort or crash mid-seed only loses the in-flight day. Re-running with
/// the same <c>--checkpoint-file</c> resumes at <see cref="LastCompletedDate"/>
/// + 1 trading day. Write-through caching in the history-service makes
/// the in-flight day idempotent on resume — duplicates collide on the
/// (ticker, ts) UNIQUE index and silently no-op.
/// </remarks>
public sealed class Checkpoint
{
    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = "";

    /// <summary>
    /// Last fully-completed trading day. <c>null</c> for a fresh checkpoint.
    /// On resume, the seeder skips all dates &lt;= this value.
    /// </summary>
    [JsonPropertyName("lastCompletedDate")]
    public DateOnly? LastCompletedDate { get; set; }

    [JsonPropertyName("totalDaysFetched")]
    public int TotalDaysFetched { get; set; }

    [JsonPropertyName("totalKeysFetched")]
    public long TotalKeysFetched { get; set; }

    [JsonPropertyName("startedAtUtc")]
    public DateTime StartedAtUtc { get; set; }

    [JsonPropertyName("updatedAtUtc")]
    public DateTime UpdatedAtUtc { get; set; }

    private static readonly JsonSerializerOptions s_JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static async Task<Checkpoint> LoadOrCreateAsync(string inPath, string inSymbol, CancellationToken inCt)
    {
        if (!File.Exists(inPath))
        {
            return new Checkpoint
            {
                Symbol = inSymbol,
                StartedAtUtc = DateTime.UtcNow,
                UpdatedAtUtc = DateTime.UtcNow,
            };
        }

        await using var tmpFs = File.OpenRead(inPath);
        var tmpCp = await JsonSerializer.DeserializeAsync<Checkpoint>(tmpFs, s_JsonOptions, inCt)
                    ?? throw new InvalidOperationException($"checkpoint file at {inPath} parsed to null");

        if (!string.Equals(tmpCp.Symbol, inSymbol, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"checkpoint symbol '{tmpCp.Symbol}' does not match requested symbol '{inSymbol}'. " +
                "Use a different --checkpoint-file or remove the existing one to start fresh.");
        }

        return tmpCp;
    }

    public async Task SaveAsync(string inPath, CancellationToken inCt)
    {
        UpdatedAtUtc = DateTime.UtcNow;
        // Write to a temp sibling then rename so a crash mid-write never
        // leaves a half-flushed checkpoint that fails to deserialize on
        // resume. POSIX rename is atomic; Windows ReplaceFile is close
        // enough for this tool's purposes.
        var tmpDir = Path.GetDirectoryName(Path.GetFullPath(inPath));
        if (!string.IsNullOrEmpty(tmpDir)) Directory.CreateDirectory(tmpDir);
        var tmpTempPath = inPath + ".tmp";
        await using (var tmpFs = File.Create(tmpTempPath))
        {
            await JsonSerializer.SerializeAsync(tmpFs, this, s_JsonOptions, inCt);
        }
        File.Move(tmpTempPath, inPath, overwrite: true);
    }
}
