using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Bars-cache provider. Lifted from MBD's
/// `MomentumBreakoutDetector.Infrastructure.Data.PostgresHistoricalDataProvider`
/// (PR #129 cold-start path) and trimmed to the bars-only surface.
///
/// On <see cref="GetBarsAsync"/> the provider:
///   1. Calls EnsureRangeCachedAsync — gap-detection + on-demand fetch.
///   2. Reads cached bars for [from, to] from historical_bars.
///   3. Returns the result + a cache_hit flag (true iff zero upstream calls).
///
/// Cold-start scenario (empty DB): gap-detection collapses to a single
/// full-range fetch; the call runs to completion. Warm cache: no Polygon
/// round-trips, fast read. The new service has no IBacktestFetchBudget
/// (MBD PR #133 removed it) — total work is bounded by the missing data
/// for the window plus the rate limiter on the fetcher.
/// </summary>
public interface IHistoricalBarsProvider
{
    /// <summary>
    /// Read bars from cache, on-demand-filling any gaps via the
    /// configured <see cref="IPolygonBarFetcher"/>. Returns the bars
    /// plus a flag indicating whether zero upstream calls were issued
    /// (i.e. the cache fully covered the requested range).
    /// </summary>
    Task<BarsReadResult> GetBarsAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt = default);
}

/// <summary>
/// Bars + a cache_hit signal. Materialised as a record so the gRPC
/// edge can populate <c>GetBarsResponse.cache_hit</c> without a second
/// round-trip to the DB.
/// </summary>
public sealed record BarsReadResult(
    IReadOnlyList<Bar> Bars,
    bool CacheHit);

public sealed class HistoricalBarsProvider : IHistoricalBarsProvider
{
    private readonly string m_ConnectionString;
    private readonly ILogger<HistoricalBarsProvider> m_Logger;
    private readonly IPolygonBarFetcher m_BarFetcher;

    /// <summary>
    /// Hard ceiling on the per-call Polygon /v2/aggs window in days.
    /// Polygon caps responses at 50,000 rows; for 1-min bars that's
    /// ~127 RTH days, so a 30-day chunk leaves ~3× headroom. Lifted
    /// verbatim from MBD PR #129.
    /// </summary>
    internal const int MaxFetchChunkDays = 30;

    public HistoricalBarsProvider(
        IOptions<HistoryServiceOptions> inOptions,
        IPolygonBarFetcher inBarFetcher,
        ILogger<HistoricalBarsProvider> inLogger)
        : this(inOptions.Value.ConnectionString, inBarFetcher, inLogger)
    {
    }

    /// <summary>Test-friendly ctor that takes the connection string directly.</summary>
    public HistoricalBarsProvider(
        string inConnectionString,
        IPolygonBarFetcher inBarFetcher,
        ILogger<HistoricalBarsProvider> inLogger)
    {
        m_ConnectionString = inConnectionString;
        m_BarFetcher = inBarFetcher;
        m_Logger = inLogger;
    }

    public async Task<BarsReadResult> GetBarsAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt = default)
    {
        var tmpUpstream = await EnsureRangeCachedAsync(inSymbol, inFromUtc, inToUtc, inTimeframe, inCt);
        var tmpBars = await ReadCachedBarsAsync(inSymbol, inFromUtc, inToUtc, inTimeframe, inCt);
        return new BarsReadResult(tmpBars, CacheHit: tmpUpstream == 0);
    }

    /// <summary>
    /// Cache-first read. Splits out the legacy query path so the
    /// on-demand wrapper can compose around it.
    /// </summary>
    private async Task<IReadOnlyList<Bar>> ReadCachedBarsAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt)
    {
        var tmpTimeframeStr = MapTimeframe(inTimeframe);
        m_Logger.LogInformation(
            "Querying {Timeframe} bars for {Symbol} from {From:yyyy-MM-dd HH:mm} to {To:yyyy-MM-dd HH:mm}",
            tmpTimeframeStr, inSymbol, inFromUtc, inToUtc);

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt);

        // For 5min and day, also synthesize from 1-min bars newer than
        // the last persisted bar so today's session shows up without
        // running a separate aggregation pass. Lifted verbatim from MBD.
        IEnumerable<BarRow> tmpRows;
        if (inTimeframe == BarTimeframe.FiveMinutes || inTimeframe == BarTimeframe.OneDay)
        {
            var tmpBucket = inTimeframe == BarTimeframe.FiveMinutes ? "5 minutes" : "1 day";
            tmpRows = await tmpConn.QueryAsync<BarRow>(
                $$"""
                WITH persisted_max AS (
                  SELECT COALESCE(MAX(timestamp), '-infinity'::timestamptz) AS latest
                  FROM historical_bars
                  WHERE symbol = @Symbol AND timeframe = @Timeframe
                ),
                derived AS (
                  SELECT
                    symbol,
                    date_bin(interval '{{tmpBucket}}', timestamp, timestamp '2000-01-01') AS timestamp,
                    (array_agg(open  ORDER BY timestamp ASC))[1]  AS open,
                    MAX(high)  AS high,
                    MIN(low)   AS low,
                    (array_agg(close ORDER BY timestamp DESC))[1] AS close,
                    SUM(volume) AS volume,
                    NULL::numeric AS vwap
                  FROM historical_bars, persisted_max
                  WHERE symbol = @Symbol AND timeframe = '1min'
                    AND timestamp >= persisted_max.latest + interval '{{tmpBucket}}'
                    AND timestamp >= @From AND timestamp <= @To
                  GROUP BY symbol, 2
                )
                SELECT symbol, timestamp, open, high, low, close, volume, vwap
                FROM historical_bars
                WHERE symbol = @Symbol AND timeframe = @Timeframe
                  AND timestamp >= @From AND timestamp <= @To
                UNION ALL
                SELECT symbol, timestamp, open, high, low, close, volume, vwap FROM derived
                ORDER BY timestamp
                """,
                new { Symbol = inSymbol, Timeframe = tmpTimeframeStr, From = inFromUtc, To = inToUtc });
        }
        else
        {
            tmpRows = await tmpConn.QueryAsync<BarRow>(
                """
                SELECT symbol, timestamp, open, high, low, close, volume, vwap
                FROM historical_bars
                WHERE symbol = @Symbol AND timeframe = @Timeframe
                  AND timestamp >= @From AND timestamp <= @To
                ORDER BY timestamp
                """,
                new { Symbol = inSymbol, Timeframe = tmpTimeframeStr, From = inFromUtc, To = inToUtc });
        }

        var tmpBars = tmpRows.Select(r => new Bar(
            Symbol: r.Symbol,
            Timestamp: r.Timestamp,
            Open: r.Open,
            High: r.High,
            Low: r.Low,
            Close: r.Close,
            Volume: r.Volume,
            VWAP: r.Vwap ?? 0m)).ToList();

        m_Logger.LogInformation("Loaded {Count} {Timeframe} bars for {Symbol}",
            tmpBars.Count, tmpTimeframeStr, inSymbol);

        return tmpBars;
    }

    // ── on-demand bar fetch (lifted from MBD PR #129) ─────────────────

    /// <summary>
    /// Identify gaps in the cache for the requested range, fetch them
    /// from Polygon, upsert into historical_bars, record miss markers
    /// for empty returns. Cold-start (empty DB) collapses to a single
    /// full-range fetch; warm cache short-circuits.
    ///
    /// Gap-detection strategy (cache-edge based, not market-calendar):
    ///   - Read MIN/MAX of cached bar timestamps in range.
    ///   - Read miss markers in range.
    ///   - Compute "uncovered" sub-ranges: [from, minCached - 1bar],
    ///     [maxCached + 1bar, to], minus any range covered by an
    ///     existing marker.
    ///   - Chunk each uncovered range to ≤ MaxFetchChunkDays days.
    ///   - Issue one Polygon call per chunk.
    ///
    /// Returns the total upstream call count so the caller (GetBarsAsync)
    /// can populate the cache_hit flag without a second probe.
    /// </summary>
    /// <returns>Number of upstream Polygon calls issued (0 = cache hit).</returns>
    internal async Task<int> EnsureRangeCachedAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt)
    {
        if (inFromUtc > inToUtc) return 0;

        var tmpTimeframeStr = MapTimeframe(inTimeframe);
        var tmpStep = StepForTimeframe(inTimeframe);

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt);

        // Cache edges
        var tmpEdges = await tmpConn.QueryFirstOrDefaultAsync<EdgeRow>(
            """
            SELECT MIN(timestamp) AS Earliest, MAX(timestamp) AS Latest, COUNT(*) AS Total
            FROM historical_bars
            WHERE symbol = @Symbol AND timeframe = @Timeframe
              AND timestamp >= @From AND timestamp <= @To
            """,
            new { Symbol = inSymbol, Timeframe = tmpTimeframeStr, From = inFromUtc, To = inToUtc });

        var tmpHaveCached = (tmpEdges?.Total ?? 0) > 0;

        // Determine uncovered sub-ranges (edge-gap detection).
        var tmpUncovered = new List<(DateTime From, DateTime To)>();
        if (!tmpHaveCached)
        {
            tmpUncovered.Add((inFromUtc, inToUtc));
        }
        else
        {
            if (inFromUtc < tmpEdges!.Earliest!.Value)
                tmpUncovered.Add((inFromUtc, tmpEdges.Earliest.Value - tmpStep));
            if (tmpEdges.Latest!.Value < inToUtc)
                tmpUncovered.Add((tmpEdges.Latest.Value + tmpStep, inToUtc));
        }

        if (tmpUncovered.Count == 0)
        {
            m_Logger.LogDebug(
                "Cache fully covers {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd} ({Total} bars) — no on-demand fetch",
                inSymbol, inTimeframe, inFromUtc, inToUtc, tmpEdges!.Total);
            return 0;
        }

        // Subtract miss markers — sub-ranges already known unfetchable.
        var tmpMarkerRows = (await tmpConn.QueryAsync<MissRow>(
            """
            SELECT range_from AS RangeFrom, range_to AS RangeTo
            FROM historical_bars_misses
            WHERE symbol = @Symbol AND timeframe = @Timeframe
              AND range_to >= @From AND range_from <= @To
            """,
            new { Symbol = inSymbol, Timeframe = tmpTimeframeStr, From = inFromUtc, To = inToUtc })).ToList();

        var tmpToFetch = SubtractMarkers(tmpUncovered, tmpMarkerRows);

        if (tmpToFetch.Count == 0)
        {
            m_Logger.LogInformation(
                "All uncovered sub-ranges for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd} are marker-shadowed — no Polygon fetch needed",
                inSymbol, inTimeframe, inFromUtc, inToUtc);
            return 0;
        }

        // Chunk + fetch + upsert + marker.
        int tmpUpstreamCalls = 0;
        foreach (var tmpRange in tmpToFetch)
        {
            foreach (var (tmpChunkFrom, tmpChunkTo) in ChunkRange(tmpRange.From, tmpRange.To, MaxFetchChunkDays))
            {
                tmpUpstreamCalls++;
                var tmpFetched = await m_BarFetcher.FetchBarsAsync(
                    inSymbol, tmpChunkFrom, tmpChunkTo, inTimeframe, inCt);

                if (tmpFetched.Count == 0)
                {
                    // Empty → write a miss marker so subsequent runs skip
                    // the re-fetch (4xx, plan-tier limit, weekend/holiday,
                    // contract not yet listed).
                    await RecordBarMissAsync(
                        tmpConn, inSymbol, tmpTimeframeStr, tmpChunkFrom, tmpChunkTo,
                        "no-data-from-polygon", inCt);
                    continue;
                }

                await UpsertBarsAsync(tmpConn, inSymbol, tmpTimeframeStr, tmpFetched, inCt);
            }
        }
        return tmpUpstreamCalls;
    }

    /// <summary>
    /// Per-bar step used to compute exclusive boundaries when slicing
    /// "below earliest cached" / "above latest cached" sub-ranges.
    /// </summary>
    private static TimeSpan StepForTimeframe(BarTimeframe inTimeframe) => inTimeframe switch
    {
        BarTimeframe.OneMinute => TimeSpan.FromMinutes(1),
        BarTimeframe.FiveMinutes => TimeSpan.FromMinutes(5),
        BarTimeframe.FifteenMinutes => TimeSpan.FromMinutes(15),
        BarTimeframe.OneHour => TimeSpan.FromHours(1),
        BarTimeframe.OneDay => TimeSpan.FromDays(1),
        _ => TimeSpan.FromMinutes(1)
    };

    /// <summary>
    /// Subtract miss-marker ranges from the uncovered range list.
    /// Internal so unit tests can pin the math directly.
    /// </summary>
    internal static List<(DateTime From, DateTime To)> SubtractMarkers(
        IReadOnlyList<(DateTime From, DateTime To)> inUncovered,
        IReadOnlyList<MissRow> inMarkers)
    {
        if (inMarkers.Count == 0) return inUncovered.ToList();

        var tmpResult = new List<(DateTime From, DateTime To)>();
        foreach (var tmpRange in inUncovered)
        {
            var tmpFragments = new List<(DateTime From, DateTime To)> { tmpRange };
            foreach (var tmpMarker in inMarkers)
            {
                var tmpNext = new List<(DateTime From, DateTime To)>();
                foreach (var tmpFrag in tmpFragments)
                {
                    if (tmpMarker.RangeTo < tmpFrag.From || tmpMarker.RangeFrom > tmpFrag.To)
                    {
                        tmpNext.Add(tmpFrag);
                        continue;
                    }
                    if (tmpMarker.RangeFrom <= tmpFrag.From && tmpMarker.RangeTo >= tmpFrag.To)
                        continue;
                    if (tmpMarker.RangeFrom > tmpFrag.From)
                        tmpNext.Add((tmpFrag.From, tmpMarker.RangeFrom.AddTicks(-1)));
                    if (tmpMarker.RangeTo < tmpFrag.To)
                        tmpNext.Add((tmpMarker.RangeTo.AddTicks(1), tmpFrag.To));
                }
                tmpFragments = tmpNext;
            }
            tmpResult.AddRange(tmpFragments);
        }
        return tmpResult;
    }

    /// <summary>
    /// Chunk a [from, to] range into successive sub-ranges no longer
    /// than inMaxDays calendar days.
    /// </summary>
    internal static IEnumerable<(DateTime From, DateTime To)> ChunkRange(
        DateTime inFrom, DateTime inTo, int inMaxDays)
    {
        if (inFrom > inTo) yield break;
        var tmpStep = TimeSpan.FromDays(inMaxDays);
        var tmpCursor = inFrom;
        while (tmpCursor <= inTo)
        {
            var tmpEnd = tmpCursor + tmpStep;
            if (tmpEnd > inTo) tmpEnd = inTo;
            yield return (tmpCursor, tmpEnd);
            if (tmpEnd >= inTo) yield break;
            tmpCursor = tmpEnd.AddTicks(1);
        }
    }

    private async Task UpsertBarsAsync(
        NpgsqlConnection inConn, string inSymbol, string inTimeframe,
        IReadOnlyList<Bar> inBars, CancellationToken inCt)
    {
        if (inBars.Count == 0) return;
        // Per-row INSERT ... ON CONFLICT DO NOTHING. The (symbol,
        // timeframe, timestamp) UNIQUE index makes this idempotent. We
        // do not use the volume-upgrade clause from MBD's
        // WriteMinuteBarAsync — that's tuned for live IEX-vs-REST
        // overlap. On-demand backfill always writes consolidated REST,
        // so DO NOTHING is the safer choice.
        foreach (var tmpBar in inBars)
        {
            await inConn.ExecuteAsync(
                """
                INSERT INTO historical_bars
                  (symbol, timeframe, timestamp, open, high, low, close, volume, vwap)
                VALUES
                  (@Symbol, @Timeframe, @Timestamp, @Open, @High, @Low, @Close, @Volume, @Vwap)
                ON CONFLICT (symbol, timeframe, timestamp) DO NOTHING
                """,
                new
                {
                    Symbol = inSymbol,
                    Timeframe = inTimeframe,
                    Timestamp = tmpBar.Timestamp,
                    Open = tmpBar.Open,
                    High = tmpBar.High,
                    Low = tmpBar.Low,
                    Close = tmpBar.Close,
                    Volume = tmpBar.Volume,
                    Vwap = (object?)(tmpBar.VWAP > 0m ? tmpBar.VWAP : (decimal?)null) ?? DBNull.Value,
                });
        }
        m_Logger.LogInformation(
            "On-demand fill: upserted {Count} {Timeframe} bars for {Symbol}",
            inBars.Count, inTimeframe, inSymbol);
    }

    private async Task RecordBarMissAsync(
        NpgsqlConnection inConn, string inSymbol, string inTimeframe,
        DateTime inFromUtc, DateTime inToUtc, string inReason, CancellationToken inCt)
    {
        await inConn.ExecuteAsync(
            """
            INSERT INTO historical_bars_misses (symbol, timeframe, range_from, range_to, reason, fetched_at)
            VALUES (@Symbol, @Timeframe, @From, @To, @Reason, NOW())
            ON CONFLICT (symbol, timeframe, range_from, range_to) DO NOTHING
            """,
            new { Symbol = inSymbol, Timeframe = inTimeframe, From = inFromUtc, To = inToUtc, Reason = inReason });
        m_Logger.LogInformation(
            "Recorded bar miss-marker {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd} ({Reason})",
            inSymbol, inTimeframe, inFromUtc, inToUtc, inReason);
    }

    /// <summary>Internal record exposed for unit tests of <see cref="SubtractMarkers"/>.</summary>
    internal sealed record MissRow(DateTime RangeFrom, DateTime RangeTo);

    private sealed record EdgeRow(DateTime? Earliest, DateTime? Latest, long Total);

    private static string MapTimeframe(BarTimeframe inTimeframe) => inTimeframe switch
    {
        BarTimeframe.OneMinute => "1min",
        BarTimeframe.FiveMinutes => "5min",
        BarTimeframe.FifteenMinutes => "15min",
        BarTimeframe.OneHour => "1hour",
        BarTimeframe.OneDay => "day",
        _ => "1min"
    };

    // Dapper mapping rows.
    private record BarRow(
        string Symbol, DateTime Timestamp,
        decimal Open, decimal High, decimal Low, decimal Close,
        decimal Volume, decimal? Vwap);
}
