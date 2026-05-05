using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Observability;
using Npgsql;
using TradingCalendar = MomentumBreakoutDetector.HistoryService.Domain.TradingCalendar;
using TradingSession = MomentumBreakoutDetector.HistoryService.Domain.TradingSession;

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

    /// <summary>
    /// Warmup entry-point used by <c>EnsureRangeCached</c>. Identifies
    /// gaps for (symbol, timeframe) over [<paramref name="inFromUtc"/>,
    /// <paramref name="inToUtc"/>] and fans out batched Polygon calls.
    /// Returns the count of upstream Polygon calls actually issued.
    /// </summary>
    /// <param name="inProgress">Optional async progress sink — awaited
    /// once per chunk. Pass <see langword="null"/> for fire-and-forget.
    /// We use a <c>Func</c> rather than <c>IProgress&lt;T&gt;</c> so the
    /// chunk loop can flush a gRPC stream write before issuing the next
    /// upstream call (vs <c>IProgress</c>, which posts to the captured
    /// <c>SynchronizationContext</c> and arrives after the loop ends in
    /// the gRPC server thread pool).</param>
    Task<int> EnsureRangeCachedAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe,
        Func<BarsWarmupProgress, CancellationToken, Task>? inProgress = null,
        CancellationToken inCt = default);
}

/// <summary>
/// Per-chunk progress emitted by <see cref="IHistoricalBarsProvider.EnsureRangeCachedAsync"/>.
/// One event per fetched chunk; <see cref="ChunkIndex"/> runs 1..<see cref="ChunksTotal"/>.
/// </summary>
public sealed record BarsWarmupProgress(
    string Symbol,
    BarTimeframe Timeframe,
    int ChunkIndex,
    int ChunksTotal,
    int BarsFetched,
    bool IsMissChunk);

/// <summary>
/// Bars + a cache_hit signal. Materialised as a record so the gRPC
/// edge can populate <c>GetBarsResponse.cache_hit</c> without a second
/// round-trip to the DB.
/// </summary>
public sealed record BarsReadResult(
    IReadOnlyList<Bar> Bars,
    bool CacheHit);

/// <summary>
/// Gap-range identity for the bars cache. Two concurrent
/// <see cref="HistoricalBarsProvider.EnsureRangeCachedAsync"/> callers
/// that compute overlapping fetch chunks will collapse on the same
/// <see cref="BarGapKey"/> via the <see cref="GapLockExecutor{TKey}"/>;
/// the second caller awaits the first's persist instead of re-fetching
/// + racing the INSERT. Record-equality + immutability give us the
/// hash/equality semantics SingleFlight expects.
/// </summary>
internal sealed record BarGapKey(
    string Symbol, string Timeframe, DateTime FromUtc, DateTime ToUtc);

public sealed class HistoricalBarsProvider : IHistoricalBarsProvider
{
    private readonly string m_ConnectionString;
    private readonly ILogger<HistoricalBarsProvider> m_Logger;
    private readonly IPolygonBarFetcher m_BarFetcher;
    private readonly MetricsCollector? m_Metrics;
    private readonly GapLockExecutor<BarGapKey> m_GapLock = new();

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
        ILogger<HistoricalBarsProvider> inLogger,
        MetricsCollector? inMetrics = null)
        : this(inOptions.Value.ConnectionString, inBarFetcher, inLogger, inMetrics)
    {
    }

    /// <summary>Test-friendly ctor that takes the connection string directly.</summary>
    public HistoricalBarsProvider(
        string inConnectionString,
        IPolygonBarFetcher inBarFetcher,
        ILogger<HistoricalBarsProvider> inLogger,
        MetricsCollector? inMetrics = null)
    {
        m_ConnectionString = inConnectionString;
        m_BarFetcher = inBarFetcher;
        m_Logger = inLogger;
        m_Metrics = inMetrics;
    }

    public async Task<BarsReadResult> GetBarsAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe, CancellationToken inCt = default)
    {
        m_Metrics?.RecordRequest(MetricKind.Bars);
        var tmpUpstream = await EnsureRangeCachedAsync(
            inSymbol, inFromUtc, inToUtc, inTimeframe,
            inProgress: null, inCt: inCt);
        var tmpBars = await ReadCachedBarsAsync(inSymbol, inFromUtc, inToUtc, inTimeframe, inCt);
        var tmpCacheHit = tmpUpstream == 0;
        // Provider-layer cache-hit accounting: zero upstream calls means
        // the read was served entirely from postgres. Fetcher records the
        // upstream side; this records the cache side so GetCacheStats can
        // compute hit-rate.
        if (tmpCacheHit) m_Metrics?.RecordCacheHit(MetricKind.Bars);
        return new BarsReadResult(tmpBars, CacheHit: tmpCacheHit);
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
    /// from Polygon, upsert into historical_bars, record range-shaped
    /// miss markers for empty returns. Cold-start (empty DB) collapses
    /// to a single full-range fetch; warm cache short-circuits.
    ///
    /// <para>
    /// <b>Gap-detection strategy (post-2026-05-02 — was cache-edge based,
    /// now expected-minus-cached-minus-marked):</b>
    /// <list type="number">
    ///   <item>
    ///     <c>expected</c> = the minute-set the cache should cover for
    ///     <paramref name="inSymbol"/> over [<paramref name="inFromUtc"/>,
    ///     <paramref name="inToUtc"/>] given <paramref name="inTimeframe"/>.
    ///     Built from <see cref="TradingCalendar.GetSessionMinutes"/>
    ///     iterated over each trading day in range, intersected with the
    ///     request window. For 1-min/5-min/15-min/1h timeframes this is the
    ///     full ExtendedHours session (04:00→20:00 ET, the window Alpaca
    ///     and Polygon both serve). For 1day this is just one timestamp
    ///     per trading day at 04:00 UTC (00:00 ET).
    ///   </item>
    ///   <item>
    ///     <c>cached</c> = SELECT timestamp FROM historical_bars WHERE
    ///     (symbol, timeframe) AND timestamp ∈ [from, to].
    ///   </item>
    ///   <item>
    ///     <c>marked</c> = the existing range-shaped marker rows for
    ///     (symbol, timeframe) overlapping [from, to].
    ///   </item>
    ///   <item>
    ///     <c>to_fetch</c> = (expected − cached) − marked, then coalesce
    ///     contiguous slots into ranges (a whole missing afternoon → ONE
    ///     range, not 195 minute-points).
    ///   </item>
    ///   <item>
    ///     For each contiguous to-fetch range: chunk into ≤
    ///     <see cref="MaxFetchChunkDays"/>-day sub-ranges, fetch, upsert.
    ///     If the chunk returns empty, defer the marker writes until end
    ///     of run, then coalesce-on-write via <see cref="RangeMarkerWriter"/>
    ///     so adjacent markers merge with each other AND with any
    ///     pre-existing marker rows.
    ///   </item>
    /// </list>
    /// </para>
    /// <para>
    /// Why this beats cache-edge: a 30-minute trading halt mid-session
    /// inside a fully-cached day is invisible to the edge logic (cache
    /// MIN/MAX still spans the day) but surfaces here as a 30-minute
    /// to_fetch range. The new pass also self-heals if markers are
    /// truncated — re-runs reconstruct them from the expected-minus-cached
    /// math.
    /// </para>
    /// </summary>
    /// <returns>Number of upstream Polygon calls issued (0 = cache hit).</returns>
    public async Task<int> EnsureRangeCachedAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        BarTimeframe inTimeframe,
        Func<BarsWarmupProgress, CancellationToken, Task>? inProgress = null,
        CancellationToken inCt = default)
    {
        if (inFromUtc > inToUtc) return 0;

        var tmpTimeframeStr = MapTimeframe(inTimeframe);
        var tmpStep = StepForTimeframe(inTimeframe);

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt);

        // 1. Build expected timestamp set per trading-calendar.
        var tmpExpected = ComputeExpectedTimestamps(inFromUtc, inToUtc, inTimeframe);
        if (tmpExpected.Count == 0)
        {
            // No trading days / minutes in window — nothing to fetch.
            m_Logger.LogDebug(
                "No expected timestamps for {Symbol} {Timeframe} {From:O}..{To:O} (weekend / all-holiday) — skip",
                inSymbol, inTimeframe, inFromUtc, inToUtc);
            return 0;
        }

        // 2. Pull cached timestamps for the window. We ask postgres for
        // the exact timestamps (rather than just MIN/MAX) so we can
        // diff against the expected set. The (symbol, timeframe,
        // timestamp) unique index makes this cheap; for a full year of
        // 1-min bars at ExtendedHours cadence that's ~240k rows per
        // symbol (manageable in-memory).
        var tmpCachedRows = (await tmpConn.QueryAsync<DateTime>(
            """
            SELECT timestamp
            FROM historical_bars
            WHERE symbol = @Symbol AND timeframe = @Timeframe
              AND timestamp >= @From AND timestamp <= @To
            """,
            new { Symbol = inSymbol, Timeframe = tmpTimeframeStr, From = inFromUtc, To = inToUtc })).ToList();
        var tmpCached = new HashSet<DateTime>(tmpCachedRows.Select(t =>
            DateTime.SpecifyKind(t, DateTimeKind.Utc)));

        // 3. Pull existing range markers.
        var tmpMarkerRows = (await tmpConn.QueryAsync<MissRow>(
            """
            SELECT range_from AS RangeFrom, range_to AS RangeTo
            FROM historical_bars_misses
            WHERE symbol = @Symbol AND timeframe = @Timeframe
              AND range_to >= @From AND range_from <= @To
            """,
            new { Symbol = inSymbol, Timeframe = tmpTimeframeStr, From = inFromUtc, To = inToUtc })).ToList();

        // 4. expected − cached − marked. Iterate expected once, drop any
        // ts that's cached OR shadowed by a marker, collect the survivors.
        var tmpMissing = new List<DateTime>(tmpExpected.Count);
        foreach (var tmpTs in tmpExpected)
        {
            if (tmpCached.Contains(tmpTs)) continue;
            var tmpShadowed = false;
            foreach (var tmpMarker in tmpMarkerRows)
            {
                if (tmpTs >= tmpMarker.RangeFrom && tmpTs <= tmpMarker.RangeTo)
                {
                    tmpShadowed = true;
                    break;
                }
            }
            if (!tmpShadowed) tmpMissing.Add(tmpTs);
        }

        if (tmpMissing.Count == 0)
        {
            m_Logger.LogDebug(
                "Cache fully covers {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd} (expected={Expected}, cached={Cached}, marked={Markers}) — no on-demand fetch",
                inSymbol, inTimeframe, inFromUtc, inToUtc,
                tmpExpected.Count, tmpCached.Count, tmpMarkerRows.Count);
            return 0;
        }

        // 5. Coalesce contiguous missing timestamps into ranges. Two
        // points are contiguous iff they differ by exactly tmpStep ticks.
        var tmpToFetchRanges = CoalesceContiguous(tmpMissing, tmpStep);
        m_Logger.LogInformation(
            "Bars gap detected for {Symbol} {Timeframe} {From:yyyy-MM-dd}..{To:yyyy-MM-dd}: {Missing} missing timestamps in {Ranges} contiguous ranges (expected={Expected}, cached={Cached}, marked={Markers})",
            inSymbol, inTimeframe, inFromUtc, inToUtc,
            tmpMissing.Count, tmpToFetchRanges.Count,
            tmpExpected.Count, tmpCached.Count, tmpMarkerRows.Count);

        // 6. Chunk + fetch + upsert. Empty-fetch results queue up for
        // post-loop coalesce-on-write so we don't fragment the marker
        // table with one row per chunk.
        var tmpChunks = new List<(DateTime From, DateTime To)>();
        foreach (var tmpRange in tmpToFetchRanges)
        {
            foreach (var tmpChunk in ChunkRange(tmpRange.From, tmpRange.To, MaxFetchChunkDays))
            {
                tmpChunks.Add(tmpChunk);
            }
        }

        // Per-chunk fetch+persist is wrapped in GapLockExecutor so two
        // concurrent EnsureRangeCachedAsync callers whose gap-detection
        // produced overlapping chunks collapse on the chunk's BarGapKey:
        // exactly one caller fetches + upserts; the other awaits and
        // re-reads the warm cache. Cross-replica safety comes from the
        // short pg_advisory_xact_lock taken inside the persist path
        // (see UpsertBarsLockedAsync below) — Polygon HTTP RTT is NOT
        // held under the lock.
        //
        // Empty-chunk miss-markers are persisted INSIDE the loop, per
        // chunk. The earlier "queue empties + write at end" pattern was
        // a self-perpetuating bug: if the gRPC deadline expired mid-loop
        // (likely with N sparse pre/AH zero-volume minutes producing N
        // single-minute single-chunk Alpaca round-trips), the post-loop
        // marker write never ran — so the SAME N hopeless fetches
        // repeated on every subsequent call. Per-chunk persistence makes
        // partial progress durable: even if the loop is canceled, every
        // marker for chunks that completed lands.
        var tmpUpstreamCalls = 0;
        for (var tmpIdx = 0; tmpIdx < tmpChunks.Count; tmpIdx++)
        {
            var (tmpChunkFrom, tmpChunkTo) = tmpChunks[tmpIdx];
            var tmpKey = new BarGapKey(
                inSymbol, tmpTimeframeStr,
                DateTime.SpecifyKind(tmpChunkFrom, DateTimeKind.Utc),
                DateTime.SpecifyKind(tmpChunkTo, DateTimeKind.Utc));

            var tmpChunkBarsCount = 0;
            var tmpChunkEmpty = false;
            var tmpRanHere = await m_GapLock.ExecuteFetchAndPersistAsync(
                tmpKey,
                async () =>
                {
                    Interlocked.Increment(ref tmpUpstreamCalls);
                    var tmpFetched = await m_BarFetcher.FetchBarsAsync(
                        inSymbol, tmpChunkFrom, tmpChunkTo, inTimeframe, inCt)
                        .ConfigureAwait(false);

                    if (tmpFetched.Count == 0)
                    {
                        tmpChunkEmpty = true;
                        return;
                    }

                    // Persist the fetched chunk inside a short
                    // advisory-lock-protected transaction. The lock keys
                    // on (table, gap-range) so a second history-service
                    // replica racing on the same chunk serialises here
                    // — first replica's INSERTs commit, second replica
                    // hits ON CONFLICT DO NOTHING.
                    await using var tmpPersistConn = new NpgsqlConnection(m_ConnectionString);
                    await tmpPersistConn.OpenAsync(inCt).ConfigureAwait(false);
                    await GapLockExecutor<BarGapKey>.WithPersistLockAsync(
                        tmpPersistConn,
                        inLockNamespace: "historical_bars",
                        inLockKeySeed: BuildBarGapKeySeed(tmpKey),
                        inWork: async (inLockedConn, inLockedTx, inLockCt) =>
                        {
                            await UpsertBarsAsync(
                                inLockedConn, inSymbol, tmpTimeframeStr,
                                tmpFetched, inLockCt, inLockedTx).ConfigureAwait(false);
                        },
                        inCt: inCt).ConfigureAwait(false);

                    tmpChunkBarsCount = tmpFetched.Count;
                }).ConfigureAwait(false);

            if (tmpChunkEmpty)
            {
                // Persist the marker for THIS chunk's missing timestamps
                // before moving on. If a later chunk's fetch hangs and the
                // gRPC deadline blows, this marker has already landed —
                // the next call sees it and skips the fetch.
                await PersistEmptyChunkMarkersAsync(
                    tmpConn, inSymbol, tmpTimeframeStr,
                    new[] { (tmpChunkFrom, tmpChunkTo) },
                    tmpMissing, tmpStep, inCt);

                if (inProgress is not null)
                {
                    await inProgress(new BarsWarmupProgress(
                        inSymbol, inTimeframe, tmpIdx + 1, tmpChunks.Count,
                        BarsFetched: 0, IsMissChunk: true), inCt);
                }
                continue;
            }

            if (inProgress is not null)
            {
                await inProgress(new BarsWarmupProgress(
                    inSymbol, inTimeframe, tmpIdx + 1, tmpChunks.Count,
                    BarsFetched: tmpChunkBarsCount, IsMissChunk: false), inCt);
            }
            // tmpRanHere is unused at present — captured for symmetry with
            // future "did this caller actually invoke the upstream" metrics.
            _ = tmpRanHere;
        }

        return tmpUpstreamCalls;
    }

    /// <summary>
    /// Build the full set of UTC-typed bar-open timestamps the cache is
    /// expected to contain over [from, to] for a given timeframe. RTH-bar
    /// timeframes use the ExtendedHours session (04:00→20:00 ET) which
    /// is what Alpaca and Polygon both return; 1day yields one timestamp
    /// per trading-day at 04:00 UTC (= 00:00 ET) matching the cached
    /// shape. Internal so unit tests can pin the math without standing
    /// up a Postgres container.
    /// </summary>
    internal static List<DateTime> ComputeExpectedTimestamps(
        DateTime inFromUtc, DateTime inToUtc, BarTimeframe inTimeframe)
    {
        var tmpFromDate = DateOnly.FromDateTime(inFromUtc);
        var tmpToDate = DateOnly.FromDateTime(inToUtc);
        var tmpResult = new List<DateTime>();

        if (inTimeframe == BarTimeframe.OneDay)
        {
            // Daily bars are stored at MIDNIGHT_ET → UTC for each trading
            // day. That is 05:00 UTC during EST (winter) and 04:00 UTC
            // during EDT (summer). The previous implementation hardcoded
            // 04:00 UTC year-round, which never matched winter rows
            // stored at 05:00 UTC — the gap detector then declared every
            // winter-EST trading day "missing" and the chunked range
            // fetch wrote miss-markers for whole days that already had
            // valid 05:00 UTC bars cached. Fridays at the end of a
            // chunk were the most-visible victims (DB scan showed
            // Mon=155 / Fri=110 for TSLA 2022-08-25..2026-05-01 — Fri
            // dramatically under-represented). Use the calendar's
            // DST-aware ET→UTC helper so the computed expected timestamps
            // match the stored shape across both DST regimes. Tests pin
            // both spring-forward and fall-back boundaries.
            foreach (var tmpDay in TradingCalendar.EnumerateTradingDays(tmpFromDate, tmpToDate))
            {
                var tmpTs = TradingCalendar.ConvertEasternToUtc(tmpDay, TimeSpan.Zero);
                if (tmpTs >= inFromUtc && tmpTs <= inToUtc) tmpResult.Add(tmpTs);
            }
            return tmpResult;
        }

        var tmpStep = StepForTimeframe(inTimeframe);
        foreach (var tmpDay in TradingCalendar.EnumerateTradingDays(tmpFromDate, tmpToDate))
        {
            // Intra-day bars cover ExtendedHours. Filter the minute set to
            // the requested timeframe stride so 5-min only yields :00, :05
            // …, 15-min only yields :00, :15, :30, :45 etc.
            foreach (var tmpMinute in TradingCalendar.GetSessionMinutes(tmpDay, TradingSession.ExtendedHours))
            {
                if (tmpMinute < inFromUtc || tmpMinute > inToUtc) continue;
                if (tmpStep.TotalMinutes > 1)
                {
                    // Align to bar-open: minute % stride == 0 from a
                    // session-anchored origin. ExtendedHours starts at
                    // 04:00 ET; modulo from start-of-day-UTC is fine
                    // because all valid stride values divide the hour.
                    if (tmpMinute.Minute % tmpStep.TotalMinutes != 0) continue;
                    if (inTimeframe == BarTimeframe.OneHour && tmpMinute.Minute != 0) continue;
                }
                tmpResult.Add(tmpMinute);
            }
        }
        return tmpResult;
    }

    /// <summary>
    /// Coalesce a sorted-or-unsorted set of timestamps into the minimal
    /// set of contiguous ranges where two adjacent timestamps differ by
    /// exactly <paramref name="inStep"/>. Internal so unit tests can pin
    /// the math directly.
    /// </summary>
    internal static List<(DateTime From, DateTime To)> CoalesceContiguous(
        IEnumerable<DateTime> inTimestamps, TimeSpan inStep)
    {
        var tmpSorted = inTimestamps.OrderBy(t => t).ToList();
        if (tmpSorted.Count == 0) return new List<(DateTime, DateTime)>();

        var tmpResult = new List<(DateTime From, DateTime To)>();
        var tmpRangeStart = tmpSorted[0];
        var tmpRangeEnd = tmpSorted[0];
        for (var i = 1; i < tmpSorted.Count; i++)
        {
            var tmpTs = tmpSorted[i];
            if (tmpTs - tmpRangeEnd <= inStep)
            {
                tmpRangeEnd = tmpTs;
            }
            else
            {
                tmpResult.Add((tmpRangeStart, tmpRangeEnd));
                tmpRangeStart = tmpTs;
                tmpRangeEnd = tmpTs;
            }
        }
        tmpResult.Add((tmpRangeStart, tmpRangeEnd));
        return tmpResult;
    }

    /// <summary>
    /// After the fetch loop, persist range markers for the contiguous
    /// runs of missing timestamps that fell inside an empty-response
    /// chunk. Coalesce-on-write merges adjacent existing markers so the
    /// table doesn't fragment over re-runs.
    /// </summary>
    private async Task PersistEmptyChunkMarkersAsync(
        NpgsqlConnection inConn, string inSymbol, string inTimeframe,
        IReadOnlyList<(DateTime From, DateTime To)> inEmptyChunks,
        IReadOnlyList<DateTime> inMissingTimestamps,
        TimeSpan inStep,
        CancellationToken inCt)
    {
        // Filter the originally-missing timestamps to those inside any
        // empty chunk (Polygon returned no bars for that whole chunk),
        // re-coalesce, and write as range markers via RangeMarkerWriter.
        var tmpInsideEmpty = new List<DateTime>();
        foreach (var tmpTs in inMissingTimestamps)
        {
            foreach (var tmpChunk in inEmptyChunks)
            {
                if (tmpTs >= tmpChunk.From && tmpTs <= tmpChunk.To)
                {
                    tmpInsideEmpty.Add(tmpTs);
                    break;
                }
            }
        }
        if (tmpInsideEmpty.Count == 0) return;

        var tmpRanges = CoalesceContiguous(tmpInsideEmpty, inStep);
        var tmpRangesUtc = tmpRanges
            .Select(r => (
                From: new DateTimeOffset(DateTime.SpecifyKind(r.From, DateTimeKind.Utc)),
                To: new DateTimeOffset(DateTime.SpecifyKind(r.To, DateTimeKind.Utc))))
            .ToList<(DateTimeOffset From, DateTimeOffset To)>();

        var tmpKeyValues = new[]
        {
            new KeyValuePair<string, object>("Symbol", inSymbol),
            new KeyValuePair<string, object>("Timeframe", inTimeframe),
        };

        var tmpFinalCount = await RangeMarkerWriter.WriteAsync(
            inConn, BarsMissTableSpec, tmpKeyValues,
            tmpRangesUtc, "no-data-from-polygon",
            inAdjacencyTicks: inStep.Ticks,
            inCt: inCt).ConfigureAwait(false);

        m_Metrics?.RecordMissMarker(MetricKind.Bars);
        m_Logger.LogInformation(
            "Recorded {Ranges} bar miss-marker range(s) for {Symbol} {Timeframe} (table now has {Total} rows for this key)",
            tmpRanges.Count, inSymbol, inTimeframe, tmpFinalCount);
    }

    /// <summary>
    /// Schema descriptor for <c>historical_bars_misses</c> — bound at
    /// class scope so it's reusable + so tests can refer to it.
    /// </summary>
    internal static readonly RangeMarkerTableSpec BarsMissTableSpec = new(
        TableName: "historical_bars_misses",
        KeyColumns: new[] { "symbol", "timeframe" },
        RangeFromColumn: "range_from",
        RangeToColumn: "range_to",
        FetchedAtColumn: "fetched_at",
        HasReasonColumn: true,
        ReasonColumn: "reason");

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

    /// <summary>
    /// Build a stable advisory-lock seed string for a bar gap key. Uses
    /// invariant ISO-8601 round-trip ("o") timestamp formatting so two
    /// replicas across different host locales hash identically.
    /// </summary>
    internal static string BuildBarGapKeySeed(BarGapKey inKey)
        => $"{inKey.Symbol}|{inKey.Timeframe}|{inKey.FromUtc:O}|{inKey.ToUtc:O}";

    private async Task UpsertBarsAsync(
        NpgsqlConnection inConn, string inSymbol, string inTimeframe,
        IReadOnlyList<Bar> inBars, CancellationToken inCt,
        NpgsqlTransaction? inTx = null)
    {
        if (inBars.Count == 0) return;
        // Per-row INSERT ... ON CONFLICT DO NOTHING. The (symbol,
        // timeframe, timestamp) UNIQUE index makes this idempotent. We
        // do not use the volume-upgrade clause from MBD's
        // WriteMinuteBarAsync — that's tuned for live IEX-vs-REST
        // overlap. On-demand backfill always writes consolidated REST,
        // so DO NOTHING is the safer choice.
        //
        // Concurrency: the caller may have the connection participating in
        // an outer pg_advisory_xact_lock-protected transaction (the new
        // gap-lock path). Pass <paramref name="inTx"/> through so Dapper
        // joins that transaction rather than auto-committing each INSERT
        // outside it.
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
                },
                transaction: inTx);
        }
        m_Logger.LogInformation(
            "On-demand fill: upserted {Count} {Timeframe} bars for {Symbol}",
            inBars.Count, inTimeframe, inSymbol);
    }

    /// <summary>Internal Dapper-mapping row for the marker-shadow read in
    /// EnsureRangeCachedAsync.</summary>
    internal sealed record MissRow(DateTime RangeFrom, DateTime RangeTo);

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
