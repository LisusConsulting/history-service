using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Observability;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Cache-first NBBO provider. Lifted from MBD's
/// <c>PostgresOptionQuoteService</c> (PRs #98 + #121). Lookup order:
///   1. In-memory hit cache.
///   2. In-memory miss-marker.
///   3. Postgres strict-match on (ticker, ts).
///   4. Postgres fuzzy at-or-before within the freshness window.
///   5. Postgres miss-marker table.
///   6. Polygon /v3/quotes via <see cref="IPolygonNbboFetcher"/>.
/// On step-6 success we write through to postgres + memory and return
/// cache_hit=false. On step-6 miss we write a miss-marker and return
/// is_miss_marker=true.
///
/// <para>
/// Differences vs. the MBD source:
///   - No <c>IBacktestFetchBudget</c> injection — that path was removed
///     in PR #133.
///   - In-memory cache layered in front of postgres for the hot path
///     (the "in-memory NBBO cache pattern" from PR #98).
///   - Scoped DI lifetime so a fresh NpgsqlConnection per request /
///     gRPC call. The fetcher + memory cache are singletons.
/// </para>
/// </summary>
/// <summary>
/// Per-quote gap key for NBBO. Two concurrent
/// <see cref="OptionQuotesProvider.GetAtOrBeforeAsync"/> callers asking
/// for the same (ticker, ts) collapse on this key — only one issues the
/// Polygon /v3/quotes call + write-through. The other awaits and reads
/// from the now-warm cache (in-memory or postgres). Without this, the
/// in-memory layer + postgres ON CONFLICT DO NOTHING handle correctness,
/// but Polygon would receive N duplicate calls during a backtest cold-
/// start burst.
/// </summary>
internal sealed record NbboGapKey(string Ticker, DateTime TsUtc);

public sealed class OptionQuotesProvider : IOptionQuotesProvider
{
    public const int DefaultStaleQuoteToleranceSeconds = 300;

    private readonly NbboMemoryCache m_MemCache;
    private readonly IPolygonNbboFetcher m_Fetcher;
    private readonly ILogger<OptionQuotesProvider> m_Logger;
    private readonly string m_ConnectionString;
    private readonly int m_StaleQuoteToleranceSeconds;
    private readonly MetricsCollector? m_Metrics;
    private readonly GapLockExecutor<NbboGapKey> m_GapLock = new();

    public OptionQuotesProvider(
        NbboMemoryCache inMemCache,
        IPolygonNbboFetcher inFetcher,
        IOptions<HistoryServiceOptions> inOpts,
        ILogger<OptionQuotesProvider> inLogger,
        MetricsCollector? inMetrics = null)
    {
        m_MemCache = inMemCache;
        m_Fetcher = inFetcher;
        m_Logger = inLogger;
        var tmpOpts = inOpts.Value;
        m_ConnectionString = tmpOpts.ConnectionString;
        m_StaleQuoteToleranceSeconds = tmpOpts.NbboStaleQuoteToleranceSeconds > 0
            ? tmpOpts.NbboStaleQuoteToleranceSeconds
            : DefaultStaleQuoteToleranceSeconds;
        m_Metrics = inMetrics;
    }

    public async Task<OptionQuotesLookup> GetAtOrBeforeAsync(
        string inTicker,
        DateTime inTsUtc,
        CancellationToken inCt = default)
    {
        m_Metrics?.RecordRequest(MetricKind.Nbbo);

        // Cache-first pre-check (in-memory hit, in-memory miss, postgres
        // strict + fuzzy, postgres miss-marker). Returns early if any of
        // these resolve. Only on a full miss do we enter the GapLockExecutor
        // body below to fan one Polygon call across N concurrent waiters.
        var tmpEarly = await TryServeFromCacheAsync(inTicker, inTsUtc, inCt).ConfigureAwait(false);
        if (tmpEarly is not null) return tmpEarly;

        // Polygon fetch + write-through under GapLockExecutor. Two
        // concurrent callers asking for the same (ticker, ts) collapse:
        // only one calls Polygon; the other awaits and re-reads the
        // warmed memory/postgres cache.
        OptionQuotesLookup? tmpResult = null;
        var tmpKey = new NbboGapKey(
            inTicker, DateTime.SpecifyKind(inTsUtc, DateTimeKind.Utc));
        await m_GapLock.ExecuteFetchAndPersistAsync(tmpKey, async () =>
        {
            // Re-check the cache under the SingleFlight slot. A previous
            // winner may have warmed mem-cache between the early read and
            // this body. Without the re-check, two adjacent burst-callers
            // would both fetch.
            var tmpRecheck = await TryServeFromCacheAsync(inTicker, inTsUtc, inCt)
                .ConfigureAwait(false);
            if (tmpRecheck is not null)
            {
                tmpResult = tmpRecheck;
                return;
            }

            var tmpFetch = await m_Fetcher.FetchAsync(inTicker, inTsUtc, inCt)
                .ConfigureAwait(false);
            switch (tmpFetch.Outcome)
            {
                case PolygonNbboOutcome.Hit when tmpFetch.Quote is not null:
                {
                    var tmpRec = ToRecord(tmpFetch.Quote);
                    await CacheAsync(tmpRec, inCt).ConfigureAwait(false);
                    m_MemCache.PutHit(tmpRec);
                    tmpResult = new OptionQuotesLookup(
                        tmpRec, CacheHit: false, IsMissMarker: false);
                    return;
                }
                case PolygonNbboOutcome.Miss:
                {
                    // Record the DB miss-marker so future calls skip Polygon,
                    // then try the last-known stale quote (no freshness floor)
                    // before returning null → $0. Only poison the in-memory
                    // miss when even that finds nothing, so future lookups keep
                    // reaching the stale fallback (via the step-5 miss-marker path).
                    await RecordMissAsync(
                        inTicker, inTsUtc, tmpFetch.MissReason ?? "miss", inCt)
                        .ConfigureAwait(false);
                    m_Metrics?.RecordMissMarker(MetricKind.Nbbo);
                    var tmpStale = await TryGetLastKnownAsync(inTicker, inTsUtc, inCt)
                        .ConfigureAwait(false);
                    if (tmpStale is not null)
                    {
                        tmpResult = new OptionQuotesLookup(
                            tmpStale, CacheHit: false, IsMissMarker: false);
                        return;
                    }
                    m_MemCache.PutMiss(inTicker, inTsUtc);
                    tmpResult = new OptionQuotesLookup(
                        null, CacheHit: false, IsMissMarker: true);
                    return;
                }
                case PolygonNbboOutcome.Transient:
                default:
                {
                    // Don't poison the cache. Caller may retry on the next
                    // call. Returning null here propagates as Transient
                    // through the SF result.
                    tmpResult = new OptionQuotesLookup(
                        null, CacheHit: false, IsMissMarker: false);
                    return;
                }
            }
        }).ConfigureAwait(false);

        // Late joiners on the same SingleFlight slot did not run the
        // body; they must read the warmed cache themselves.
        if (tmpResult is null)
        {
            var tmpAfterFlight = await TryServeFromCacheAsync(inTicker, inTsUtc, inCt)
                .ConfigureAwait(false);
            return tmpAfterFlight
                ?? new OptionQuotesLookup(null, CacheHit: false, IsMissMarker: false);
        }
        return tmpResult;
    }

    /// <summary>
    /// Cache-first probe. Returns <c>null</c> if no layer can serve the
    /// request; otherwise returns the resolved <see cref="OptionQuotesLookup"/>.
    /// Order: in-memory hit, in-memory miss, postgres strict, postgres
    /// fuzzy at-or-before within freshness window, postgres miss-marker.
    /// Mirror of the lookup waterfall described on the class doc.
    /// </summary>
    private async Task<OptionQuotesLookup?> TryServeFromCacheAsync(
        string inTicker, DateTime inTsUtc, CancellationToken inCt)
    {
        // 1) In-memory hit?
        if (m_MemCache.TryGetHit(inTicker, inTsUtc, out var tmpMemHit) && tmpMemHit is not null)
        {
            m_Metrics?.RecordCacheHit(MetricKind.Nbbo);
            return new OptionQuotesLookup(tmpMemHit, CacheHit: true, IsMissMarker: false);
        }

        // 2) In-memory miss-marker?
        if (m_MemCache.IsMiss(inTicker, inTsUtc))
        {
            m_Metrics?.RecordCacheHit(MetricKind.Nbbo);
            return new OptionQuotesLookup(null, CacheHit: true, IsMissMarker: true);
        }

        // 3-4) Postgres cached row (strict-match → fuzzy at-or-before).
        var tmpDb = await TryGetCachedAsync(inTicker, inTsUtc, inCt).ConfigureAwait(false);
        if (tmpDb is not null)
        {
            m_MemCache.PutHit(tmpDb);
            m_Metrics?.RecordCacheHit(MetricKind.Nbbo);
            return new OptionQuotesLookup(tmpDb, CacheHit: true, IsMissMarker: false);
        }

        // 5) Postgres miss-marker? Upstream already had nothing at this minute.
        //    Before declaring a hard miss (→ null → $0 in the fill path), try
        //    the last-known stale quote (no freshness floor). Only set the
        //    in-memory miss when even that finds nothing, so future lookups
        //    keep reaching the stale fallback rather than short-circuiting to null.
        if (await IsKnownMissAsync(inTicker, inTsUtc, inCt).ConfigureAwait(false))
        {
            var tmpStale = await TryGetLastKnownAsync(inTicker, inTsUtc, inCt).ConfigureAwait(false);
            if (tmpStale is not null)
            {
                m_Metrics?.RecordCacheHit(MetricKind.Nbbo);
                return new OptionQuotesLookup(tmpStale, CacheHit: true, IsMissMarker: false);
            }
            m_MemCache.PutMiss(inTicker, inTsUtc);
            m_Metrics?.RecordCacheHit(MetricKind.Nbbo);
            return new OptionQuotesLookup(null, CacheHit: true, IsMissMarker: true);
        }

        return null;
    }

    // ── postgres reads ────────────────────────────────────────────────────

    private async Task<OptionQuoteRecord?> TryGetCachedAsync(
        string inTicker, DateTime inTsUtc, CancellationToken inCt)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        // Strict match first — the legacy hot path. Index-friendly via the
        // unique (ticker, ts) btree.
        var tmpRow = await tmpConn.QueryFirstOrDefaultAsync<QuoteRow>(
            """
            SELECT ticker AS Ticker, ts AS RequestedTs, as_of_ts AS AsOfTs,
                   bid_price AS BidPrice, ask_price AS AskPrice,
                   bid_size AS BidSize, ask_size AS AskSize,
                   bid_exchange AS BidExchange, ask_exchange AS AskExchange
            FROM historical_options_quotes
            WHERE ticker = @Ticker AND ts = @Ts
            """,
            new { Ticker = inTicker, Ts = inTsUtc }).ConfigureAwait(false);

        if (tmpRow is null && m_StaleQuoteToleranceSeconds > 0)
        {
            // Fuzzy at-or-before — most-recent cached row within freshness
            // window. ORDER BY ts DESC LIMIT 1 walks the unique index
            // backward from the requested ts.
            var tmpFloor = inTsUtc.AddSeconds(-m_StaleQuoteToleranceSeconds);
            tmpRow = await tmpConn.QueryFirstOrDefaultAsync<QuoteRow>(
                """
                SELECT ticker AS Ticker, ts AS RequestedTs, as_of_ts AS AsOfTs,
                       bid_price AS BidPrice, ask_price AS AskPrice,
                       bid_size AS BidSize, ask_size AS AskSize,
                       bid_exchange AS BidExchange, ask_exchange AS AskExchange
                FROM historical_options_quotes
                WHERE ticker = @Ticker AND ts <= @Ts AND ts >= @Floor
                ORDER BY ts DESC
                LIMIT 1
                """,
                new { Ticker = inTicker, Ts = inTsUtc, Floor = tmpFloor })
                .ConfigureAwait(false);
        }

        if (tmpRow is null) return null;
        return new OptionQuoteRecord(
            Ticker: tmpRow.Ticker,
            RequestedTsUtc: DateTime.SpecifyKind(tmpRow.RequestedTs, DateTimeKind.Utc),
            AsOfTsUtc: DateTime.SpecifyKind(tmpRow.AsOfTs ?? tmpRow.RequestedTs, DateTimeKind.Utc),
            BidPrice: tmpRow.BidPrice ?? 0m,
            AskPrice: tmpRow.AskPrice ?? 0m,
            BidSize: tmpRow.BidSize,
            AskSize: tmpRow.AskSize,
            BidExchange: tmpRow.BidExchange,
            AskExchange: tmpRow.AskExchange);
    }

    /// <summary>
    /// 2026-06-28 — LAST-RESORT at-or-before lookup with NO freshness floor:
    /// the most-recent cached quote at-or-before <paramref name="inTsUtc"/>
    /// regardless of age. Used ONLY when the normal path (in-window fuzzy +
    /// Polygon) has fully MISSED. Rescues forced-exit fill pricing on sparse
    /// fresh-symbol contracts whose exact-minute NBBO upstream lacks but which
    /// DO have an earlier cached quote — without this they price null → $0,
    /// silently dropping a spread leg and producing garbage backtest P&L
    /// (observed: MU/SMCI/ARM, 44/72 contracts entry-quote-only). The record
    /// carries its real AsOfTs, so freshness-sensitive callers (the exit-
    /// DECISION pass) still defer on stale data; only the fill path — which
    /// has no fresh quote either way — benefits. PARITY-SAFE: dense symbols
    /// (TSLA) almost never reach a full miss, so this rarely runs for them.
    /// </summary>
    private async Task<OptionQuoteRecord?> TryGetLastKnownAsync(
        string inTicker, DateTime inTsUtc, CancellationToken inCt)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);
        var tmpRow = await tmpConn.QueryFirstOrDefaultAsync<QuoteRow>(
            """
            SELECT ticker AS Ticker, ts AS RequestedTs, as_of_ts AS AsOfTs,
                   bid_price AS BidPrice, ask_price AS AskPrice,
                   bid_size AS BidSize, ask_size AS AskSize,
                   bid_exchange AS BidExchange, ask_exchange AS AskExchange
            FROM historical_options_quotes
            WHERE ticker = @Ticker AND ts <= @Ts
            ORDER BY ts DESC
            LIMIT 1
            """,
            new { Ticker = inTicker, Ts = inTsUtc }).ConfigureAwait(false);
        if (tmpRow is null) return null;
        return new OptionQuoteRecord(
            Ticker: tmpRow.Ticker,
            RequestedTsUtc: DateTime.SpecifyKind(tmpRow.RequestedTs, DateTimeKind.Utc),
            AsOfTsUtc: DateTime.SpecifyKind(tmpRow.AsOfTs ?? tmpRow.RequestedTs, DateTimeKind.Utc),
            BidPrice: tmpRow.BidPrice ?? 0m,
            AskPrice: tmpRow.AskPrice ?? 0m,
            BidSize: tmpRow.BidSize,
            AskSize: tmpRow.AskSize,
            BidExchange: tmpRow.BidExchange,
            AskExchange: tmpRow.AskExchange);
    }

    private async Task<bool> IsKnownMissAsync(
        string inTicker, DateTime inTsUtc, CancellationToken inCt)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);
        // Range-shape lookup (post-PR #3): a row with range_from <= ts <=
        // range_to means we already tried this minute (or a contiguous run
        // that includes it) and upstream had nothing. Faster on average
        // than the old (ticker, ts) point lookup because adjacent
        // single-minute writes coalesce into one range, so this predicate
        // hits fewer index entries per ticker.
        var tmpHit = await tmpConn.ExecuteScalarAsync<int?>(
            """
            SELECT 1 FROM historical_options_quotes_misses
            WHERE ticker = @Ticker AND range_from <= @Ts AND range_to >= @Ts
            LIMIT 1
            """,
            new { Ticker = inTicker, Ts = inTsUtc }).ConfigureAwait(false);
        return tmpHit == 1;
    }

    // ── postgres writes ───────────────────────────────────────────────────

    private async Task CacheAsync(OptionQuoteRecord inRec, CancellationToken inCt)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);
        await tmpConn.ExecuteAsync(
            """
            INSERT INTO historical_options_quotes
              (ticker, ts, as_of_ts, bid_price, ask_price, bid_size, ask_size, bid_exchange, ask_exchange, fetched_at)
            VALUES
              (@Ticker, @Ts, @AsOfTs, @Bid, @Ask, @BidSize, @AskSize, @BidEx, @AskEx, NOW())
            ON CONFLICT (ticker, ts) DO NOTHING
            """,
            new
            {
                Ticker = inRec.Ticker,
                Ts = inRec.RequestedTsUtc,
                AsOfTs = inRec.AsOfTsUtc,
                Bid = inRec.BidPrice,
                Ask = inRec.AskPrice,
                BidSize = inRec.BidSize,
                AskSize = inRec.AskSize,
                BidEx = inRec.BidExchange,
                AskEx = inRec.AskExchange,
            }).ConfigureAwait(false);
    }

    private async Task RecordMissAsync(
        string inTicker, DateTime inTsUtc, string inReason, CancellationToken inCt)
    {
        try
        {
            await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
            await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

            // Write as a degenerate 1-minute range marker. The
            // RangeMarkerWriter coalesces with adjacent existing markers
            // (within 1 minute of an existing range_from/range_to) so
            // sequential single-call writes for contiguous minutes collapse
            // into one range row over time. This is the principle from
            // brief 2026-05-02: store missing data as ranges, not points.
            //
            // Adjacency = 1 minute (60s in ticks). Two markers separated
            // by exactly 60s collapse; >60s stays separate. NBBO grid is
            // already minute-aligned upstream so this matches our cadence
            // exactly.
            var tmpRange = new DateTimeOffset(DateTime.SpecifyKind(inTsUtc, DateTimeKind.Utc));
            await RangeMarkerWriter.WriteAsync(
                tmpConn, NbboMissTableSpec,
                inKeyValues: new[]
                {
                    new KeyValuePair<string, object>("Ticker", inTicker),
                },
                inNewRanges: new[] { (tmpRange, tmpRange) },
                inReason: inReason,
                inAdjacencyTicks: TimeSpan.FromMinutes(1).Ticks,
                inCt: inCt).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Don't break the response just because miss-marker insert
            // failed (e.g. table doesn't exist on a brand-new DB). Log
            // and continue — in-memory miss is still set.
            m_Logger.LogWarning(ex,
                "Failed to record NBBO miss-marker for {Ticker} @ {Ts:O}",
                inTicker, inTsUtc);
        }
    }

    /// <summary>
    /// Schema descriptor for <c>historical_options_quotes_misses</c>
    /// (range-shape post migration 009). Bound at class scope so tests
    /// can refer to it for setup.
    /// </summary>
    internal static readonly RangeMarkerTableSpec NbboMissTableSpec = new(
        TableName: "historical_options_quotes_misses",
        KeyColumns: new[] { "ticker" },
        RangeFromColumn: "range_from",
        RangeToColumn: "range_to",
        FetchedAtColumn: "fetched_at",
        HasReasonColumn: true,
        ReasonColumn: "reason");

    private static OptionQuoteRecord ToRecord(PolygonNbboResult inResult)
        => new(
            Ticker: inResult.Ticker,
            RequestedTsUtc: DateTime.SpecifyKind(inResult.RequestedTsUtc, DateTimeKind.Utc),
            AsOfTsUtc: DateTime.SpecifyKind(inResult.AsOfTsUtc, DateTimeKind.Utc),
            BidPrice: inResult.BidPrice,
            AskPrice: inResult.AskPrice,
            BidSize: inResult.BidSize,
            AskSize: inResult.AskSize,
            BidExchange: inResult.BidExchange,
            AskExchange: inResult.AskExchange);

    private sealed record QuoteRow(
        string Ticker,
        DateTime RequestedTs,
        DateTime? AsOfTs,
        decimal? BidPrice,
        decimal? AskPrice,
        int? BidSize,
        int? AskSize,
        int? BidExchange,
        int? AskExchange);
}
