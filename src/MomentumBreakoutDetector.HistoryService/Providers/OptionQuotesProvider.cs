using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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
public sealed class OptionQuotesProvider : IOptionQuotesProvider
{
    public const int DefaultStaleQuoteToleranceSeconds = 300;

    private readonly NbboMemoryCache m_MemCache;
    private readonly IPolygonNbboFetcher m_Fetcher;
    private readonly ILogger<OptionQuotesProvider> m_Logger;
    private readonly string m_ConnectionString;
    private readonly int m_StaleQuoteToleranceSeconds;
    private readonly MetricsCollector? m_Metrics;

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

        // 5) Postgres miss-marker?
        if (await IsKnownMissAsync(inTicker, inTsUtc, inCt).ConfigureAwait(false))
        {
            m_MemCache.PutMiss(inTicker, inTsUtc);
            m_Metrics?.RecordCacheHit(MetricKind.Nbbo);
            return new OptionQuotesLookup(null, CacheHit: true, IsMissMarker: true);
        }

        // 6) Polygon fetch + write-through.
        var tmpFetch = await m_Fetcher.FetchAsync(inTicker, inTsUtc, inCt).ConfigureAwait(false);
        switch (tmpFetch.Outcome)
        {
            case PolygonNbboOutcome.Hit when tmpFetch.Quote is not null:
            {
                var tmpRec = ToRecord(tmpFetch.Quote);
                await CacheAsync(tmpRec, inCt).ConfigureAwait(false);
                m_MemCache.PutHit(tmpRec);
                return new OptionQuotesLookup(tmpRec, CacheHit: false, IsMissMarker: false);
            }
            case PolygonNbboOutcome.Miss:
            {
                await RecordMissAsync(inTicker, inTsUtc, tmpFetch.MissReason ?? "miss", inCt)
                    .ConfigureAwait(false);
                m_MemCache.PutMiss(inTicker, inTsUtc);
                m_Metrics?.RecordMissMarker(MetricKind.Nbbo);
                return new OptionQuotesLookup(null, CacheHit: false, IsMissMarker: true);
            }
            case PolygonNbboOutcome.Transient:
            default:
            {
                // Don't poison the cache. Caller may retry on the next call.
                return new OptionQuotesLookup(null, CacheHit: false, IsMissMarker: false);
            }
        }
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

    private async Task<bool> IsKnownMissAsync(
        string inTicker, DateTime inTsUtc, CancellationToken inCt)
    {
        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);
        var tmpHit = await tmpConn.ExecuteScalarAsync<int?>(
            """
            SELECT 1 FROM historical_options_quotes_misses
            WHERE ticker = @Ticker AND ts = @Ts
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
            await tmpConn.ExecuteAsync(
                """
                INSERT INTO historical_options_quotes_misses (ticker, ts, reason, recorded_at)
                VALUES (@Ticker, @Ts, @Reason, NOW())
                ON CONFLICT (ticker, ts) DO NOTHING
                """,
                new { Ticker = inTicker, Ts = inTsUtc, Reason = inReason })
                .ConfigureAwait(false);
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
