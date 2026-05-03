using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using MomentumBreakoutDetector.HistoryService.Domain;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Observability;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Single row of the <c>historical_options_contracts</c> cache table —
/// shape returned to the gRPC layer. Mirrors the columns the original
/// MBD picker selected, plus the descriptive fields the gRPC contract
/// (<c>OptionContract</c> message) exposes.
/// </summary>
public sealed record OptionContractRow(
  string Ticker,
  string UnderlyingTicker,
  string? ContractType,
  string? ExerciseStyle,
  DateOnly ExpirationDate,
  decimal? StrikePrice,
  int? SharesPerContract,
  string? PrimaryExchange);

/// <summary>
/// Result of a chain lookup. <see cref="CacheHit"/> is true iff zero
/// upstream calls were made; <see cref="IsMissMarker"/> is true iff a
/// permanent-miss marker matched (caller should NOT retry).
/// </summary>
public sealed record OptionChainResult(
  IReadOnlyList<OptionContractRow> Contracts,
  bool CacheHit,
  bool IsMissMarker);

/// <summary>
/// Provider abstraction the gRPC service depends on. Cache-first;
/// on miss the implementation calls <see cref="IPolygonChainFetcher"/>
/// and write-throughs the result.
/// </summary>
public interface IOptionChainProvider
{
  /// <summary>
  /// Read the chain for <paramref name="inSymbol"/> as of
  /// <paramref name="inAsOfDate"/>. Cache-first; on miss the implementation
  /// triggers <see cref="EnsureChainCachedAsync"/> and re-reads.
  /// </summary>
  Task<OptionChainResult> GetChainAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt);

  /// <summary>
  /// Ensure the chain for <paramref name="inSymbol"/> as of
  /// <paramref name="inAsOfDate"/> is present in
  /// historical_options_contracts. Read-path:
  ///   1. If any rows exist for (underlying, as_of_date) → return.
  ///   2. If a miss-marker covers this (symbol, as_of_date) → return.
  ///   3. Otherwise call IPolygonChainFetcher.FetchChainAsync and
  ///      upsert (idempotent on (as_of_date, ticker)).
  ///   4. If the fetch returned empty → write degenerate 1-day range
  ///      miss-marker via <see cref="RangeMarkerWriter"/>.
  /// </summary>
  Task EnsureChainCachedAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt);

  /// <summary>
  /// Intra-range warmup: ensure the chain cache covers every TRADING
  /// day in [<paramref name="inFromDate"/>, <paramref name="inToDate"/>]
  /// for <paramref name="inSymbol"/>. Computes <c>expected − cached − marked</c>
  /// using the shared <see cref="TradingCalendar"/>, fetches each contiguous
  /// missing day-range from Polygon, and persists genuinely-empty contiguous
  /// day-ranges as ONE marker row (not N day-rows) via
  /// <see cref="RangeMarkerWriter"/>. Adjacent markers from prior + new
  /// runs coalesce on write.
  /// </summary>
  /// <returns>Number of upstream Polygon calls issued (0 = cache hit).</returns>
  Task<int> EnsureRangeCachedAsync(
    string inSymbol, DateOnly inFromDate, DateOnly inToDate, CancellationToken inCt);
}

/// <summary>
/// Postgres-backed chain provider. Lifted from MBD's
/// <c>PostgresOptionContractPicker</c> (PR #130 era) for the standalone
/// history-service. The picker's signal-direction selection logic stays
/// in the engine — this service exposes the raw chain. The
/// EnsureChainCachedAsync + miss-marker logic + in-flight de-dup is
/// preserved verbatim. <c>IBacktestFetchBudget</c> dropped.
///
/// Stable ORDER BY clauses preserved (MBD PR #120 determinism): chain
/// reads order by strike, expiration, ticker so any downstream "nearest
/// strike then ThenBy(expiration).First()" resolves to the same single
/// contract every run.
///
/// <para>
/// <b>Range markers (post 2026-05-02 / PR #21).</b>
/// <c>historical_options_chains_misses</c> migrated from point-shape
/// (symbol, as_of_date) → range-shape (symbol, range_from, range_to)
/// in migration 010. The per-day write path now writes a degenerate
/// 1-day range marker via <see cref="RangeMarkerWriter"/>, which
/// coalesces adjacent existing markers. The new range warmup
/// <see cref="EnsureRangeCachedAsync"/> fetches whole contiguous
/// trading-day gaps in one shot and writes ONE marker row per
/// genuinely-empty contiguous day-range.
/// </para>
/// </summary>
/// <summary>
/// Per-day chain gap key. Used for both
/// <see cref="OptionChainProvider.EnsureChainCachedAsync"/> (one day) and
/// each iteration of the per-day loop inside
/// <see cref="OptionChainProvider.EnsureRangeCachedAsync"/>.
/// </summary>
internal sealed record ChainGapKey(string Symbol, DateOnly AsOfDate);

public sealed class OptionChainProvider : IOptionChainProvider
{
  private readonly string m_ConnectionString;
  private readonly ILogger<OptionChainProvider> m_Logger;
  private readonly IPolygonChainFetcher m_ChainFetcher;

  /// <summary>
  /// In-flight de-duplication via the shared
  /// <see cref="GapLockExecutor{TKey}"/> primitive. Many concurrent
  /// requests on the same (symbol, as_of) — typical at backtest
  /// cold-start — collapse on the same <see cref="ChainGapKey"/>: only
  /// one Polygon fetch runs; the other callers await and re-read the
  /// warmed DB. Replaces the bespoke Dictionary+lock used in MBD's
  /// PostgresOptionContractPicker so all four providers share one
  /// concurrency abstraction.
  /// </summary>
  private readonly GapLockExecutor<ChainGapKey> m_GapLock = new();

  private readonly MetricsCollector? m_Metrics;

  public OptionChainProvider(
    IOptions<HistoryServiceOptions> inOptions,
    ILogger<OptionChainProvider> inLogger,
    IPolygonChainFetcher inChainFetcher,
    MetricsCollector? inMetrics = null)
  {
    m_ConnectionString = inOptions.Value.ConnectionString;
    m_Logger = inLogger;
    m_ChainFetcher = inChainFetcher;
    m_Metrics = inMetrics;
  }

  public async Task<OptionChainResult> GetChainAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
  {
    if (string.IsNullOrEmpty(inSymbol))
    {
      return new OptionChainResult(
        Array.Empty<OptionContractRow>(), CacheHit: true, IsMissMarker: false);
    }
    m_Metrics?.RecordRequest(MetricKind.Chains);

    await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
    await tmpConn.OpenAsync(inCt);

    // 1. Cache pre-check. If the cache already has this (symbol, as_of)
    //    we return immediately without invoking the fetcher.
    var tmpDateStr = inAsOfDate.ToString("yyyy-MM-dd");
    var tmpHave = await tmpConn.QuerySingleAsync<bool>(
      """
      SELECT EXISTS (
        SELECT 1 FROM historical_options_contracts
        WHERE underlying_ticker = @Symbol
          AND as_of_date = @Date::date
      )
      """,
      new { Symbol = inSymbol, Date = tmpDateStr });

    var tmpCacheHit = tmpHave;

    if (!tmpHave)
    {
      // 2. Miss-marker pre-check. A range-shape row in
      //    historical_options_chains_misses whose [range_from, range_to]
      //    contains the requested as_of_date means we already tried
      //    Polygon and got nothing — return empty with IsMissMarker=true
      //    and skip the fetch.
      var tmpMissed = await IsKnownMissAsync(tmpConn, inSymbol, inAsOfDate);

      if (tmpMissed)
      {
        m_Metrics?.RecordCacheHit(MetricKind.Chains);
        return new OptionChainResult(
          Array.Empty<OptionContractRow>(), CacheHit: true, IsMissMarker: true);
      }

      // 3. Cold path — single-flight fetch + write-through, then re-open
      //    the connection (the fetch's internal upsert uses its own conn).
      await EnsureChainCachedAsync(inSymbol, inAsOfDate, inCt);
    }

    // 4. Read the rows. ORDER BY preserved from PR #120 for determinism.
    var tmpRows = await tmpConn.QueryAsync<OptionContractRow>(
      """
      SELECT
        ticker              AS "Ticker",
        underlying_ticker   AS "UnderlyingTicker",
        contract_type       AS "ContractType",
        exercise_style      AS "ExerciseStyle",
        expiration_date     AS "ExpirationDate",
        strike_price        AS "StrikePrice",
        shares_per_contract AS "SharesPerContract",
        primary_exchange    AS "PrimaryExchange"
      FROM historical_options_contracts
      WHERE underlying_ticker = @Symbol
        AND as_of_date = @Date::date
      ORDER BY strike_price, expiration_date, ticker
      """,
      new { Symbol = inSymbol, Date = tmpDateStr });

    var tmpList = tmpRows.ToList();

    // After cold-fill: if we still have zero rows, the fetch returned
    // empty and a miss-marker was just written. Surface that.
    var tmpIsMiss = !tmpCacheHit && tmpList.Count == 0;

    if (tmpCacheHit) m_Metrics?.RecordCacheHit(MetricKind.Chains);
    return new OptionChainResult(tmpList, CacheHit: tmpCacheHit, IsMissMarker: tmpIsMiss);
  }

  // ── on-demand chain fetch (lifted from MBD picker, PR #130) ──────────

  public Task EnsureChainCachedAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
  {
    // GapLockExecutor wraps the entire fetch+persist for this
    // (symbol, as_of_date) gap. Identical-key concurrent callers
    // collapse on the same SingleFlight slot; the persist step inside
    // DoEnsureAsync runs ON CONFLICT DO NOTHING + RangeMarkerWriter
    // with its own pg_advisory_xact_lock for cross-replica safety.
    var tmpKey = new ChainGapKey(inSymbol, inAsOfDate);
    return m_GapLock.ExecuteFetchAndPersistAsync(
      tmpKey,
      () => DoEnsureAsync(inSymbol, inAsOfDate, inCt));
  }

  /// <summary>
  /// Intra-range warmup. Computes <c>expected − cached − marked</c> using
  /// <see cref="TradingCalendar.EnumerateTradingDays"/>, fetches each
  /// trading day in any contiguous missing day-range, and persists
  /// genuinely-empty contiguous day-ranges as ONE marker row via
  /// <see cref="RangeMarkerWriter"/>.
  ///
  /// <para>
  /// <b>Why one Polygon call per day rather than per range:</b> Polygon's
  /// chain endpoint takes <c>as_of</c> as a single date — there's no
  /// range-fetch primitive. So a 5-day gap is 5 upstream calls. The win
  /// from the range-shape is on the marker side: a Christmas-week silence
  /// becomes ONE marker row instead of 5.
  /// </para>
  /// </summary>
  public async Task<int> EnsureRangeCachedAsync(
    string inSymbol, DateOnly inFromDate, DateOnly inToDate, CancellationToken inCt)
  {
    if (string.IsNullOrEmpty(inSymbol)) return 0;
    if (inFromDate > inToDate) return 0;

    m_Metrics?.RecordRequest(MetricKind.Chains);

    await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
    await tmpConn.OpenAsync(inCt);

    // 1. Expected trading days in window.
    var tmpExpected = TradingCalendar.GetTradingDays(inFromDate, inToDate);
    if (tmpExpected.Count == 0)
    {
      m_Logger.LogDebug(
        "No trading days in {Symbol} {From}..{To} (weekend / all-holiday) — skip",
        inSymbol, inFromDate, inToDate);
      return 0;
    }

    // 2. Cached days. Read DATE column as text + parse so Dapper does not
    //    fight Npgsql's DATE → DateOnly default mapping (the version
    //    pinned in this repo binds DATE → DateOnly, but Dapper's column
    //    binder treats the destination type as DateTime).
    var tmpCachedRows = await tmpConn.QueryAsync<string>(
      """
      SELECT DISTINCT as_of_date::text
      FROM historical_options_contracts
      WHERE underlying_ticker = @Symbol
        AND as_of_date >= @From::date AND as_of_date <= @To::date
      """,
      new
      {
        Symbol = inSymbol,
        From = inFromDate.ToString("yyyy-MM-dd"),
        To = inToDate.ToString("yyyy-MM-dd"),
      });
    var tmpCached = new HashSet<DateOnly>(tmpCachedRows.Select(d => DateOnly.Parse(d)));

    // 3. Marker ranges that overlap the window. Read as text + parse so
    //    Dapper doesn't need a DateOnly type-handler (the package version
    //    in this repo binds DATE → DateTime, not DateOnly).
    var tmpMarkerRows = (await tmpConn.QueryAsync<(string RangeFrom, string RangeTo)>(
      """
      SELECT range_from::text AS RangeFrom, range_to::text AS RangeTo
      FROM historical_options_chains_misses
      WHERE symbol = @Symbol
        AND range_to >= @From::date AND range_from <= @To::date
      """,
      new
      {
        Symbol = inSymbol,
        From = inFromDate.ToString("yyyy-MM-dd"),
        To = inToDate.ToString("yyyy-MM-dd"),
      }))
      .Select(r => new ChainMissRow(DateOnly.Parse(r.RangeFrom), DateOnly.Parse(r.RangeTo)))
      .ToList();

    // 4. expected − cached − marked.
    var tmpMissing = new List<DateOnly>(tmpExpected.Count);
    foreach (var tmpDay in tmpExpected)
    {
      if (tmpCached.Contains(tmpDay)) continue;
      var tmpShadowed = false;
      foreach (var tmpMarker in tmpMarkerRows)
      {
        if (tmpDay >= tmpMarker.RangeFrom && tmpDay <= tmpMarker.RangeTo)
        {
          tmpShadowed = true;
          break;
        }
      }
      if (!tmpShadowed) tmpMissing.Add(tmpDay);
    }

    if (tmpMissing.Count == 0)
    {
      m_Metrics?.RecordCacheHit(MetricKind.Chains);
      m_Logger.LogDebug(
        "Chain cache fully covers {Symbol} {From}..{To} (expected={Expected}, cached={Cached}, marked-ranges={Markers}) — no fetch",
        inSymbol, inFromDate, inToDate,
        tmpExpected.Count, tmpCached.Count, tmpMarkerRows.Count);
      return 0;
    }

    // 5. Coalesce missing days into contiguous trading-day ranges. Two
    //    days are contiguous iff there are no trading days strictly
    //    between them (weekends / holidays in between are fine — they
    //    are not trading days).
    var tmpRanges = CoalesceContiguousTradingDays(tmpMissing);
    m_Logger.LogInformation(
      "Chain gap detected for {Symbol} {From}..{To}: {Missing} missing days in {Ranges} contiguous ranges (expected={Expected}, cached={Cached}, marked-ranges={Markers})",
      inSymbol, inFromDate, inToDate,
      tmpMissing.Count, tmpRanges.Count,
      tmpExpected.Count, tmpCached.Count, tmpMarkerRows.Count);

    // 6. Per-day fetch. Polygon's chain endpoint is single-day; for each
    //    contiguous gap-range, walk every trading day in the range and
    //    fetch. Track which days came back empty inside each range so
    //    we can mark contiguous empty runs as one row.
    //
    //    Each per-day fetch+persist runs through the shared
    //    GapLockExecutor on (symbol, day) so two concurrent
    //    EnsureRangeCachedAsync callers whose ranges overlap collapse
    //    day-by-day where they share gaps.
    var tmpUpstreamCalls = 0;
    var tmpEmptyDaysBag = new System.Collections.Concurrent.ConcurrentBag<DateOnly>();
    foreach (var tmpRange in tmpRanges)
    {
      foreach (var tmpDay in TradingCalendar.EnumerateTradingDays(tmpRange.From, tmpRange.To))
      {
        var tmpDayKey = new ChainGapKey(inSymbol, tmpDay);
        await m_GapLock.ExecuteFetchAndPersistAsync(
          tmpDayKey,
          async () =>
          {
            Interlocked.Increment(ref tmpUpstreamCalls);
            var tmpFetched = await m_ChainFetcher.FetchChainAsync(inSymbol, tmpDay, inCt)
              .ConfigureAwait(false);
            if (tmpFetched.Count == 0)
            {
              tmpEmptyDaysBag.Add(tmpDay);
              return;
            }
            // Open a dedicated connection for the persist step. Holding
            // tmpConn across the GapLockExecutor body would be safe
            // (single thread per connection) but we keep the persist
            // connection separate so a future async fan-out across days
            // does not need to reason about connection sharing.
            await using var tmpPersistConn = new NpgsqlConnection(m_ConnectionString);
            await tmpPersistConn.OpenAsync(inCt).ConfigureAwait(false);
            await UpsertChainAsync(tmpPersistConn, inSymbol, tmpDay, tmpFetched, inCt);
          }).ConfigureAwait(false);
      }
    }
    var tmpEmptyDays = tmpEmptyDaysBag.ToList();

    // 7. Re-coalesce the empty-day set across all ranges (safety net for
    //    a fetcher returning empty for some-but-not-all days inside one
    //    gap range — split into the actual contiguous-empty sub-runs).
    if (tmpEmptyDays.Count > 0)
    {
      var tmpEmptyRanges = CoalesceContiguousTradingDays(tmpEmptyDays);
      var tmpRangesUtc = tmpEmptyRanges
        .Select(r => (
          From: new DateTimeOffset(r.From.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
          To: new DateTimeOffset(r.To.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero)))
        .ToList<(DateTimeOffset From, DateTimeOffset To)>();

      var tmpKeyValues = new[]
      {
        new KeyValuePair<string, object>("Symbol", inSymbol),
      };

      // Adjacency = 1 day in ticks. Two markers separated by exactly
      // 1 day (e.g. Dec 24 and Dec 26 with Christmas in between as a
      // non-trading day) collapse on-write. Cross-run merging happens
      // here via RangeMarkerWriter against existing rows.
      var tmpFinalCount = await RangeMarkerWriter.WriteAsync(
        tmpConn, ChainsMissTableSpec, tmpKeyValues,
        tmpRangesUtc, "no-data-from-polygon",
        inAdjacencyTicks: TimeSpan.FromDays(1).Ticks,
        inCt: inCt).ConfigureAwait(false);

      m_Metrics?.RecordMissMarker(MetricKind.Chains);
      m_Logger.LogInformation(
        "Recorded {Ranges} chain miss-marker range(s) for {Symbol} (table now has {Total} rows for this key)",
        tmpEmptyRanges.Count, inSymbol, tmpFinalCount);
    }

    return tmpUpstreamCalls;
  }

  /// <summary>
  /// Coalesce a set of trading days into the minimal set of contiguous
  /// ranges. Two days are "contiguous" iff there are no trading days
  /// strictly between them — weekends and holidays in between are fine
  /// (they are not trading days, so the gap is preserved as zero-trading
  /// days). Internal so unit tests can pin the math directly.
  /// </summary>
  internal static List<(DateOnly From, DateOnly To)> CoalesceContiguousTradingDays(
    IEnumerable<DateOnly> inDays)
  {
    var tmpSorted = inDays.Distinct().OrderBy(d => d).ToList();
    if (tmpSorted.Count == 0) return new List<(DateOnly, DateOnly)>();

    var tmpResult = new List<(DateOnly From, DateOnly To)>();
    var tmpRangeStart = tmpSorted[0];
    var tmpRangeEnd = tmpSorted[0];
    for (var i = 1; i < tmpSorted.Count; i++)
    {
      var tmpDay = tmpSorted[i];
      // Contiguous iff there is no trading day strictly between
      // tmpRangeEnd and tmpDay.
      var tmpHasGap = false;
      for (var tmpProbe = tmpRangeEnd.AddDays(1); tmpProbe < tmpDay; tmpProbe = tmpProbe.AddDays(1))
      {
        if (TradingCalendar.IsTradingDay(tmpProbe)) { tmpHasGap = true; break; }
      }
      if (!tmpHasGap)
      {
        tmpRangeEnd = tmpDay;
      }
      else
      {
        tmpResult.Add((tmpRangeStart, tmpRangeEnd));
        tmpRangeStart = tmpDay;
        tmpRangeEnd = tmpDay;
      }
    }
    tmpResult.Add((tmpRangeStart, tmpRangeEnd));
    return tmpResult;
  }

  private async Task<bool> IsKnownMissAsync(
    NpgsqlConnection inConn, string inSymbol, DateOnly inAsOfDate)
  {
    // Range-shape lookup (post-PR #21): a row with
    // range_from <= as_of <= range_to means we already tried this day
    // (or a contiguous run including it) and upstream had nothing.
    var tmpHit = await inConn.ExecuteScalarAsync<int?>(
      """
      SELECT 1 FROM historical_options_chains_misses
      WHERE symbol = @Symbol
        AND range_from <= @Date::date
        AND range_to >= @Date::date
      LIMIT 1
      """,
      new { Symbol = inSymbol, Date = inAsOfDate.ToString("yyyy-MM-dd") });
    return tmpHit == 1;
  }

  private async Task DoEnsureAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
  {
    await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
    await tmpConn.OpenAsync(inCt);

    var tmpDateStr = inAsOfDate.ToString("yyyy-MM-dd");

    // 1. Already cached?
    var tmpHave = await tmpConn.QuerySingleAsync<bool>(
      """
      SELECT EXISTS (
        SELECT 1 FROM historical_options_contracts
        WHERE underlying_ticker = @Symbol
          AND as_of_date = @Date::date
      )
      """,
      new { Symbol = inSymbol, Date = tmpDateStr });
    if (tmpHave) return;

    // 2. Permanent miss-marker?
    var tmpMissed = await IsKnownMissAsync(tmpConn, inSymbol, inAsOfDate);
    if (tmpMissed)
    {
      m_Logger.LogDebug(
        "Chain miss-marker for {Symbol} as_of {AsOf} — skipping Polygon fetch",
        inSymbol, inAsOfDate);
      return;
    }

    // 3. Polygon fetch + upsert.
    var tmpFetched = await m_ChainFetcher.FetchChainAsync(inSymbol, inAsOfDate, inCt);

    if (tmpFetched.Count == 0)
    {
      // 4. Empty → write degenerate 1-day range marker via the
      //    coalesce-on-write helper. Adjacent existing markers (within
      //    1 calendar day) merge into a single row, so sequential
      //    per-day writes for contiguous trading days collapse over
      //    time. Same principle as PR #20 for NBBO.
      await RecordChainMissAsync(tmpConn, inSymbol, inAsOfDate, "no-data-from-polygon", inCt);
      return;
    }

    await UpsertChainAsync(tmpConn, inSymbol, inAsOfDate, tmpFetched, inCt);
  }

  /// <summary>
  /// Idempotent insert of a fetched chain into
  /// historical_options_contracts. ON CONFLICT (as_of_date, ticker) DO
  /// NOTHING means a re-run on the same day is a no-op rather than a
  /// constraint failure — same shape as MBD ContractsBackfillService.
  /// </summary>
  private async Task UpsertChainAsync(
    NpgsqlConnection inConn, string inSymbol, DateOnly inAsOfDate,
    IReadOnlyList<TreyThomasCodes.Polygon.Models.Options.OptionsContract> inContracts,
    CancellationToken inCt)
  {
    var tmpInserted = 0;
    foreach (var tmpC in inContracts)
    {
      if (string.IsNullOrEmpty(tmpC.Ticker)) continue;
      if (!DateOnly.TryParse(tmpC.ExpirationDate, out var tmpExp)) continue;

      await using var tmpCmd = new NpgsqlCommand(
        """
        INSERT INTO historical_options_contracts
          (as_of_date, ticker, underlying_ticker, contract_type,
           exercise_style, expiration_date, strike_price,
           shares_per_contract, primary_exchange)
        VALUES (@asof, @ticker, @under, @ctype, @style, @exp,
                @strike, @shares, @ex)
        ON CONFLICT (as_of_date, ticker) DO NOTHING
        """, inConn);
      tmpCmd.Parameters.AddWithValue("asof", inAsOfDate);
      tmpCmd.Parameters.AddWithValue("ticker", tmpC.Ticker);
      tmpCmd.Parameters.AddWithValue("under",
        (object?)tmpC.UnderlyingTicker ?? (object)inSymbol);
      tmpCmd.Parameters.AddWithValue("ctype",
        (object?)tmpC.ContractType ?? DBNull.Value);
      tmpCmd.Parameters.AddWithValue("style",
        (object?)tmpC.ExerciseStyle ?? DBNull.Value);
      tmpCmd.Parameters.AddWithValue("exp", tmpExp);
      tmpCmd.Parameters.AddWithValue("strike",
        (object?)tmpC.StrikePrice ?? DBNull.Value);
      tmpCmd.Parameters.AddWithValue("shares",
        (object?)tmpC.SharesPerContract ?? DBNull.Value);
      tmpCmd.Parameters.AddWithValue("ex", DBNull.Value);

      tmpInserted += await tmpCmd.ExecuteNonQueryAsync(inCt);
    }

    m_Logger.LogInformation(
      "On-demand chain fill: upserted {Inserted}/{Fetched} contracts for {Symbol} as_of {AsOf}",
      tmpInserted, inContracts.Count, inSymbol, inAsOfDate);
  }

  /// <summary>
  /// Write a permanently-unavailable marker via
  /// <see cref="RangeMarkerWriter"/> as a degenerate 1-day range. The
  /// writer coalesces adjacent existing markers (within
  /// <c>TimeSpan.FromDays(1).Ticks</c> of an existing range bound) so
  /// sequential per-day writes for contiguous trading days collapse
  /// into one range row over time.
  /// </summary>
  private async Task RecordChainMissAsync(
    NpgsqlConnection inConn, string inSymbol, DateOnly inAsOfDate,
    string inReason, CancellationToken inCt)
  {
    try
    {
      var tmpDt = new DateTimeOffset(inAsOfDate.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
      await RangeMarkerWriter.WriteAsync(
        inConn, ChainsMissTableSpec,
        inKeyValues: new[]
        {
          new KeyValuePair<string, object>("Symbol", inSymbol),
        },
        inNewRanges: new[] { (tmpDt, tmpDt) },
        inReason: inReason,
        inAdjacencyTicks: TimeSpan.FromDays(1).Ticks,
        inCt: inCt).ConfigureAwait(false);

      m_Metrics?.RecordMissMarker(MetricKind.Chains);
      m_Logger.LogInformation(
        "Recorded chain miss-marker {Symbol} as_of {AsOf} ({Reason})",
        inSymbol, inAsOfDate, inReason);
    }
    catch (Exception ex)
    {
      // Don't break the response just because miss-marker insert
      // failed (e.g. table doesn't exist on a brand-new DB). Log
      // and continue.
      m_Logger.LogWarning(ex,
        "Failed to record chain miss-marker for {Symbol} as_of {AsOf}",
        inSymbol, inAsOfDate);
    }
  }

  /// <summary>
  /// Schema descriptor for <c>historical_options_chains_misses</c>
  /// (range-shape post migration 010). Bound at class scope so tests
  /// can refer to it for setup.
  /// </summary>
  internal static readonly RangeMarkerTableSpec ChainsMissTableSpec = new(
    TableName: "historical_options_chains_misses",
    KeyColumns: new[] { "symbol" },
    RangeFromColumn: "range_from",
    RangeToColumn: "range_to",
    FetchedAtColumn: "fetched_at",
    HasReasonColumn: true,
    ReasonColumn: "reason",
    RangeColumnType: RangeMarkerColumnType.Date);

  /// <summary>Internal mapping row for the marker-shadow read in
  /// EnsureRangeCachedAsync. Constructed from text-cast date columns
  /// (Dapper's DATE binding lands as DateTime; we keep DateOnly here
  /// to match the rest of the code path).</summary>
  internal sealed record ChainMissRow(DateOnly RangeFrom, DateOnly RangeTo);
}
