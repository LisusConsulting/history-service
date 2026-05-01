using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MomentumBreakoutDetector.HistoryService.Fetchers;
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
  ///   4. If the fetch returned empty → write miss-marker.
  /// </summary>
  Task EnsureChainCachedAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt);
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
/// </summary>
public sealed class OptionChainProvider : IOptionChainProvider
{
  private readonly string m_ConnectionString;
  private readonly ILogger<OptionChainProvider> m_Logger;
  private readonly IPolygonChainFetcher m_ChainFetcher;

  /// <summary>
  /// In-flight de-duplication: many concurrent requests on the same
  /// (symbol, as_of) — typical at backtest cold-start — share a single
  /// Polygon fetch. Keyed lock; subsequent callers await the same Task
  /// and read from the now-warm DB.
  /// </summary>
  private readonly Dictionary<(string, DateOnly), Task> m_FetchInflight = new();
  private readonly object m_FetchInflightLock = new();

  public OptionChainProvider(
    IOptions<HistoryServiceOptions> inOptions,
    ILogger<OptionChainProvider> inLogger,
    IPolygonChainFetcher inChainFetcher)
  {
    m_ConnectionString = inOptions.Value.ConnectionString;
    m_Logger = inLogger;
    m_ChainFetcher = inChainFetcher;
  }

  public async Task<OptionChainResult> GetChainAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
  {
    if (string.IsNullOrEmpty(inSymbol))
    {
      return new OptionChainResult(
        Array.Empty<OptionContractRow>(), CacheHit: true, IsMissMarker: false);
    }

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
      // 2. Miss-marker pre-check. A row in historical_options_chains_misses
      //    means we already tried Polygon and got nothing — return empty
      //    with IsMissMarker=true and skip the fetch.
      var tmpMissed = await tmpConn.QuerySingleAsync<bool>(
        """
        SELECT EXISTS (
          SELECT 1 FROM historical_options_chains_misses
          WHERE symbol = @Symbol
            AND as_of_date = @Date::date
        )
        """,
        new { Symbol = inSymbol, Date = tmpDateStr });

      if (tmpMissed)
      {
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

    return new OptionChainResult(tmpList, CacheHit: tmpCacheHit, IsMissMarker: tmpIsMiss);
  }

  // ── on-demand chain fetch (lifted from MBD picker, PR #130) ──────────

  public async Task EnsureChainCachedAsync(
    string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
  {
    var tmpKey = (inSymbol, inAsOfDate);

    // In-flight de-dup. We hold the lock only long enough to claim or
    // join the in-flight Task; the actual await happens outside the lock.
    Task tmpTask;
    lock (m_FetchInflightLock)
    {
      if (!m_FetchInflight.TryGetValue(tmpKey, out var tmpExisting))
      {
        tmpExisting = DoEnsureAsync(inSymbol, inAsOfDate, inCt);
        m_FetchInflight[tmpKey] = tmpExisting;
      }
      tmpTask = tmpExisting;
    }

    try
    {
      await tmpTask;
    }
    finally
    {
      lock (m_FetchInflightLock)
      {
        if (m_FetchInflight.TryGetValue(tmpKey, out var tmpExisting)
            && ReferenceEquals(tmpExisting, tmpTask))
        {
          m_FetchInflight.Remove(tmpKey);
        }
      }
    }
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
    var tmpMissed = await tmpConn.QuerySingleAsync<bool>(
      """
      SELECT EXISTS (
        SELECT 1 FROM historical_options_chains_misses
        WHERE symbol = @Symbol
          AND as_of_date = @Date::date
      )
      """,
      new { Symbol = inSymbol, Date = tmpDateStr });
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
      // 4. Empty → write miss-marker. Reasons: 4xx, plan-tier limit,
      //    weekend/holiday, date predates listing.
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
  /// Write a permanently-unavailable marker so subsequent runs skip
  /// re-fetching. Idempotent — same (symbol, as_of_date) is a no-op.
  /// </summary>
  private async Task RecordChainMissAsync(
    NpgsqlConnection inConn, string inSymbol, DateOnly inAsOfDate,
    string inReason, CancellationToken inCt)
  {
    await inConn.ExecuteAsync(
      """
      INSERT INTO historical_options_chains_misses
        (symbol, as_of_date, reason, fetched_at)
      VALUES (@Symbol, @Date::date, @Reason, NOW())
      ON CONFLICT (symbol, as_of_date) DO NOTHING
      """,
      new { Symbol = inSymbol, Date = inAsOfDate.ToString("yyyy-MM-dd"), Reason = inReason });
    m_Logger.LogInformation(
      "Recorded chain miss-marker {Symbol} as_of {AsOf} ({Reason})",
      inSymbol, inAsOfDate, inReason);
  }
}
