using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// One row of <c>intraday_atm_iv</c> (migration 016). Mirrors the DB
/// schema verbatim; the gRPC layer maps to
/// <see cref="Contracts.V1.IntradayAtmIvRow"/> at the edge.
/// </summary>
/// <remarks>
/// Unlike <see cref="DailyAtmIvRow"/>, both <see cref="AtmIv"/> and
/// <see cref="ContractCount"/> are NOT nullable — the live engine only
/// calls <see cref="IIntradayAtmIvProvider.RecordAsync"/> when it has a
/// valid (non-zero) reading with at least one contributing contract.
/// </remarks>
public sealed record IntradayAtmIvRow(
    string UnderlyingTicker,
    DateTime CapturedAt,
    decimal AtmIv,
    int ContractCount);

/// <summary>
/// HWZ-36 — write + read surface for <c>intraday_atm_iv</c>. Replaces
/// the temporary direct-Npgsql shortcut the live engine used during
/// Phase B.2/B.3 (committed in <c>SignalSourcesService</c> +
/// <c>BacktestEngine</c>; removed by this PR).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this surface is not past-only.</b> Unlike daily_atm_iv (which
/// is strictly historical aggregates), intraday_atm_iv is captured by
/// the LIVE engine right now and read back by backtests of today / very
/// recent days. The whole point of the surface is to let backtests
/// replay against the same intraday IV reading the live engine actually
/// saw. The no-lookahead invariant is enforced implicitly by the
/// at-or-before semantic on the read path — callers can never observe a
/// row stamped later than the timestamp they asked about.
/// </para>
///
/// <para>
/// <b>Concurrency.</b> The PRIMARY KEY (underlying_ticker, captured_at)
/// makes <see cref="RecordAsync"/> a naturally-serialized upsert; no
/// advisory lock needed. Two concurrent writers at the exact same
/// captured_at would conflict and one would block briefly, then the
/// ON CONFLICT branch would idempotently bring the row to the latest
/// values. In practice the live engine's refresh cadence is ~5 min so
/// concurrent writes don't happen.
/// </para>
/// </remarks>
public interface IIntradayAtmIvProvider
{
    /// <summary>
    /// HWZ-36 — write path. Idempotent UPSERT keyed on
    /// (underlying_ticker, captured_at). Returns true iff a new row was
    /// inserted (false = an existing row at the same timestamp was
    /// upserted to the request values).
    /// </summary>
    Task<bool> RecordAsync(
        string inSymbol, DateTime inCapturedAtUtc,
        decimal inAtmIv, int inContractCount,
        CancellationToken inCt = default);

    /// <summary>
    /// HWZ-36 — at-or-before lookup. Returns the most recent row whose
    /// <c>captured_at</c> is &lt;= <paramref name="inAtOrBeforeUtc"/>
    /// for the requested underlying, or null when none exists (the
    /// requested window is before the earliest capture).
    /// </summary>
    Task<IntradayAtmIvRow?> GetAtOrBeforeAsync(
        string inSymbol, DateTime inAtOrBeforeUtc,
        CancellationToken inCt = default);

    /// <summary>
    /// HWZ-36 — range read. Returns all rows for
    /// <paramref name="inSymbol"/> with <c>captured_at BETWEEN
    /// inFromUtc AND inToUtc</c> (inclusive), sorted ascending by
    /// captured_at. Backtest's primary read path: one call per run,
    /// then client-side binary search per bar.
    /// </summary>
    Task<IReadOnlyList<IntradayAtmIvRow>> ListRangeAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        CancellationToken inCt = default);
}

/// <summary>
/// Postgres-backed reader + writer for <c>intraday_atm_iv</c>.
/// </summary>
public sealed class IntradayAtmIvProvider : IIntradayAtmIvProvider
{
    private readonly string m_ConnectionString;
    private readonly ILogger<IntradayAtmIvProvider> m_Logger;

    public IntradayAtmIvProvider(
        IOptions<HistoryServiceOptions> inOptions,
        ILogger<IntradayAtmIvProvider> inLogger)
        : this(inOptions.Value.ConnectionString, inLogger)
    {
    }

    /// <summary>Test-friendly ctor that takes the connection string directly.</summary>
    public IntradayAtmIvProvider(
        string inConnectionString,
        ILogger<IntradayAtmIvProvider> inLogger)
    {
        m_ConnectionString = inConnectionString;
        m_Logger = inLogger;
    }

    /// <inheritdoc />
    public async Task<bool> RecordAsync(
        string inSymbol, DateTime inCapturedAtUtc,
        decimal inAtmIv, int inContractCount,
        CancellationToken inCt = default)
    {
        if (string.IsNullOrWhiteSpace(inSymbol))
            throw new ArgumentException("symbol is required", nameof(inSymbol));

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        await using var tmpCmd = new NpgsqlCommand(
            """
            INSERT INTO intraday_atm_iv
              (underlying_ticker, captured_at, atm_iv, contract_count)
            VALUES (@sym, @ts, @iv, @cnt)
            ON CONFLICT (underlying_ticker, captured_at) DO UPDATE
              SET atm_iv = EXCLUDED.atm_iv,
                  contract_count = EXCLUDED.contract_count
            RETURNING (xmax = 0) AS inserted
            """, tmpConn);
        tmpCmd.Parameters.AddWithValue("sym", inSymbol);
        tmpCmd.Parameters.AddWithValue("ts",
            DateTime.SpecifyKind(inCapturedAtUtc, DateTimeKind.Utc));
        tmpCmd.Parameters.AddWithValue("iv", inAtmIv);
        tmpCmd.Parameters.AddWithValue("cnt", inContractCount);

        var tmpScalar = await tmpCmd.ExecuteScalarAsync(inCt).ConfigureAwait(false);
        var tmpInserted = tmpScalar is bool b && b;

        m_Logger.LogDebug(
            "IntradayAtmIv {Op}: {Sym}@{Ts:O} iv={Iv:P2} contracts={Cnt}",
            tmpInserted ? "INSERT" : "UPSERT",
            inSymbol, inCapturedAtUtc, inAtmIv, inContractCount);

        return tmpInserted;
    }

    /// <inheritdoc />
    public async Task<IntradayAtmIvRow?> GetAtOrBeforeAsync(
        string inSymbol, DateTime inAtOrBeforeUtc,
        CancellationToken inCt = default)
    {
        if (string.IsNullOrWhiteSpace(inSymbol)) return null;

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        await using var tmpCmd = new NpgsqlCommand(
            """
            SELECT underlying_ticker, captured_at, atm_iv, contract_count
            FROM intraday_atm_iv
            WHERE underlying_ticker = @sym
              AND captured_at <= @at
            ORDER BY captured_at DESC
            LIMIT 1
            """, tmpConn);
        tmpCmd.Parameters.AddWithValue("sym", inSymbol);
        tmpCmd.Parameters.AddWithValue("at",
            DateTime.SpecifyKind(inAtOrBeforeUtc, DateTimeKind.Utc));

        await using var tmpReader = await tmpCmd.ExecuteReaderAsync(inCt).ConfigureAwait(false);
        if (!await tmpReader.ReadAsync(inCt).ConfigureAwait(false))
            return null;

        return new IntradayAtmIvRow(
            UnderlyingTicker: tmpReader.GetString(0),
            CapturedAt: DateTime.SpecifyKind(tmpReader.GetDateTime(1), DateTimeKind.Utc),
            AtmIv: tmpReader.GetDecimal(2),
            ContractCount: tmpReader.GetInt32(3));
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<IntradayAtmIvRow>> ListRangeAsync(
        string inSymbol, DateTime inFromUtc, DateTime inToUtc,
        CancellationToken inCt = default)
    {
        if (string.IsNullOrWhiteSpace(inSymbol))
            return Array.Empty<IntradayAtmIvRow>();
        if (inFromUtc > inToUtc)
            return Array.Empty<IntradayAtmIvRow>();

        await using var tmpConn = new NpgsqlConnection(m_ConnectionString);
        await tmpConn.OpenAsync(inCt).ConfigureAwait(false);

        await using var tmpCmd = new NpgsqlCommand(
            """
            SELECT underlying_ticker, captured_at, atm_iv, contract_count
            FROM intraday_atm_iv
            WHERE underlying_ticker = @sym
              AND captured_at >= @from
              AND captured_at <= @to
            ORDER BY captured_at ASC
            """, tmpConn);
        tmpCmd.Parameters.AddWithValue("sym", inSymbol);
        tmpCmd.Parameters.AddWithValue("from",
            DateTime.SpecifyKind(inFromUtc, DateTimeKind.Utc));
        tmpCmd.Parameters.AddWithValue("to",
            DateTime.SpecifyKind(inToUtc, DateTimeKind.Utc));

        var tmpResult = new List<IntradayAtmIvRow>();
        await using var tmpReader = await tmpCmd.ExecuteReaderAsync(inCt).ConfigureAwait(false);
        while (await tmpReader.ReadAsync(inCt).ConfigureAwait(false))
        {
            tmpResult.Add(new IntradayAtmIvRow(
                UnderlyingTicker: tmpReader.GetString(0),
                CapturedAt: DateTime.SpecifyKind(tmpReader.GetDateTime(1), DateTimeKind.Utc),
                AtmIv: tmpReader.GetDecimal(2),
                ContractCount: tmpReader.GetInt32(3)));
        }

        m_Logger.LogInformation(
            "IntradayAtmIv list {Count} row(s) for {Symbol} {From:O}..{To:O}",
            tmpResult.Count, inSymbol, inFromUtc, inToUtc);

        return tmpResult;
    }
}
