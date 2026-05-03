using Dapper;
using MomentumBreakoutDetector.HistoryService.Concurrency;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Observability;
using Npgsql;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Per-series cadence metadata used by the gap-detection logic in
/// <see cref="MacroDataProvider.EnsureRangeCachedAsync"/>. Cadence cannot
/// be inferred from the data alone (an empty cache for a daily series
/// looks identical to an empty cache for a monthly series until FRED is
/// queried), so we hard-code the FRED catalogue's published cadence for
/// the series the runtime cares about.
/// </summary>
public enum FredSeriesCadence
{
    /// <summary>Business-day cadence — Mon-Fri, skipping market holidays.
    /// Examples: T10Y2Y, DGS10. A "gap" means the cache lacks rows for one
    /// or more weekdays in the window.</summary>
    Daily,

    /// <summary>Monthly cadence — one observation per month, typically the
    /// first day of the month. Examples: CPIAUCSL, UNRATE. A "gap" means
    /// the cache lacks rows for one or more month boundaries in the window.</summary>
    Monthly,
}

public interface IMacroDataProvider
{
    Task EnsureRangeCachedAsync(
        string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct);

    Task EnsureRangeCachedAsync(
        IEnumerable<string> seriesIds, DateOnly fromDate, DateOnly toDate, CancellationToken ct);

    Task<IReadOnlyList<FredObservationRow>> GetSeriesAsync(
        string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct);
}

/// <summary>
/// Lifted from MBD's PostgresMacroDataProvider (PR D, 2026-04-30). Reads
/// <c>macro_data</c> from PostgreSQL with on-demand FRED fetch when the
/// cached range is incomplete. Idempotent upsert; range-shape miss-marker
/// table for permanently-unavailable observations.
///
/// <para>
/// Cadence-aware gap detection: T10Y2Y publishes Mon-Fri (skipping
/// market holidays); CPIAUCSL and UNRATE publish monthly. A naive
/// "fetch every weekday with no row" loop would endlessly re-fetch
/// monthly series. The cadence map below tells the gap detector how
/// many points it should expect in a given window — short of that,
/// fetch the missing range from FRED.
/// </para>
///
/// <para>
/// <b>Range markers (post 2026-05-02 / PR #22).</b>
/// <c>macro_data_misses</c> migrated from point-shape
/// (series_id, observation_date) → range-shape
/// (series_id, range_from, range_to) in migration 011. The gap detector
/// now uses an expected-minus-cached-minus-marked set diff (matching
/// PRs #19 / #20 / #21) instead of count-based comparison: enumerate the
/// expected publication boundaries (Daily = every Mon-Fri; Monthly =
/// first-of-month), drop dates already cached, drop dates shadowed by an
/// existing marker range, and fetch what is left. Empty FRED responses
/// produce ONE marker row per contiguous run via
/// <see cref="RangeMarkerWriter"/> (coalesce-on-write merges adjacent
/// existing markers).
/// </para>
/// </summary>
/// <summary>
/// Gap-range identity for the macro cache. Two concurrent
/// <see cref="MacroDataProvider.EnsureRangeCachedAsync(string, DateOnly, DateOnly, CancellationToken)"/>
/// callers requesting overlapping ranges for the same series collapse on
/// this key via the <see cref="GapLockExecutor{TKey}"/>: only one runs the
/// FRED fetch + persist; the other awaits and re-reads.
/// </summary>
internal sealed record MacroGapKey(
    string SeriesId, DateOnly FromDate, DateOnly ToDate);

public sealed class MacroDataProvider : IMacroDataProvider
{
    private readonly string _connectionString;
    private readonly ILogger<MacroDataProvider> _logger;
    private readonly IFredFetcher? _fredFetcher;
    private readonly MetricsCollector? _metrics;
    private readonly GapLockExecutor<MacroGapKey> _gapLock = new();

    /// <summary>
    /// FRED series the runtime depends on, with their published cadence.
    /// Aligned with MBD's <c>MacroDataRefreshService.FRED_SERIES</c>:
    /// T10Y2Y feeds the yield-curve sub-factor; CPIAUCSL + UNRATE are
    /// macro-context inputs the live engine surfaces in the explain
    /// endpoint; DGS3MO (3-month T-bill yield) is the risk-free rate
    /// input for the Black-Scholes IV solver added in Wave A / PR 2 of
    /// the ATM-IV full historical coverage plan
    /// (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md).
    /// FRED publishes DGS3MO Mon-Fri (excluding federal holidays);
    /// "Daily" cadence is the correct gap-detection model. Keep this
    /// map in sync with the live refresh service if you add a series.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, FredSeriesCadence> KnownSeriesCadence =
        new Dictionary<string, FredSeriesCadence>(StringComparer.OrdinalIgnoreCase)
        {
            ["T10Y2Y"] = FredSeriesCadence.Daily,
            ["CPIAUCSL"] = FredSeriesCadence.Monthly,
            ["UNRATE"] = FredSeriesCadence.Monthly,
            ["DGS3MO"] = FredSeriesCadence.Daily,
        };

    public MacroDataProvider(
        string connectionString,
        ILogger<MacroDataProvider> logger,
        IFredFetcher? fredFetcher = null,
        MetricsCollector? metrics = null)
    {
        _connectionString = connectionString;
        _logger = logger;
        _fredFetcher = fredFetcher;
        _metrics = metrics;
    }

    /// <summary>
    /// Identify gaps in the macro cache for the requested range, fetch
    /// them from FRED (concurrency-gated by <see cref="IFredFetcher"/>),
    /// upsert into <c>macro_data</c>, record range-shape miss-markers for
    /// empty returns. Cold-start (empty table) collapses to a single
    /// full-range fetch per series; warm cache short-circuits with no
    /// FRED calls.
    /// </summary>
    public Task EnsureRangeCachedAsync(
        string seriesId, DateOnly fromDate, DateOnly toDate,
        CancellationToken ct)
    {
        _metrics?.RecordRequest(MetricKind.Macro);
        if (_fredFetcher is null) return Task.CompletedTask;
        if (fromDate > toDate) return Task.CompletedTask;

        // GapLockExecutor wraps the whole fetch+persist for this
        // (seriesId, fromDate, toDate) gap. Two concurrent callers asking
        // for the same series + overlapping windows collapse here: only
        // one runs the FRED fetch + the marker write; the other awaits.
        // The cross-replica advisory lock is taken inside the persist
        // step (DoEnsureRangeCachedAsync writes data + markers under
        // RangeMarkerWriter's own pg_advisory_xact_lock).
        var tmpKey = new MacroGapKey(seriesId, fromDate, toDate);
        return _gapLock.ExecuteFetchAndPersistAsync(
            tmpKey,
            () => DoEnsureRangeCachedAsync(seriesId, fromDate, toDate, ct));
    }

    private async Task DoEnsureRangeCachedAsync(
        string seriesId, DateOnly fromDate, DateOnly toDate,
        CancellationToken ct)
    {
        if (_fredFetcher is null) return;

        if (!KnownSeriesCadence.TryGetValue(seriesId, out var cadence))
        {
            // Unknown series — default to daily cadence. Better to over-fetch
            // than to silently mis-model. Logged once per unknown series so
            // ops can update the catalogue.
            _logger.LogWarning(
                "Unknown FRED series cadence for {Series}; defaulting to Daily",
                seriesId);
            cadence = FredSeriesCadence.Daily;
        }

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // 1. Expected publication boundaries.
        var expected = EnumerateBoundaries(fromDate, toDate, cadence).ToList();
        if (expected.Count == 0)
        {
            _logger.LogDebug(
                "No expected boundaries for {Series} {From}..{To} ({Cadence}) — skip",
                seriesId, fromDate, toDate, cadence);
            return;
        }

        // 2. Cached observation dates. Read DATE column as text + parse so
        //    Dapper does not fight Npgsql's DATE → DateOnly mapping.
        var cachedRows = await conn.QueryAsync<string>(
            """
            SELECT observation_date::text FROM macro_data
            WHERE series_id = @SeriesId
              AND observation_date >= @From::date AND observation_date <= @To::date
            """,
            new
            {
                SeriesId = seriesId,
                From = fromDate.ToString("yyyy-MM-dd"),
                To = toDate.ToString("yyyy-MM-dd"),
            });
        var cached = new HashSet<DateOnly>(cachedRows.Select(d => DateOnly.Parse(d)));

        // 3. Existing range markers that overlap the window.
        var markerRows = (await conn.QueryAsync<(string RangeFrom, string RangeTo)>(
            """
            SELECT range_from::text AS RangeFrom, range_to::text AS RangeTo
            FROM macro_data_misses
            WHERE series_id = @SeriesId
              AND range_to >= @From::date AND range_from <= @To::date
            """,
            new
            {
                SeriesId = seriesId,
                From = fromDate.ToString("yyyy-MM-dd"),
                To = toDate.ToString("yyyy-MM-dd"),
            }))
            .Select(r => (From: DateOnly.Parse(r.RangeFrom), To: DateOnly.Parse(r.RangeTo)))
            .ToList();

        // 4. expected − cached − marked.
        var missing = new List<DateOnly>(expected.Count);
        foreach (var date in expected)
        {
            if (cached.Contains(date)) continue;
            var shadowed = false;
            foreach (var marker in markerRows)
            {
                if (date >= marker.From && date <= marker.To) { shadowed = true; break; }
            }
            if (!shadowed) missing.Add(date);
        }

        if (missing.Count == 0)
        {
            _metrics?.RecordCacheHit(MetricKind.Macro);
            _logger.LogDebug(
                "Macro cache fully covers {Series} {From}..{To} ({Cadence}) (expected={Expected}, cached={Cached}, marked-ranges={Markers}) — no FRED fetch",
                seriesId, fromDate, toDate, cadence, expected.Count, cached.Count, markerRows.Count);
            return;
        }

        _logger.LogInformation(
            "Macro gap for {Series} {From}..{To} ({Cadence}): {Missing} boundaries missing (expected={Expected}, cached={Cached}, marked-ranges={Markers}) — fetching from FRED",
            seriesId, fromDate, toDate, cadence,
            missing.Count, expected.Count, cached.Count, markerRows.Count);

        // 5. Fetch the full window from FRED. FRED's range fetch returns
        //    every observation in [from, to] in a single call — no need
        //    to chunk. Using fromDate/toDate gives FRED the broadest
        //    window; we filter cached/marked locally.
        var fetched = await _fredFetcher.FetchSeriesAsync(seriesId, fromDate, toDate, ct);

        // 6. Upsert real values; collect FRED-null observations + boundaries
        //    FRED did not return at all into a single set for marker writes.
        var upserts = 0;
        var nullValueDates = new List<DateOnly>();
        var returnedDates = new HashSet<DateOnly>();
        foreach (var row in fetched)
        {
            returnedDates.Add(row.ObservationDate);
            if (row.Value is null)
            {
                nullValueDates.Add(row.ObservationDate);
            }
            else
            {
                await UpsertObservationAsync(conn, seriesId, row.ObservationDate, row.Value.Value, ct);
                upserts++;
            }
        }

        // Boundaries inside the missing-set that FRED did not return at all.
        var notReturned = missing.Where(d => !returnedDates.Contains(d)).ToList();

        // 7. Coalesce-on-write all marker dates as range markers. Two
        //    sources combine: FRED's null-value dates + boundaries-not-
        //    returned. Coalesce contiguous publication boundaries into one
        //    range row per cadence.
        var markerDates = new List<DateOnly>(nullValueDates.Count + notReturned.Count);
        markerDates.AddRange(nullValueDates);
        markerDates.AddRange(notReturned);
        var rangesWritten = 0;
        if (markerDates.Count > 0)
        {
            var ranges = CoalesceContiguousBoundaries(markerDates, cadence);
            var rangesUtc = ranges
                .Select(r => (
                    From: new DateTimeOffset(r.From.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
                    To: new DateTimeOffset(r.To.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero)))
                .ToList<(DateTimeOffset From, DateTimeOffset To)>();

            // Adjacency for cross-run coalesce-on-write:
            //   Daily: 3 days (covers a Fri..Mon weekend gap so Friday's
            //     marker and Monday's marker from two separate runs merge).
            //   Monthly: 32 days (covers a month-to-month succession).
            var adjacencyTicks = cadence == FredSeriesCadence.Daily
                ? TimeSpan.FromDays(3).Ticks
                : TimeSpan.FromDays(32).Ticks;

            var keyValues = new[]
            {
                new KeyValuePair<string, object>("SeriesId", seriesId),
            };

            rangesWritten = await RangeMarkerWriter.WriteAsync(
                conn, MacroMissTableSpec, keyValues,
                rangesUtc, "fred-no-data",
                inAdjacencyTicks: adjacencyTicks,
                inCt: ct).ConfigureAwait(false);

            _metrics?.RecordMissMarker(MetricKind.Macro);
        }

        _logger.LogInformation(
            "Macro on-demand fill: {Series} {From}..{To} → {Upserts} values upserted, {NullValues} fred-null dates, {NotReturned} not-returned dates, {RangesWritten} marker ranges in table",
            seriesId, fromDate, toDate, upserts, nullValueDates.Count, notReturned.Count, rangesWritten);
    }

    /// <inheritdoc/>
    public async Task EnsureRangeCachedAsync(
        IEnumerable<string> seriesIds, DateOnly fromDate, DateOnly toDate,
        CancellationToken ct)
    {
        foreach (var sid in seriesIds)
        {
            await EnsureRangeCachedAsync(sid, fromDate, toDate, ct);
        }
    }

    /// <summary>
    /// Read all cached observations for <paramref name="seriesId"/> in
    /// [<paramref name="fromDate"/>, <paramref name="toDate"/>]. Pure read
    /// — no FRED side-effects. Use after <see cref="EnsureRangeCachedAsync(string, DateOnly, DateOnly, CancellationToken)"/>
    /// to retrieve the warmed cache.
    /// </summary>
    public async Task<IReadOnlyList<FredObservationRow>> GetSeriesAsync(
        string seriesId, DateOnly fromDate, DateOnly toDate,
        CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // Read observation_date as text + parse client-side; the Dapper
        // binder doesn't natively round-trip DateOnly.
        var rows = await conn.QueryAsync<(string SeriesId, string ObsDate, decimal? Value)>(
            """
            SELECT series_id, observation_date::text AS ObsDate, value AS Value
            FROM macro_data
            WHERE series_id = @SeriesId
              AND observation_date >= @From::date AND observation_date <= @To::date
            ORDER BY observation_date
            """,
            new { SeriesId = seriesId, From = fromDate.ToString("yyyy-MM-dd"), To = toDate.ToString("yyyy-MM-dd") });

        return rows.Select(r => new FredObservationRow(
            r.SeriesId, DateOnly.Parse(r.ObsDate), r.Value)).ToList();
    }

    // ── DDL writers ──────────────────────────────────────────────────────

    private static async Task UpsertObservationAsync(
        NpgsqlConnection conn, string seriesId, DateOnly date, decimal value,
        CancellationToken ct)
    {
        await conn.ExecuteAsync(
            """
            INSERT INTO macro_data (series_id, observation_date, value)
            VALUES (@SeriesId, @Date::date, @Value)
            ON CONFLICT (series_id, observation_date) DO UPDATE SET value = EXCLUDED.value
            """,
            new { SeriesId = seriesId, Date = date.ToString("yyyy-MM-dd"), Value = value });
    }

    // ── Cadence math (pure, internal for tests) ─────────────────────────

    /// <summary>Count Mon-Fri days in [start, end] inclusive. Kept for
    /// callers (e.g. legacy expected-count math); not used by the new
    /// expected-set gap detector but stable as a pure helper.</summary>
    internal static int CountWeekdays(DateOnly start, DateOnly end)
    {
        if (start > end) return 0;
        var count = 0;
        for (var date = start; date <= end; date = date.AddDays(1))
        {
            if (date.DayOfWeek != DayOfWeek.Saturday && date.DayOfWeek != DayOfWeek.Sunday)
                count++;
        }
        return count;
    }

    /// <summary>
    /// Count distinct year-month pairs that have a first-of-month
    /// observation within [start, end] inclusive. Monthly FRED series
    /// publish on the first of each month, so the count equals the number
    /// of (year, month) intervals whose first day falls in the window.
    /// </summary>
    internal static int CountMonthBoundaries(DateOnly start, DateOnly end)
    {
        if (start > end) return 0;
        var count = 0;
        var date = new DateOnly(start.Year, start.Month, 1);
        if (date < start) date = date.AddMonths(1);
        while (date <= end)
        {
            count++;
            date = date.AddMonths(1);
        }
        return count;
    }

    /// <summary>Enumerate the expected publication boundaries in
    /// [start, end] for the given cadence. Daily yields every Mon-Fri;
    /// Monthly yields the first day of each month boundary in window.</summary>
    internal static IEnumerable<DateOnly> EnumerateBoundaries(
        DateOnly start, DateOnly end, FredSeriesCadence cadence)
    {
        if (start > end) yield break;
        if (cadence == FredSeriesCadence.Daily)
        {
            for (var date = start; date <= end; date = date.AddDays(1))
            {
                if (date.DayOfWeek != DayOfWeek.Saturday && date.DayOfWeek != DayOfWeek.Sunday)
                    yield return date;
            }
        }
        else
        {
            var date = new DateOnly(start.Year, start.Month, 1);
            if (date < start) date = date.AddMonths(1);
            while (date <= end)
            {
                yield return date;
                date = date.AddMonths(1);
            }
        }
    }

    /// <summary>
    /// Coalesce a set of publication boundaries into the minimal set of
    /// contiguous ranges per cadence. Two boundaries are "contiguous" iff
    /// no expected boundary of the given cadence falls strictly between
    /// them. Internal so unit tests can pin the math directly.
    ///
    /// <para>
    /// For Daily: Fri 4/17 + Mon 4/20 are contiguous (no business days
    /// strictly between — the weekend doesn't count). Tue 4/14 + Thu 4/16
    /// are NOT contiguous (Wed 4/15 is an expected boundary in between).
    /// </para>
    ///
    /// <para>
    /// For Monthly: Apr 1 + May 1 are contiguous. Apr 1 + Jun 1 are NOT
    /// (May 1 is an expected boundary in between).
    /// </para>
    /// </summary>
    internal static List<(DateOnly From, DateOnly To)> CoalesceContiguousBoundaries(
        IEnumerable<DateOnly> inDates, FredSeriesCadence inCadence)
    {
        var sorted = inDates.Distinct().OrderBy(d => d).ToList();
        if (sorted.Count == 0) return new List<(DateOnly, DateOnly)>();

        var result = new List<(DateOnly From, DateOnly To)>();
        var rangeStart = sorted[0];
        var rangeEnd = sorted[0];
        for (var i = 1; i < sorted.Count; i++)
        {
            var next = sorted[i];
            var hasBoundaryBetween = HasBoundaryStrictlyBetween(rangeEnd, next, inCadence);
            if (!hasBoundaryBetween)
            {
                rangeEnd = next;
            }
            else
            {
                result.Add((rangeStart, rangeEnd));
                rangeStart = next;
                rangeEnd = next;
            }
        }
        result.Add((rangeStart, rangeEnd));
        return result;
    }

    /// <summary>True iff there is at least one expected boundary of the
    /// given cadence strictly between <paramref name="inA"/> and
    /// <paramref name="inB"/> (exclusive at both ends).</summary>
    private static bool HasBoundaryStrictlyBetween(
        DateOnly inA, DateOnly inB, FredSeriesCadence inCadence)
    {
        if (inA >= inB) return false;
        var probeStart = inA.AddDays(1);
        var probeEnd = inB.AddDays(-1);
        if (probeStart > probeEnd) return false;
        foreach (var _ in EnumerateBoundaries(probeStart, probeEnd, inCadence))
        {
            return true;
        }
        return false;
    }

    /// <summary>
    /// Schema descriptor for <c>macro_data_misses</c> (range-shape post
    /// migration 011). DATE-typed range columns; the writer's
    /// <see cref="RangeMarkerColumnType.Date"/> spec ensures the
    /// timestamptz-cast on read and date-cast on write are wired.
    /// </summary>
    internal static readonly RangeMarkerTableSpec MacroMissTableSpec = new(
        TableName: "macro_data_misses",
        KeyColumns: new[] { "series_id" },
        RangeFromColumn: "range_from",
        RangeToColumn: "range_to",
        FetchedAtColumn: "fetched_at",
        HasReasonColumn: true,
        ReasonColumn: "reason",
        RangeColumnType: RangeMarkerColumnType.Date);
}
