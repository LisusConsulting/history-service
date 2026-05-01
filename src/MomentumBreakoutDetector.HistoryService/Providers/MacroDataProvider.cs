using Dapper;
using MomentumBreakoutDetector.HistoryService.Fetchers;
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
/// cached range is incomplete. Edge-only gap detection, idempotent
/// upsert, miss-marker table for permanently-unavailable observations.
///
/// <para>
/// Cadence-aware gap detection: T10Y2Y publishes Mon-Fri (skipping
/// market holidays); CPIAUCSL and UNRATE publish monthly. A naive
/// "fetch every weekday with no row" loop would endlessly re-fetch
/// monthly series. The cadence map below tells the gap detector how
/// many points it should expect in a given window — short of that,
/// fetch the missing range from FRED.
/// </para>
/// </summary>
public sealed class MacroDataProvider : IMacroDataProvider
{
    private readonly string _connectionString;
    private readonly ILogger<MacroDataProvider> _logger;
    private readonly IFredFetcher? _fredFetcher;

    /// <summary>
    /// FRED series the runtime depends on, with their published cadence.
    /// Aligned with MBD's <c>MacroDataRefreshService.FRED_SERIES</c>:
    /// T10Y2Y feeds the yield-curve sub-factor; CPIAUCSL + UNRATE are
    /// macro-context inputs the live engine surfaces in the explain
    /// endpoint. Keep this map in sync with the live refresh service if
    /// you add a series.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, FredSeriesCadence> KnownSeriesCadence =
        new Dictionary<string, FredSeriesCadence>(StringComparer.OrdinalIgnoreCase)
        {
            ["T10Y2Y"] = FredSeriesCadence.Daily,
            ["CPIAUCSL"] = FredSeriesCadence.Monthly,
            ["UNRATE"] = FredSeriesCadence.Monthly,
        };

    public MacroDataProvider(
        string connectionString,
        ILogger<MacroDataProvider> logger,
        IFredFetcher? fredFetcher = null)
    {
        _connectionString = connectionString;
        _logger = logger;
        _fredFetcher = fredFetcher;
    }

    /// <summary>
    /// Identify gaps in the macro cache for the requested range, fetch
    /// them from FRED (concurrency-gated by <see cref="IFredFetcher"/>),
    /// upsert into <c>macro_data</c>, record miss-markers for empty
    /// returns. Cold-start (empty table) collapses to a single full-range
    /// fetch per series; warm cache short-circuits with no FRED calls.
    /// </summary>
    public async Task EnsureRangeCachedAsync(
        string seriesId, DateOnly fromDate, DateOnly toDate,
        CancellationToken ct)
    {
        if (_fredFetcher is null) return;
        if (fromDate > toDate) return;

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

        // Cached + marker count. Pass DateOnly as ISO strings + ::date cast —
        // Dapper's binder doesn't support DateOnly natively in older Npgsql.
        var cachedCount = await conn.ExecuteScalarAsync<long>(
            """
            SELECT COUNT(*) FROM macro_data
            WHERE series_id = @SeriesId
              AND observation_date >= @From::date AND observation_date <= @To::date
            """,
            new { SeriesId = seriesId, From = fromDate.ToString("yyyy-MM-dd"), To = toDate.ToString("yyyy-MM-dd") });

        var markerCount = await conn.ExecuteScalarAsync<long>(
            """
            SELECT COUNT(*) FROM macro_data_misses
            WHERE series_id = @SeriesId
              AND observation_date >= @From::date AND observation_date <= @To::date
            """,
            new { SeriesId = seriesId, From = fromDate.ToString("yyyy-MM-dd"), To = toDate.ToString("yyyy-MM-dd") });

        var expected = cadence == FredSeriesCadence.Daily
            ? CountWeekdays(fromDate, toDate)
            : CountMonthBoundaries(fromDate, toDate);

        if (cachedCount + markerCount >= expected)
        {
            _logger.LogDebug(
                "Macro cache fully covers {Series} {From}..{To} ({Cached} rows + {Markers} markers >= {Expected} expected) — no FRED fetch",
                seriesId, fromDate, toDate, cachedCount, markerCount, expected);
            return;
        }

        _logger.LogInformation(
            "Macro cache gap for {Series} {From}..{To} ({Cached} rows + {Markers} markers < {Expected} expected) — fetching from FRED",
            seriesId, fromDate, toDate, cachedCount, markerCount, expected);

        var fetched = await _fredFetcher.FetchSeriesAsync(
            seriesId, fromDate, toDate, ct);

        if (fetched.Count == 0)
        {
            // FRED returned nothing for the entire window. Write a marker per
            // expected cadence boundary so we don't loop. Markers are
            // idempotent (PK).
            await RecordRangeMissAsync(conn, seriesId, fromDate, toDate, cadence,
                "no-data-from-fred", ct);
            return;
        }

        // Upsert observations. FRED's "." sentinel surfaces as null Value —
        // we record those as miss-markers (FRED knows the date is a
        // publication boundary but has no value to publish, e.g. holiday
        // on a daily series). Real values go to macro_data.
        var upserts = 0;
        var markers = 0;
        var returnedDates = new HashSet<DateOnly>();
        foreach (var row in fetched)
        {
            returnedDates.Add(row.ObservationDate);
            if (row.Value is null)
            {
                await RecordSingleMissAsync(conn, seriesId, row.ObservationDate,
                    "fred-null-value", ct);
                markers++;
            }
            else
            {
                await UpsertObservationAsync(conn, seriesId, row.ObservationDate,
                    row.Value.Value, ct);
                upserts++;
            }
        }

        // Any expected publication boundary FRED did not return at all also
        // gets a marker. This bounds re-fetching: a permanently-stale series
        // writes a full marker set on the first run, hits the fast path on
        // every subsequent one.
        var unreturnedMarkers = await BackfillMissingBoundaryMarkersAsync(
            conn, seriesId, fromDate, toDate, cadence, returnedDates, ct);

        _logger.LogInformation(
            "Macro on-demand fill: {Series} {From}..{To} → {Upserts} values upserted, {Markers} missing-value markers, {Boundary} boundary markers",
            seriesId, fromDate, toDate, upserts, markers, unreturnedMarkers);
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

    private static async Task RecordSingleMissAsync(
        NpgsqlConnection conn, string seriesId, DateOnly date, string reason,
        CancellationToken ct)
    {
        await conn.ExecuteAsync(
            """
            INSERT INTO macro_data_misses (series_id, observation_date, reason, fetched_at)
            VALUES (@SeriesId, @Date::date, @Reason, NOW())
            ON CONFLICT (series_id, observation_date) DO NOTHING
            """,
            new { SeriesId = seriesId, Date = date.ToString("yyyy-MM-dd"), Reason = reason });
    }

    /// <summary>
    /// Empty FRED response → mark every expected publication boundary in
    /// the requested window. For Daily: every Mon-Fri. For Monthly: the
    /// first day of every month boundary in [from, to]. Subsequent runs
    /// see the marker count match expected and skip the fetch.
    /// </summary>
    private async Task RecordRangeMissAsync(
        NpgsqlConnection conn, string seriesId, DateOnly from, DateOnly to,
        FredSeriesCadence cadence, string reason, CancellationToken ct)
    {
        var rows = 0;
        foreach (var date in EnumerateBoundaries(from, to, cadence))
        {
            await RecordSingleMissAsync(conn, seriesId, date, reason, ct);
            rows++;
        }
        _logger.LogInformation(
            "Recorded {Count} miss-markers for {Series} {From}..{To} ({Reason})",
            rows, seriesId, from, to, reason);
    }

    /// <summary>
    /// After an on-demand fetch, mark any expected boundary FRED did not
    /// return at all. Distinguishes "FRED has no data on this date" (the
    /// "." sentinel, recorded above as a single-miss) from "FRED skipped
    /// this date in its response", which is the same symptom from the
    /// cache's perspective: the boundary is unfetchable.
    /// </summary>
    private static async Task<int> BackfillMissingBoundaryMarkersAsync(
        NpgsqlConnection conn, string seriesId, DateOnly from, DateOnly to,
        FredSeriesCadence cadence, HashSet<DateOnly> returned, CancellationToken ct)
    {
        var count = 0;
        foreach (var date in EnumerateBoundaries(from, to, cadence))
        {
            if (returned.Contains(date)) continue;
            await RecordSingleMissAsync(conn, seriesId, date, "fred-not-returned", ct);
            count++;
        }
        return count;
    }

    // ── Cadence math (pure, internal for tests) ─────────────────────────

    /// <summary>Count Mon-Fri days in [start, end] inclusive.</summary>
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
}
