using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;
using MomentumBreakoutDetector.HistoryService.Observability;
using MomentumBreakoutDetector.HistoryService.Providers;
using MomentumBreakoutDetector.HistoryService.Validation;
using DomainBarTimeframe = MomentumBreakoutDetector.HistoryService.Domain.BarTimeframe;
using V1Bar = MomentumBreakoutDetector.HistoryService.Contracts.V1.Bar;

namespace MomentumBreakoutDetector.HistoryService;

/// <summary>
/// Phase 1 in-progress. RPCs that have been lifted execute against
/// real providers; everything else still throws
/// <see cref="StatusCode.Unimplemented"/> so consumers see a clean,
/// well-typed "not yet" rather than a 500.
/// </summary>
/// <remarks>
/// Lifted in this revision:
///   <list type="bullet">
///     <item>GetBars  → micro-PR #2 (PolygonBarFetcher + bars provider)</item>
///     <item>GetNbbo  → micro-PR #3 (NBBO + miss-marker provider)</item>
///     <item>GetMacro → micro-PR #5 (FredFetcher + macro provider)</item>
///   </list>
/// Still stubbed:
///   <list type="bullet">
///     <item>GetOptionChain     → micro-PR #4</item>
///     <item>GetCacheStats      → micro-PR #6</item>
///     <item>EnsureRangeCached  → micro-PR #7</item>
///   </list>
/// micro-PR #8 builds out the Testcontainers integration test suite.
/// </remarks>
public sealed class HistoryServiceImpl : Contracts.V1.HistoryService.HistoryServiceBase
{
    private readonly ILogger<HistoryServiceImpl> _logger;
    private readonly IHistoricalBarsProvider _barsProvider;
    private readonly IOptionQuotesProvider _quotes;
    private readonly IMacroDataProvider? _macroProvider;
    // Optional provider injection — each lift PR (#2-#5) sets its own
    // provider. Nullable so that earlier-not-yet-merged PRs don't break
    // the DI graph; an absent provider falls through to Unimplemented.
    private readonly IOptionChainProvider? _optionChainProvider;
    // Phase 1 micro-PR #8 — observability surface. Nullable for tests
    // that bypass DI; in production the singleton is always registered.
    private readonly MetricsCollector? _metrics;

    public HistoryServiceImpl(
        ILogger<HistoryServiceImpl> logger,
        IHistoricalBarsProvider barsProvider,
        IOptionQuotesProvider quotes,
        IMacroDataProvider? macroProvider = null,
        IOptionChainProvider? optionChainProvider = null,
        MetricsCollector? metrics = null)
    {
        _logger = logger;
        _barsProvider = barsProvider;
        _quotes = quotes;
        _macroProvider = macroProvider;
        _optionChainProvider = optionChainProvider;
        _metrics = metrics;
    }

    public override async Task<GetBarsResponse> GetBars(GetBarsRequest request, ServerCallContext context)
    {
        // Validate request shape — empty symbol or unspecified timeframe
        // is a client error; surface it as InvalidArgument rather than
        // silently returning zero bars.
        if (string.IsNullOrWhiteSpace(request.Symbol))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "symbol is required"));
        if (request.FromTs is null || request.ToTs is null)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "from_ts and to_ts are required"));
        if (request.Timeframe == BarTimeframe.Unspecified)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "timeframe is required"));

        var fromUtc = request.FromTs.ToDateTime();
        var toUtc = request.ToTs.ToDateTime();
        var timeframe = MapTimeframe(request.Timeframe);

        // Past-day-only guard. Reject requests whose range includes
        // today or later — today's data lives in MBD-local, not here.
        PastOnlyRangeValidator.EnsurePastOnly(fromUtc, toUtc);

        _logger.LogInformation(
            "GetBars symbol={Symbol} timeframe={Timeframe} from={From:O} to={To:O}",
            request.Symbol, timeframe, fromUtc, toUtc);

        var result = await _barsProvider.GetBarsAsync(
            request.Symbol, fromUtc, toUtc, timeframe, context.CancellationToken);

        var response = new GetBarsResponse { CacheHit = result.CacheHit };
        foreach (var bar in result.Bars)
        {
            response.Bars.Add(new V1Bar
            {
                Symbol = bar.Symbol,
                Timestamp = Timestamp.FromDateTime(DateTime.SpecifyKind(bar.Timestamp, DateTimeKind.Utc)),
                Open = (double)bar.Open,
                High = (double)bar.High,
                Low = (double)bar.Low,
                Close = (double)bar.Close,
                Volume = (double)bar.Volume,
                Vwap = (double)bar.VWAP,
                // trade_count not currently surfaced through the cache —
                // historical_bars.trade_count is nullable and unset by
                // the on-demand fetch path. Defaulted to 0 here; PR #8
                // can wire it through once the schema is populated.
                TradeCount = 0,
            });
        }
        return response;
    }

    /// <summary>
    /// Map the proto BarTimeframe to the internal Domain enum.
    /// 15-minute and 1-hour aren't present in the wire enum (the proto
    /// only declares minute / 5-minute / day) — the warmup orchestrator
    /// can extend the proto in a later PR if intermediate timeframes
    /// become callable.
    /// </summary>
    private static DomainBarTimeframe MapTimeframe(BarTimeframe inWire) => inWire switch
    {
        BarTimeframe.Minute => DomainBarTimeframe.OneMinute,
        BarTimeframe.FiveMinute => DomainBarTimeframe.FiveMinutes,
        BarTimeframe.Day => DomainBarTimeframe.OneDay,
        _ => DomainBarTimeframe.OneMinute,
    };

    public override async Task<GetNbboResponse> GetNbbo(GetNbboRequest request, ServerCallContext context)
    {
        // Micro-PR #3 — NBBO quotes lift. Cache-first lookup with
        // write-through; miss-markers prevent re-polling Polygon for
        // known-empty (ticker, ts) pairs.
        if (string.IsNullOrWhiteSpace(request.Ticker))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "ticker is required"));
        }
        if (request.Ts is null)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "ts is required"));
        }

        var tmpTsUtc = request.Ts.ToDateTime();

        // Past-day-only guard. Reject point timestamps at or beyond the
        // today-in-ET boundary; today's NBBO lives in MBD-local.
        PastOnlyRangeValidator.EnsurePastOnly(tmpTsUtc);

        _logger.LogInformation("GetNbbo ticker={Ticker} ts={Ts:O}", request.Ticker, tmpTsUtc);

        var tmpLookup = await _quotes
            .GetAtOrBeforeAsync(request.Ticker, tmpTsUtc, context.CancellationToken)
            .ConfigureAwait(false);

        var tmpResp = new GetNbboResponse
        {
            CacheHit = tmpLookup.CacheHit,
            IsMissMarker = tmpLookup.IsMissMarker,
        };
        if (tmpLookup.Quote is { } q)
        {
            tmpResp.Quote = new NbboQuote
            {
                Ticker = q.Ticker,
                RequestedTs = Timestamp.FromDateTime(DateTime.SpecifyKind(q.RequestedTsUtc, DateTimeKind.Utc)),
                AsOfTs = Timestamp.FromDateTime(DateTime.SpecifyKind(q.AsOfTsUtc, DateTimeKind.Utc)),
                BidPrice = (double)q.BidPrice,
                AskPrice = (double)q.AskPrice,
                BidSize = q.BidSize ?? 0,
                AskSize = q.AskSize ?? 0,
                BidExchange = q.BidExchange ?? 0,
                AskExchange = q.AskExchange ?? 0,
            };
        }
        return tmpResp;
    }

    public override async Task<GetOptionChainResponse> GetOptionChain(GetOptionChainRequest request, ServerCallContext context)
    {
        if (_optionChainProvider is null)
        {
            // DI not wired — typical only in tests that intentionally omit the provider.
            throw new RpcException(new Status(StatusCode.Unimplemented, "Option-chain provider is not registered."));
        }

        if (string.IsNullOrWhiteSpace(request.UnderlyingTicker))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "underlying_ticker is required."));
        }
        if (request.AsOfDate is null)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "as_of_date is required."));
        }

        _logger.LogInformation(
            "GetOptionChain underlying={Underlying} as_of={AsOf}",
            request.UnderlyingTicker, request.AsOfDate.ToDateTime());

        // proto Timestamp → DateOnly. Time component intentionally ignored;
        // the contract's comment on as_of_date says so.
        var tmpAsOfUtc = request.AsOfDate.ToDateTime();
        var tmpAsOfDate = DateOnly.FromDateTime(tmpAsOfUtc);

        // Past-day-only guard. as_of_date is a single point; reject if
        // it lands on or after the today-in-ET boundary.
        PastOnlyRangeValidator.EnsurePastOnly(tmpAsOfUtc);

        var tmpResult = await _optionChainProvider.GetChainAsync(
            request.UnderlyingTicker, tmpAsOfDate, context.CancellationToken);

        var tmpResponse = new GetOptionChainResponse
        {
            CacheHit = tmpResult.CacheHit,
            IsMissMarker = tmpResult.IsMissMarker,
        };

        // Apply optional filters at the gRPC layer rather than baking them
        // into the SQL query. The chain is small (~500 rows on TSLA) so an
        // in-memory filter is cheap, and keeps the provider's stable
        // ORDER BY clause untouched. See proto: contract_type / strike_min /
        // strike_max / expiration_after / expiration_before are "0 or empty
        // = no filter".
        var tmpFiltered = ApplyFilters(tmpResult.Contracts, request);
        foreach (var tmpRow in tmpFiltered)
        {
            tmpResponse.Contracts.Add(MapToProto(tmpRow));
        }

        return tmpResponse;
    }

    private static IEnumerable<OptionContractRow> ApplyFilters(
        IReadOnlyList<OptionContractRow> inRows, GetOptionChainRequest inRequest)
    {
        IEnumerable<OptionContractRow> tmp = inRows;

        if (inRequest.ContractType != ContractType.Unspecified)
        {
            var tmpWanted = inRequest.ContractType == ContractType.Call ? "call" : "put";
            tmp = tmp.Where(r => string.Equals(r.ContractType, tmpWanted, StringComparison.OrdinalIgnoreCase));
        }
        if (inRequest.ExpirationAfter is not null)
        {
            var tmpAfter = DateOnly.FromDateTime(inRequest.ExpirationAfter.ToDateTime());
            tmp = tmp.Where(r => r.ExpirationDate > tmpAfter);
        }
        if (inRequest.ExpirationBefore is not null)
        {
            var tmpBefore = DateOnly.FromDateTime(inRequest.ExpirationBefore.ToDateTime());
            tmp = tmp.Where(r => r.ExpirationDate < tmpBefore);
        }
        if (inRequest.StrikeMin > 0)
        {
            tmp = tmp.Where(r => r.StrikePrice.HasValue && (double)r.StrikePrice.Value >= inRequest.StrikeMin);
        }
        if (inRequest.StrikeMax > 0)
        {
            tmp = tmp.Where(r => r.StrikePrice.HasValue && (double)r.StrikePrice.Value <= inRequest.StrikeMax);
        }
        return tmp;
    }

    private static OptionContract MapToProto(OptionContractRow inRow)
    {
        var tmpContract = new OptionContract
        {
            Ticker = inRow.Ticker,
            UnderlyingTicker = inRow.UnderlyingTicker,
            ContractType = inRow.ContractType?.ToLowerInvariant() switch
            {
                "call" => ContractType.Call,
                "put"  => ContractType.Put,
                _      => ContractType.Unspecified,
            },
            ExerciseStyle = inRow.ExerciseStyle ?? string.Empty,
            ExpirationDate = Timestamp.FromDateTime(
                inRow.ExpirationDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc)),
            StrikePrice = inRow.StrikePrice.HasValue ? (double)inRow.StrikePrice.Value : 0d,
            SharesPerContract = inRow.SharesPerContract ?? 0,
            PrimaryExchange = inRow.PrimaryExchange ?? string.Empty,
        };
        return tmpContract;
    }

    public override async Task<GetMacroResponse> GetMacro(GetMacroRequest request, ServerCallContext context)
    {
        _logger.LogInformation(
            "GetMacro called series={Series} from={From} to={To}",
            request.SeriesId, request.FromDate, request.ToDate);

        if (_macroProvider is null)
        {
            throw new RpcException(new Status(
                StatusCode.FailedPrecondition,
                "MacroDataProvider not registered — check History:ConnectionString configuration."));
        }

        if (string.IsNullOrWhiteSpace(request.SeriesId))
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument, "series_id is required."));
        }
        if (request.FromDate is null || request.ToDate is null)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument, "from_date and to_date are required."));
        }

        var fromDate = DateOnly.FromDateTime(request.FromDate.ToDateTime().ToUniversalTime().Date);
        var toDate = DateOnly.FromDateTime(request.ToDate.ToDateTime().ToUniversalTime().Date);

        if (fromDate > toDate)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument, "from_date must be <= to_date."));
        }

        // Past-day-only guard. Reject ranges that touch today or
        // beyond. We pass the original UTC instants so the boundary
        // comparison runs at full timestamp resolution (not rounded to
        // a calendar date).
        PastOnlyRangeValidator.EnsurePastOnly(
            request.FromDate.ToDateTime(),
            request.ToDate.ToDateTime());

        // Quick cache-hit probe: if the cache already covers the requested
        // window, EnsureRangeCached short-circuits with no FRED calls and
        // we report cache_hit=true. Otherwise we run the warmup, then read.
        var cachedBefore = await _macroProvider.GetSeriesAsync(
            request.SeriesId, fromDate, toDate, context.CancellationToken);

        await _macroProvider.EnsureRangeCachedAsync(
            request.SeriesId, fromDate, toDate, context.CancellationToken);

        var rows = await _macroProvider.GetSeriesAsync(
            request.SeriesId, fromDate, toDate, context.CancellationToken);

        var resp = new GetMacroResponse
        {
            CacheHit = rows.Count == cachedBefore.Count
                       && cachedBefore.Count > 0,
        };

        foreach (var row in rows)
        {
            // Skip null-value rows (FRED "." sentinel) — they live as
            // miss-markers, not in the response payload. Consumers expect
            // observations to carry a real value.
            if (row.Value is null) continue;
            resp.Observations.Add(new MacroObservation
            {
                SeriesId = row.SeriesId,
                ObservationDate = Timestamp.FromDateTime(
                    DateTime.SpecifyKind(row.ObservationDate.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc)),
                Value = (double)row.Value.Value,
            });
        }

        return resp;
    }

    public override async Task EnsureRangeCached(
        EnsureRangeCachedRequest request,
        IServerStreamWriter<EnsureRangeCachedProgress> responseStream,
        ServerCallContext context)
    {
        // ── Validate ────────────────────────────────────────────────────
        if (request.Symbols.Count == 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "at least one symbol is required"));
        }
        if (request.DataClasses.Count == 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "at least one data_class is required"));
        }
        if (request.FromTs is null || request.ToTs is null)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "from_ts and to_ts are required"));
        }
        var fromUtc = request.FromTs.ToDateTime();
        var toUtc = request.ToTs.ToDateTime();
        if (fromUtc > toUtc)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "from_ts must be <= to_ts"));
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        _logger.LogInformation(
            "EnsureRangeCached symbols=[{Symbols}] classes=[{Classes}] from={From:O} to={To:O}",
            string.Join(",", request.Symbols),
            string.Join(",", request.DataClasses),
            fromUtc, toUtc);

        // Stream lock: gRPC server-streaming write is not concurrency-safe.
        // We fan out kind handlers in parallel so total wall time is the
        // slowest kind, not the sum — but writes funnel through this lock.
        var writeLock = new SemaphoreSlim(1, 1);

        async Task EmitAsync(EnsureRangeCachedProgress inProgress)
        {
            inProgress.ElapsedMs = stopwatch.ElapsedMilliseconds;
            await writeLock.WaitAsync(context.CancellationToken).ConfigureAwait(false);
            try
            {
                await responseStream.WriteAsync(inProgress).ConfigureAwait(false);
            }
            finally { writeLock.Release(); }
        }

        // ── Per-class tasks ─────────────────────────────────────────────
        var tasks = new List<Task>();
        foreach (var dataClass in request.DataClasses.Distinct())
        {
            switch (dataClass)
            {
                case DataClass.Bars:
                    tasks.Add(WarmBarsAsync(request, fromUtc, toUtc, EmitAsync, context.CancellationToken));
                    break;
                case DataClass.Chains:
                    tasks.Add(WarmChainsAsync(request, fromUtc, toUtc, EmitAsync, context.CancellationToken));
                    break;
                case DataClass.Macro:
                    tasks.Add(WarmMacroAsync(request, fromUtc, toUtc, EmitAsync, context.CancellationToken));
                    break;
                case DataClass.Nbbo:
                    // NBBO is point-fetch by nature (one quote per
                    // (ticker, ts) sub-second); warmup over a [from, to]
                    // window doesn't have a sensible enumeration. Emit a
                    // terminal FAILED event with an explanatory message
                    // instead of throwing — other kinds keep running.
                    tasks.Add(EmitAsync(new EnsureRangeCachedProgress
                    {
                        Status = WarmupStatus.Failed,
                        DataClass = DataClass.Nbbo,
                        Message = "NBBO warmup is not implemented (deferred to micro-PR #8). NBBO quotes warm naturally via point fetch.",
                    }));
                    break;
                default:
                    tasks.Add(EmitAsync(new EnsureRangeCachedProgress
                    {
                        Status = WarmupStatus.Failed,
                        DataClass = dataClass,
                        Message = $"Unsupported data_class: {dataClass}",
                    }));
                    break;
            }
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
        _logger.LogInformation(
            "EnsureRangeCached completed in {Elapsed}ms",
            stopwatch.ElapsedMilliseconds);
    }

    private async Task WarmBarsAsync(
        EnsureRangeCachedRequest inRequest, DateTime inFromUtc, DateTime inToUtc,
        Func<EnsureRangeCachedProgress, Task> inEmit, CancellationToken inCt)
    {
        try
        {
            var timeframe = inRequest.BarTimeframe == BarTimeframe.Unspecified
                ? DomainBarTimeframe.OneMinute
                : MapTimeframe(inRequest.BarTimeframe);

            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Planning,
                DataClass = DataClass.Bars,
                Message = $"Planning bars warmup for {inRequest.Symbols.Count} symbol(s), timeframe={timeframe}",
            }).ConfigureAwait(false);

            int totalUpstream = 0;
            var symbols = inRequest.Symbols.ToList();
            foreach (var symbol in symbols)
            {
                Func<BarsWarmupProgress, CancellationToken, Task> progress =
                    async (p, ct) => await inEmit(new EnsureRangeCachedProgress
                    {
                        Status = WarmupStatus.Fetching,
                        DataClass = DataClass.Bars,
                        KeysComplete = p.ChunkIndex,
                        KeysTotal = p.ChunksTotal,
                        UpstreamCalls = p.ChunkIndex,
                        Message = $"{p.Symbol} {p.Timeframe} chunk {p.ChunkIndex}/{p.ChunksTotal} → {p.BarsFetched} bars{(p.IsMissChunk ? " (miss-marker)" : "")}",
                    }).ConfigureAwait(false);

                var tmpUpstream = await _barsProvider.EnsureRangeCachedAsync(
                    symbol, inFromUtc, inToUtc, timeframe, progress, inCt).ConfigureAwait(false);
                totalUpstream += tmpUpstream;
            }

            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Completed,
                DataClass = DataClass.Bars,
                UpstreamCalls = totalUpstream,
                KeysComplete = totalUpstream,
                KeysTotal = totalUpstream,
                Message = $"Bars warmup complete: {symbols.Count} symbol(s), {totalUpstream} upstream call(s)",
            }).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Bars warmup failed");
            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Failed,
                DataClass = DataClass.Bars,
                Message = $"Bars warmup failed: {ex.Message}",
            }).ConfigureAwait(false);
        }
    }

    private async Task WarmChainsAsync(
        EnsureRangeCachedRequest inRequest, DateTime inFromUtc, DateTime inToUtc,
        Func<EnsureRangeCachedProgress, Task> inEmit, CancellationToken inCt)
    {
        try
        {
            if (_optionChainProvider is null)
            {
                await inEmit(new EnsureRangeCachedProgress
                {
                    Status = WarmupStatus.Failed,
                    DataClass = DataClass.Chains,
                    Message = "Option-chain provider not registered.",
                }).ConfigureAwait(false);
                return;
            }

            // Enumerate calendar days [from, to]. Weekends are filtered —
            // OptionChainProvider's miss-marker logic still handles
            // holidays cleanly, but skipping Sat/Sun avoids guaranteed
            // miss-marker writes. Day enumeration is in UTC.
            var fromDate = DateOnly.FromDateTime(inFromUtc);
            var toDate = DateOnly.FromDateTime(inToUtc);
            var days = new List<DateOnly>();
            for (var d = fromDate; d <= toDate; d = d.AddDays(1))
            {
                if (d.DayOfWeek == DayOfWeek.Saturday || d.DayOfWeek == DayOfWeek.Sunday) continue;
                days.Add(d);
            }

            var totalKeys = inRequest.Symbols.Count * days.Count;

            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Planning,
                DataClass = DataClass.Chains,
                KeysTotal = totalKeys,
                Message = $"Planning chains warmup: {inRequest.Symbols.Count} symbol(s) × {days.Count} weekday(s) = {totalKeys} key(s)",
            }).ConfigureAwait(false);

            var completed = 0;
            foreach (var symbol in inRequest.Symbols)
            {
                foreach (var day in days)
                {
                    await _optionChainProvider.EnsureChainCachedAsync(symbol, day, inCt).ConfigureAwait(false);
                    completed++;
                    if (completed % 5 == 0 || completed == totalKeys)
                    {
                        await inEmit(new EnsureRangeCachedProgress
                        {
                            Status = WarmupStatus.Fetching,
                            DataClass = DataClass.Chains,
                            KeysComplete = completed,
                            KeysTotal = totalKeys,
                            Message = $"Chains: {symbol} {day} ({completed}/{totalKeys})",
                        }).ConfigureAwait(false);
                    }
                }
            }

            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Completed,
                DataClass = DataClass.Chains,
                KeysComplete = totalKeys,
                KeysTotal = totalKeys,
                Message = $"Chains warmup complete: {totalKeys} (symbol, as-of-date) key(s)",
            }).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Chains warmup failed");
            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Failed,
                DataClass = DataClass.Chains,
                Message = $"Chains warmup failed: {ex.Message}",
            }).ConfigureAwait(false);
        }
    }

    private async Task WarmMacroAsync(
        EnsureRangeCachedRequest inRequest, DateTime inFromUtc, DateTime inToUtc,
        Func<EnsureRangeCachedProgress, Task> inEmit, CancellationToken inCt)
    {
        try
        {
            if (_macroProvider is null)
            {
                await inEmit(new EnsureRangeCachedProgress
                {
                    Status = WarmupStatus.Failed,
                    DataClass = DataClass.Macro,
                    Message = "Macro provider not registered.",
                }).ConfigureAwait(false);
                return;
            }

            // For macro, the request's `symbols` field carries FRED
            // series ids (T10Y2Y, CPIAUCSL, UNRATE, ...). The warmup
            // entry in MacroDataProvider already iterates per-series.
            var fromDate = DateOnly.FromDateTime(inFromUtc);
            var toDate = DateOnly.FromDateTime(inToUtc);

            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Planning,
                DataClass = DataClass.Macro,
                KeysTotal = inRequest.Symbols.Count,
                Message = $"Planning macro warmup: {inRequest.Symbols.Count} series",
            }).ConfigureAwait(false);

            var done = 0;
            foreach (var seriesId in inRequest.Symbols)
            {
                await _macroProvider.EnsureRangeCachedAsync(seriesId, fromDate, toDate, inCt).ConfigureAwait(false);
                done++;
                await inEmit(new EnsureRangeCachedProgress
                {
                    Status = WarmupStatus.Fetching,
                    DataClass = DataClass.Macro,
                    KeysComplete = done,
                    KeysTotal = inRequest.Symbols.Count,
                    Message = $"Macro: {seriesId} ({done}/{inRequest.Symbols.Count})",
                }).ConfigureAwait(false);
            }

            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Completed,
                DataClass = DataClass.Macro,
                KeysComplete = inRequest.Symbols.Count,
                KeysTotal = inRequest.Symbols.Count,
                Message = $"Macro warmup complete: {inRequest.Symbols.Count} series",
            }).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Macro warmup failed");
            await inEmit(new EnsureRangeCachedProgress
            {
                Status = WarmupStatus.Failed,
                DataClass = DataClass.Macro,
                Message = $"Macro warmup failed: {ex.Message}",
            }).ConfigureAwait(false);
        }
    }

    public override Task<GetCacheStatsResponse> GetCacheStats(GetCacheStatsRequest request, ServerCallContext context)
    {
        _logger.LogInformation("GetCacheStats called class={Class}", request.DataClass);

        if (_metrics is null)
        {
            // No collector wired (tests that omit DI). Return empty
            // response rather than 5xx so probe endpoints stay healthy.
            return Task.FromResult(new GetCacheStatsResponse
            {
                AsOf = Timestamp.FromDateTime(DateTime.UtcNow),
            });
        }

        var snapshot = _metrics.Snapshot();
        var resp = new GetCacheStatsResponse
        {
            AsOf = Timestamp.FromDateTime(DateTime.SpecifyKind(snapshot.AsOfUtc, DateTimeKind.Utc)),
        };

        // Map the internal MetricKind → proto DataClass enum. We emit
        // one ClassStats row per kind regardless of the request filter
        // for simple consumers; a `data_class` filter on the request
        // narrows to a single row.
        foreach (var c in snapshot.Classes)
        {
            var protoClass = MapKind(c.Kind);
            if (request.DataClass != DataClass.Unspecified && request.DataClass != protoClass)
            {
                continue;
            }
            resp.ClassStats.Add(new ClassStats
            {
                DataClass = protoClass,
                TotalRequests = c.TotalRequests,
                CacheHits = c.CacheHits,
                UpstreamFetches = c.UpstreamFetches,
                MissMarkers = c.MissMarkers,
                InFlightCount = c.InFlightCount,
                LatencyP50Ms = c.LatencyP50Ms,
                LatencyP95Ms = c.LatencyP95Ms,
                LatencyP99Ms = c.LatencyP99Ms,
            });
        }
        return Task.FromResult(resp);
    }

    /// <summary>
    /// Bridge between the internal counter taxonomy
    /// (<see cref="MetricKind"/>) and the proto-emitted enum
    /// (<see cref="DataClass"/>).
    /// </summary>
    private static DataClass MapKind(MetricKind inKind) => inKind switch
    {
        MetricKind.Bars => DataClass.Bars,
        MetricKind.Nbbo => DataClass.Nbbo,
        MetricKind.Chains => DataClass.Chains,
        MetricKind.Macro => DataClass.Macro,
        _ => DataClass.Unspecified,
    };
}
