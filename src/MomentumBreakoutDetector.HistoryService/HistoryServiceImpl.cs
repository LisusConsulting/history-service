using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;
using MomentumBreakoutDetector.HistoryService.Providers;

namespace MomentumBreakoutDetector.HistoryService;

/// <summary>
/// Phase 1, micro-PR #1 stub. Every RPC throws
/// <see cref="StatusCode.Unimplemented"/> so consumers see a clean,
/// well-typed "not yet" rather than a 500.
/// </summary>
/// <remarks>
/// Each method's TODO comment names the micro-PR that will lift the
/// real implementation:
///   <list type="bullet">
///     <item>GetBars            → micro-PR #2 (PolygonBarFetcher + provider)</item>
///     <item>GetMacro           → micro-PR #3 (FredFetcher + provider)</item>
///     <item>GetOptionChain     → micro-PR #4 (PolygonChainFetcher + provider)</item>
///     <item>GetNbbo            → micro-PR #5 (NBBO + miss-marker provider)</item>
///     <item>GetCacheStats      → micro-PR #6 (SingleFlight coalescer + stats)</item>
///     <item>EnsureRangeCached  → micro-PR #7 (warmup orchestrator)</item>
///   </list>
/// micro-PR #8 builds out the Testcontainers integration test suite.
/// </remarks>
public sealed class HistoryServiceImpl : Contracts.V1.HistoryService.HistoryServiceBase
{
    private readonly ILogger<HistoryServiceImpl> _logger;
    private readonly IOptionQuotesProvider _quotes;
    private readonly IMacroDataProvider? _macroProvider;

    public HistoryServiceImpl(
        ILogger<HistoryServiceImpl> logger,
        IOptionQuotesProvider quotes,
        IMacroDataProvider? macroProvider = null)
    {
        _logger = logger;
        _quotes = quotes;
        _macroProvider = macroProvider;
    }

    public override Task<GetBarsResponse> GetBars(GetBarsRequest request, ServerCallContext context)
    {
        _logger.LogInformation("GetBars called (stub) symbol={Symbol}", request.Symbol);
        throw new RpcException(new Status(StatusCode.Unimplemented, "TODO: micro-PR #2 — lift PolygonBarFetcher + bars provider."));
    }

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

    public override Task<GetOptionChainResponse> GetOptionChain(GetOptionChainRequest request, ServerCallContext context)
    {
        _logger.LogInformation("GetOptionChain called (stub) underlying={Underlying}", request.UnderlyingTicker);
        throw new RpcException(new Status(StatusCode.Unimplemented, "TODO: micro-PR #4 — lift PolygonChainFetcher + chain provider."));
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

    public override Task EnsureRangeCached(
        EnsureRangeCachedRequest request,
        IServerStreamWriter<EnsureRangeCachedProgress> responseStream,
        ServerCallContext context)
    {
        _logger.LogInformation("EnsureRangeCached called (stub) symbols={Count}", request.Symbols.Count);
        throw new RpcException(new Status(StatusCode.Unimplemented, "TODO: micro-PR #7 — warmup orchestrator (depends on PRs #2-#6)."));
    }

    public override Task<GetCacheStatsResponse> GetCacheStats(GetCacheStatsRequest request, ServerCallContext context)
    {
        _logger.LogInformation("GetCacheStats called (stub) class={Class}", request.DataClass);
        throw new RpcException(new Status(StatusCode.Unimplemented, "TODO: micro-PR #6 — SingleFlight coalescer + stats accumulator."));
    }
}
