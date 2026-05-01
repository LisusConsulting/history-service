using Grpc.Core;
using Microsoft.Extensions.Logging;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;

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

    public HistoryServiceImpl(ILogger<HistoryServiceImpl> logger)
    {
        _logger = logger;
    }

    public override Task<GetBarsResponse> GetBars(GetBarsRequest request, ServerCallContext context)
    {
        _logger.LogInformation("GetBars called (stub) symbol={Symbol}", request.Symbol);
        throw new RpcException(new Status(StatusCode.Unimplemented, "TODO: micro-PR #2 — lift PolygonBarFetcher + bars provider."));
    }

    public override Task<GetNbboResponse> GetNbbo(GetNbboRequest request, ServerCallContext context)
    {
        _logger.LogInformation("GetNbbo called (stub) ticker={Ticker}", request.Ticker);
        throw new RpcException(new Status(StatusCode.Unimplemented, "TODO: micro-PR #5 — lift NBBO fetcher + miss-marker provider."));
    }

    public override Task<GetOptionChainResponse> GetOptionChain(GetOptionChainRequest request, ServerCallContext context)
    {
        _logger.LogInformation("GetOptionChain called (stub) underlying={Underlying}", request.UnderlyingTicker);
        throw new RpcException(new Status(StatusCode.Unimplemented, "TODO: micro-PR #4 — lift PolygonChainFetcher + chain provider."));
    }

    public override Task<GetMacroResponse> GetMacro(GetMacroRequest request, ServerCallContext context)
    {
        _logger.LogInformation("GetMacro called (stub) series={Series}", request.SeriesId);
        throw new RpcException(new Status(StatusCode.Unimplemented, "TODO: micro-PR #3 — lift FredFetcher + macro provider."));
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
