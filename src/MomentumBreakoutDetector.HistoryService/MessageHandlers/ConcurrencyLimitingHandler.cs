using Microsoft.Extensions.Options;

namespace MomentumBreakoutDetector.HistoryService.MessageHandlers;

/// <summary>
/// Process-wide concurrency cap on in-flight Polygon HTTP calls. Replaces
/// the per-fetcher SemaphoreSlim that lived inside the raw-HttpClient
/// fetchers (PolygonBarFetcher, PolygonNbboFetcher, PolygonChainFetcher)
/// before the Phase E SDK refactor.
///
/// Implementation note — the SDK's <c>AddPolygonClient</c> registers the
/// handler as <c>AddTransient</c> via <c>AddHttpMessageHandler&lt;T&gt;</c>,
/// so a fresh instance lands on every Refit-bound HttpClient. To preserve
/// the "process-wide" guarantee that the original SemaphoreSlim provided,
/// the gate itself is a static field — ALL handler instances share the
/// same semaphore. The static is keyed off
/// <see cref="HistoryServiceOptions.PolygonMaxConcurrentFetches"/> at
/// first-resolve and won't pick up runtime changes; that matches the
/// original behaviour where the ctor wired the semaphore once.
/// </summary>
public sealed class ConcurrencyLimitingHandler : DelegatingHandler
{
    /// <summary>Default process-wide cap. 8 — same as the bar / NBBO / chain
    /// fetchers used in their original raw-HttpClient form.</summary>
    public const int DefaultMaxConcurrent = 8;

    private static SemaphoreSlim? s_Gate;
    private static readonly object s_GateLock = new();

    public ConcurrencyLimitingHandler(IOptions<HistoryServiceOptions> inOpts)
    {
        var tmpMax = inOpts.Value.PolygonMaxConcurrentFetches;
        EnsureGate(tmpMax > 0 ? tmpMax : DefaultMaxConcurrent);
    }

    /// <summary>Test-friendly ctor that lets a unit test stand up a handler
    /// with an explicit semaphore size, isolated from the static cap.</summary>
    public ConcurrencyLimitingHandler(int inMaxConcurrent)
    {
        EnsureGate(inMaxConcurrent > 0 ? inMaxConcurrent : DefaultMaxConcurrent);
    }

    private static void EnsureGate(int inMax)
    {
        if (s_Gate is not null) return;
        lock (s_GateLock)
        {
            s_Gate ??= new SemaphoreSlim(inMax, inMax);
        }
    }

    /// <summary>Reset hook — exposed for tests so a fresh max value takes
    /// effect. Not used in production code paths.</summary>
    internal static void ResetGateForTests(int inMax)
    {
        lock (s_GateLock)
        {
            s_Gate?.Dispose();
            s_Gate = new SemaphoreSlim(inMax, inMax);
        }
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var tmpGate = s_Gate
            ?? throw new InvalidOperationException(
                "ConcurrencyLimitingHandler used before initialization");
        await tmpGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            tmpGate.Release();
        }
    }
}
