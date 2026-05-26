using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using MomentumBreakoutDetector.HistoryService.Contracts.V1;
using MomentumBreakoutDetector.HistoryService.Fetchers;
using MomentumBreakoutDetector.HistoryService.Providers;
using MomentumBreakoutDetector.HistoryService.Validation;
using Shouldly;
using Xunit;
using DomainBar = MomentumBreakoutDetector.HistoryService.Domain.Bar;
using DomainBarTimeframe = MomentumBreakoutDetector.HistoryService.Domain.BarTimeframe;
using ProviderDailyOptionsFlowRow = MomentumBreakoutDetector.HistoryService.Providers.DailyOptionsFlowRow;
using ProviderDailyAtmIvRow = MomentumBreakoutDetector.HistoryService.Providers.DailyAtmIvRow;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Past-day-only invariant. history-service serves data strictly before
/// the today-in-ET boundary; today's data must come from MBD-local. Any
/// request whose range or point timestamp lands on or after the boundary
/// is rejected with FAILED_PRECONDITION before any data fetch.
///
/// These tests run against <see cref="HistoryServiceImpl"/> directly
/// (server-side method invocation) with stub providers that throw if
/// invoked — proving the validator short-circuits before hitting the
/// data layer. Using stubs (not Testcontainers) keeps the suite fast and
/// hermetic.
///
/// Boundary semantics under test:
///   ✓ All-past range:                              succeeds
///   ✗ Range ending exactly at boundary:            rejected
///   ✗ Range entirely in the future:                rejected
///   ✗ Straddling range (from past, to ≥ boundary): rejected (no truncation)
/// </summary>
public sealed class PastOnlyRangeValidationTests
{
    // ─────────────────────────────────────────────────────────────────
    // Boundary helper. Mirrors PastOnlyRangeValidator's computation so
    // tests use the exact same instant the production code rejects on.
    // ─────────────────────────────────────────────────────────────────
    private static DateTime TodayBoundaryUtc()
    {
        var tmpEt = ResolveEasternTz();
        var tmpNowEt = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tmpEt);
        var tmpMidnight = new DateTime(
            tmpNowEt.Year, tmpNowEt.Month, tmpNowEt.Day, 0, 0, 0, DateTimeKind.Unspecified);
        return TimeZoneInfo.ConvertTimeToUtc(tmpMidnight, tmpEt);
    }

    private static TimeZoneInfo ResolveEasternTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
    }

    // ─────────────────────────────────────────────────────────────────
    // GetBars
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetBars_AllPastRange_PassesValidationAndReachesProvider()
    {
        // Range entirely a week ago → must NOT throw FailedPrecondition.
        // We use a stub that returns 0 bars so the call completes cleanly
        // once the validator has cleared.
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(-7);
        var tmpToUtc = tmpBoundary.AddDays(-7).AddHours(1);

        var tmpImpl = NewImplWith(new RecordingBarsProvider());
        var tmpRequest = NewBarsRequest(tmpFromUtc, tmpToUtc);

        var tmpResp = await tmpImpl.GetBars(tmpRequest, NewServerCallContext());
        // Reaching here without an RpcException is the success criterion;
        // an empty bar list is fine.
        tmpResp.Bars.Count.ShouldBe(0);
    }

    [Fact]
    public async Task GetBars_RangeEndingAtBoundary_IsRejected()
    {
        // to == boundary means "request includes today's first
        // millisecond" → reject.
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(-1);
        var tmpToUtc = tmpBoundary;

        var tmpImpl = NewImplWith(new ThrowingBarsProvider());
        var tmpRequest = NewBarsRequest(tmpFromUtc, tmpToUtc);

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetBars(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpEx.Status.Detail.ShouldContain("past-day data only");
        tmpEx.Status.Detail.ShouldContain("ET");
    }

    [Fact]
    public async Task GetBars_RangeEntirelyInFuture_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(1);
        var tmpToUtc = tmpBoundary.AddDays(2);

        var tmpImpl = NewImplWith(new ThrowingBarsProvider());
        var tmpRequest = NewBarsRequest(tmpFromUtc, tmpToUtc);

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetBars(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
    }

    [Fact]
    public async Task GetBars_StraddlingRange_IsRejected_NotSilentlyTruncated()
    {
        // from in the past, to in the future. Must NOT truncate; the
        // whole request fails so the caller can surface a router bug.
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(-1);
        var tmpToUtc = tmpBoundary.AddHours(1);

        var tmpProvider = new ThrowingBarsProvider();
        var tmpImpl = NewImplWith(tmpProvider);
        var tmpRequest = NewBarsRequest(tmpFromUtc, tmpToUtc);

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetBars(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        // Provider must NOT have been touched — that would mean we
        // tried to fetch the truncated past portion silently.
        tmpProvider.WasInvoked.ShouldBeFalse();
    }

    // ─────────────────────────────────────────────────────────────────
    // GetNbbo (point timestamp)
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetNbbo_PastTimestamp_PassesValidation()
    {
        // We need a quotes provider that doesn't throw — return a miss.
        var tmpBoundary = TodayBoundaryUtc();
        var tmpTsUtc = tmpBoundary.AddDays(-3);

        var tmpQuotes = new RecordingQuotesProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: tmpQuotes,
            macroProvider: null);

        var tmpRequest = new GetNbboRequest
        {
            Ticker = "O:TSLA241220C00250000",
            Ts = Timestamp.FromDateTime(tmpTsUtc),
        };

        var tmpResp = await tmpImpl.GetNbbo(tmpRequest, NewServerCallContext());
        tmpResp.ShouldNotBeNull();
        tmpQuotes.WasInvoked.ShouldBeTrue();
    }

    [Fact]
    public async Task GetNbbo_TimestampAtBoundary_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpQuotes = new ThrowingQuotesProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: tmpQuotes);

        var tmpRequest = new GetNbboRequest
        {
            Ticker = "O:TSLA241220C00250000",
            Ts = Timestamp.FromDateTime(tmpBoundary),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetNbbo(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
    }

    [Fact]
    public async Task GetNbbo_FutureTimestamp_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpQuotes = new ThrowingQuotesProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: tmpQuotes);

        var tmpRequest = new GetNbboRequest
        {
            Ticker = "O:TSLA241220C00250000",
            Ts = Timestamp.FromDateTime(tmpBoundary.AddHours(8)), // sometime later today/tomorrow
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetNbbo(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpQuotes.WasInvoked.ShouldBeFalse();
    }

    // ─────────────────────────────────────────────────────────────────
    // GetOptionChain (single as-of-date)
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetOptionChain_PastAsOfDate_PassesValidation()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpAsOf = tmpBoundary.AddDays(-5);

        var tmpChain = new RecordingChainProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: tmpChain);

        var tmpRequest = new GetOptionChainRequest
        {
            UnderlyingTicker = "TSLA",
            AsOfDate = Timestamp.FromDateTime(tmpAsOf),
        };

        var tmpResp = await tmpImpl.GetOptionChain(tmpRequest, NewServerCallContext());
        tmpResp.ShouldNotBeNull();
        tmpChain.WasInvoked.ShouldBeTrue();
    }

    [Fact]
    public async Task GetOptionChain_AsOfAtBoundary_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpChain = new ThrowingChainProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: tmpChain);

        var tmpRequest = new GetOptionChainRequest
        {
            UnderlyingTicker = "TSLA",
            AsOfDate = Timestamp.FromDateTime(tmpBoundary),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetOptionChain(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpChain.WasInvoked.ShouldBeFalse();
    }

    [Fact]
    public async Task GetOptionChain_FutureAsOfDate_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpChain = new ThrowingChainProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: tmpChain);

        var tmpRequest = new GetOptionChainRequest
        {
            UnderlyingTicker = "TSLA",
            AsOfDate = Timestamp.FromDateTime(tmpBoundary.AddDays(3)),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetOptionChain(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
    }

    // GetOptionChain treats as_of as a single point — there's no
    // "straddling range" case for it. The other three RPCs cover that.

    // ─────────────────────────────────────────────────────────────────
    // GetMacro
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetMacro_AllPastRange_PassesValidation()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(-30);
        var tmpToUtc = tmpBoundary.AddDays(-1);

        var tmpMacro = new RecordingMacroProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: tmpMacro);

        var tmpRequest = new GetMacroRequest
        {
            SeriesId = "T10Y2Y",
            FromDate = Timestamp.FromDateTime(tmpFromUtc),
            ToDate = Timestamp.FromDateTime(tmpToUtc),
        };

        var tmpResp = await tmpImpl.GetMacro(tmpRequest, NewServerCallContext());
        tmpResp.ShouldNotBeNull();
        tmpMacro.WasInvoked.ShouldBeTrue();
    }

    [Fact]
    public async Task GetMacro_RangeEndingAtBoundary_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpMacro = new ThrowingMacroProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: tmpMacro);

        var tmpRequest = new GetMacroRequest
        {
            SeriesId = "T10Y2Y",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-7)),
            ToDate = Timestamp.FromDateTime(tmpBoundary),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetMacro(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpMacro.WasInvoked.ShouldBeFalse();
    }

    [Fact]
    public async Task GetMacro_RangeEntirelyInFuture_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpMacro = new ThrowingMacroProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: tmpMacro);

        var tmpRequest = new GetMacroRequest
        {
            SeriesId = "T10Y2Y",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(1)),
            ToDate = Timestamp.FromDateTime(tmpBoundary.AddDays(7)),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetMacro(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
    }

    [Fact]
    public async Task GetMacro_StraddlingRange_IsRejected_NotSilentlyTruncated()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpMacro = new ThrowingMacroProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: tmpMacro);

        var tmpRequest = new GetMacroRequest
        {
            SeriesId = "T10Y2Y",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-14)),
            ToDate = Timestamp.FromDateTime(tmpBoundary.AddDays(2)),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetMacro(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpMacro.WasInvoked.ShouldBeFalse();
    }

    // ─────────────────────────────────────────────────────────────────
    // GetDailyOptionsFlow (PR 1, daily_options_flow surface)
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetDailyOptionsFlow_AllPastRange_PassesValidationAndReachesProvider()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(-30);
        var tmpToUtc = tmpBoundary.AddDays(-1);

        var tmpFlow = new RecordingDailyOptionsFlowProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: tmpFlow);

        var tmpRequest = new GetDailyOptionsFlowRequest
        {
            UnderlyingTicker = "TSLA",
            FromDate = Timestamp.FromDateTime(tmpFromUtc),
            ToDate = Timestamp.FromDateTime(tmpToUtc),
        };

        var tmpResp = await tmpImpl.GetDailyOptionsFlow(tmpRequest, NewServerCallContext());
        tmpResp.ShouldNotBeNull();
        tmpResp.CacheHit.ShouldBeTrue();
        tmpResp.Rows.Count.ShouldBe(0);
        tmpFlow.WasInvoked.ShouldBeTrue();
    }

    [Fact]
    public async Task GetDailyOptionsFlow_RangeEndingAtBoundary_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFlow = new ThrowingDailyOptionsFlowProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: tmpFlow);

        var tmpRequest = new GetDailyOptionsFlowRequest
        {
            UnderlyingTicker = "TSLA",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-7)),
            ToDate = Timestamp.FromDateTime(tmpBoundary),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetDailyOptionsFlow(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpFlow.WasInvoked.ShouldBeFalse();
    }

    [Fact]
    public async Task GetDailyOptionsFlow_RangeEntirelyInFuture_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFlow = new ThrowingDailyOptionsFlowProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: tmpFlow);

        var tmpRequest = new GetDailyOptionsFlowRequest
        {
            UnderlyingTicker = "TSLA",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(1)),
            ToDate = Timestamp.FromDateTime(tmpBoundary.AddDays(7)),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetDailyOptionsFlow(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpFlow.WasInvoked.ShouldBeFalse();
    }

    [Fact]
    public async Task GetDailyOptionsFlow_StraddlingRange_IsRejected_NotSilentlyTruncated()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFlow = new ThrowingDailyOptionsFlowProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: tmpFlow);

        var tmpRequest = new GetDailyOptionsFlowRequest
        {
            UnderlyingTicker = "TSLA",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-14)),
            ToDate = Timestamp.FromDateTime(tmpBoundary.AddDays(2)),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetDailyOptionsFlow(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpFlow.WasInvoked.ShouldBeFalse();
    }

    [Fact]
    public async Task GetDailyOptionsFlow_InvertedRange_RejectedAsInvalidArgument()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFlow = new ThrowingDailyOptionsFlowProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: tmpFlow);

        var tmpRequest = new GetDailyOptionsFlowRequest
        {
            UnderlyingTicker = "TSLA",
            // from > to → InvalidArgument BEFORE the past-only guard runs.
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-1)),
            ToDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-30)),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetDailyOptionsFlow(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.InvalidArgument);
        tmpFlow.WasInvoked.ShouldBeFalse();
    }

    // ─────────────────────────────────────────────────────────────────
    // GetDailyAtmIv (Wave B / PR 5, daily_atm_iv surface)
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetDailyAtmIv_AllPastRange_PassesValidationAndReachesProvider()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(-30);
        var tmpToUtc = tmpBoundary.AddDays(-1);

        var tmpAtm = new RecordingDailyAtmIvProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: null,
            dailyAtmIvProvider: tmpAtm);

        var tmpRequest = new GetDailyAtmIvRequest
        {
            UnderlyingTicker = "TSLA",
            FromDate = Timestamp.FromDateTime(tmpFromUtc),
            ToDate = Timestamp.FromDateTime(tmpToUtc),
        };

        var tmpResp = await tmpImpl.GetDailyAtmIv(tmpRequest, NewServerCallContext());
        tmpResp.ShouldNotBeNull();
        tmpResp.CacheHit.ShouldBeTrue();
        tmpResp.Rows.Count.ShouldBe(0);
        tmpAtm.WasInvoked.ShouldBeTrue();
    }

    [Fact]
    public async Task GetDailyAtmIv_RangeEndingAtBoundary_IsRejected()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpAtm = new ThrowingDailyAtmIvProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: null,
            dailyAtmIvProvider: tmpAtm);

        var tmpRequest = new GetDailyAtmIvRequest
        {
            UnderlyingTicker = "TSLA",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-7)),
            ToDate = Timestamp.FromDateTime(tmpBoundary),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetDailyAtmIv(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpAtm.WasInvoked.ShouldBeFalse();
    }

    [Fact]
    public async Task GetDailyAtmIv_StraddlingRange_IsRejected_NotSilentlyTruncated()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpAtm = new ThrowingDailyAtmIvProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: null,
            dailyAtmIvProvider: tmpAtm);

        var tmpRequest = new GetDailyAtmIvRequest
        {
            UnderlyingTicker = "TSLA",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-14)),
            ToDate = Timestamp.FromDateTime(tmpBoundary.AddDays(2)),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetDailyAtmIv(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpAtm.WasInvoked.ShouldBeFalse();
    }

    [Fact]
    public async Task GetDailyAtmIv_InvertedRange_RejectedAsInvalidArgument()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpAtm = new ThrowingDailyAtmIvProvider();
        var tmpImpl = new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            new RecordingBarsProvider(),
            quotes: new RecordingQuotesProvider(),
            macroProvider: null,
            optionChainProvider: null,
            dailyOptionsFlowProvider: null,
            dailyAtmIvProvider: tmpAtm);

        var tmpRequest = new GetDailyAtmIvRequest
        {
            UnderlyingTicker = "TSLA",
            FromDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-1)),
            ToDate = Timestamp.FromDateTime(tmpBoundary.AddDays(-30)),
        };

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.GetDailyAtmIv(tmpRequest, NewServerCallContext()));
        tmpEx.StatusCode.ShouldBe(StatusCode.InvalidArgument);
        tmpAtm.WasInvoked.ShouldBeFalse();
    }

    // ─────────────────────────────────────────────────────────────────
    // Direct validator unit checks. Belt-and-braces — exercises the
    // helper without going through the gRPC surface.
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Validator_PastRange_DoesNotThrow()
    {
        var tmpBoundary = TodayBoundaryUtc();
        Should.NotThrow(() => PastOnlyRangeValidator.EnsurePastOnly(
            tmpBoundary.AddDays(-2), tmpBoundary.AddDays(-1)));
    }

    [Fact]
    public void Validator_BoundaryEqualToTo_Throws()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpEx = Should.Throw<RpcException>(() => PastOnlyRangeValidator.EnsurePastOnly(
            tmpBoundary.AddDays(-1), tmpBoundary));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
    }

    [Fact]
    public void Validator_PointAtBoundary_Throws()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpEx = Should.Throw<RpcException>(() => PastOnlyRangeValidator.EnsurePastOnly(tmpBoundary));
        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
    }

    [Fact]
    public void Validator_PointInPast_DoesNotThrow()
    {
        var tmpBoundary = TodayBoundaryUtc();
        Should.NotThrow(() => PastOnlyRangeValidator.EnsurePastOnly(tmpBoundary.AddSeconds(-1)));
    }

    // ─────────────────────────────────────────────────────────────────
    // Post-close same-day override (HISTORY_POST_CLOSE_OPEN_AT_ET env var).
    //
    // When set to "HH:mm" (ET) and the current ET wall-clock is at or
    // past that time, today's data becomes "past-day" — the boundary
    // advances to tomorrow's midnight ET so backtests can fetch the
    // current day's bars/NBBO/chains via gRPC.
    // ─────────────────────────────────────────────────────────────────

    private const string c_OverrideEnvVar = "HISTORY_POST_CLOSE_OPEN_AT_ET";

    /// <summary>
    /// Compute today's ET wall-clock time-of-day. Tests use this to
    /// pick override values that are deterministically in the past or
    /// future relative to wall-clock — no fragile sleep / clock-mock.
    /// </summary>
    private static TimeSpan NowEtTimeOfDay()
    {
        var tmpEt = ResolveEasternTz();
        return TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tmpEt).TimeOfDay;
    }

    [Fact]
    public void Validator_PostCloseOverride_Unset_PreservesMidnightBoundary()
    {
        // Sanity check: with the env var unset, validation behaves as the
        // pre-override baseline (rejects any timestamp at-or-after today's
        // midnight ET).
        var tmpPrev = Environment.GetEnvironmentVariable(c_OverrideEnvVar);
        Environment.SetEnvironmentVariable(c_OverrideEnvVar, null);
        try
        {
            var tmpBoundary = TodayBoundaryUtc();
            Should.Throw<RpcException>(() => PastOnlyRangeValidator.EnsurePastOnly(tmpBoundary))
                .StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        }
        finally
        {
            Environment.SetEnvironmentVariable(c_OverrideEnvVar, tmpPrev);
        }
    }

    [Fact]
    public void Validator_PostCloseOverride_BeforeOpenTime_PreservesMidnightBoundary()
    {
        // Override set to one hour AFTER current ET wall-clock → not yet
        // active → today still rejected.
        var tmpPrev = Environment.GetEnvironmentVariable(c_OverrideEnvVar);
        var tmpFutureOpenAt = NowEtTimeOfDay().Add(TimeSpan.FromHours(1));
        if (tmpFutureOpenAt >= TimeSpan.FromHours(24))
        {
            // Edge case: late-night run would wrap past 24h. Skip rather
            // than write a brittle assertion. Validator's behaviour at
            // the day-wrap boundary is covered by the other override tests.
            return;
        }
        Environment.SetEnvironmentVariable(
            c_OverrideEnvVar, $"{tmpFutureOpenAt.Hours:D2}:{tmpFutureOpenAt.Minutes:D2}");
        try
        {
            var tmpBoundary = TodayBoundaryUtc();
            Should.Throw<RpcException>(() => PastOnlyRangeValidator.EnsurePastOnly(tmpBoundary))
                .StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        }
        finally
        {
            Environment.SetEnvironmentVariable(c_OverrideEnvVar, tmpPrev);
        }
    }

    [Fact]
    public void Validator_PostCloseOverride_AfterOpenTime_AdvancesBoundary()
    {
        // Override set to "00:01" → guaranteed to be past for any test
        // run after 00:01 ET. Today's midnight UTC must now PASS (no
        // throw); anything at-or-after tomorrow's midnight ET still
        // fails.
        var tmpPrev = Environment.GetEnvironmentVariable(c_OverrideEnvVar);
        Environment.SetEnvironmentVariable(c_OverrideEnvVar, "00:01");
        try
        {
            // If the test happens to run at exactly 00:00:00–00:00:59
            // ET, the override hasn't fired yet. Skip rather than flake.
            if (NowEtTimeOfDay() < TimeSpan.FromMinutes(1)) return;

            var tmpBoundary = TodayBoundaryUtc();
            // Range ending at today's midnight (pre-override boundary)
            // used to fail; with the override active, that range is now
            // entirely past-day. Should pass.
            Should.NotThrow(() => PastOnlyRangeValidator.EnsurePastOnly(
                tmpBoundary.AddDays(-1), tmpBoundary));

            // But a point at tomorrow's midnight ET (the NEW boundary)
            // is still future → must throw.
            var tmpEt = ResolveEasternTz();
            var tmpNowEt = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tmpEt);
            var tmpTomorrowMidnightEt = new DateTime(
                tmpNowEt.Year, tmpNowEt.Month, tmpNowEt.Day, 0, 0, 0, DateTimeKind.Unspecified)
                .AddDays(1);
            var tmpTomorrowMidnightUtc = TimeZoneInfo.ConvertTimeToUtc(tmpTomorrowMidnightEt, tmpEt);
            Should.Throw<RpcException>(() =>
                PastOnlyRangeValidator.EnsurePastOnly(tmpTomorrowMidnightUtc))
                .StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        }
        finally
        {
            Environment.SetEnvironmentVariable(c_OverrideEnvVar, tmpPrev);
        }
    }

    [Fact]
    public void Validator_PostCloseOverride_Malformed_FallsBackToDefaultBoundary()
    {
        // Garbage value → TimeSpan.TryParse fails → validator silently
        // falls through to the default midnight-ET boundary. Belt-and-
        // braces against operator typos in compose env vars.
        var tmpPrev = Environment.GetEnvironmentVariable(c_OverrideEnvVar);
        Environment.SetEnvironmentVariable(c_OverrideEnvVar, "not-a-time");
        try
        {
            var tmpBoundary = TodayBoundaryUtc();
            Should.Throw<RpcException>(() => PastOnlyRangeValidator.EnsurePastOnly(tmpBoundary))
                .StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        }
        finally
        {
            Environment.SetEnvironmentVariable(c_OverrideEnvVar, tmpPrev);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // EnsureRangeCached (server-streaming RPC).
    //
    // The validator must fire BEFORE any stream write. An invalid range
    // must throw FAILED_PRECONDITION immediately and leave the capturing
    // stream empty (zero progress messages).
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EnsureRangeCached_RangeEndingAtBoundary_IsRejected_WithZeroProgressEmitted()
    {
        // to == boundary → includes today's first instant → rejected.
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(-1);
        var tmpToUtc = tmpBoundary;

        var tmpImpl = NewImplWith(new ThrowingBarsProvider());
        var tmpRequest = new EnsureRangeCachedRequest
        {
            FromTs = Timestamp.FromDateTime(DateTime.SpecifyKind(tmpFromUtc, DateTimeKind.Utc)),
            ToTs = Timestamp.FromDateTime(DateTime.SpecifyKind(tmpToUtc, DateTimeKind.Utc)),
        };
        tmpRequest.Symbols.Add("TSLA");
        tmpRequest.DataClasses.Add(DataClass.Bars);

        var tmpStream = new CapturingStreamWriter<EnsureRangeCachedProgress>();

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.EnsureRangeCached(tmpRequest, tmpStream, NewServerCallContext()));

        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpEx.Status.Detail.ShouldContain("past-day data only");
        tmpEx.Status.Detail.ShouldContain("ET");
        // The validator fires before any stream write — no progress at all.
        tmpStream.Captured.Count.ShouldBe(0,
            "no progress messages should be emitted before the validator rejects the range");
    }

    [Fact]
    public async Task EnsureRangeCached_RangeEntirelyInFuture_IsRejected_WithZeroProgressEmitted()
    {
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(1);
        var tmpToUtc = tmpBoundary.AddDays(7);

        var tmpImpl = NewImplWith(new ThrowingBarsProvider());
        var tmpRequest = new EnsureRangeCachedRequest
        {
            FromTs = Timestamp.FromDateTime(DateTime.SpecifyKind(tmpFromUtc, DateTimeKind.Utc)),
            ToTs = Timestamp.FromDateTime(DateTime.SpecifyKind(tmpToUtc, DateTimeKind.Utc)),
        };
        tmpRequest.Symbols.Add("TSLA");
        tmpRequest.DataClasses.Add(DataClass.Bars);

        var tmpStream = new CapturingStreamWriter<EnsureRangeCachedProgress>();

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.EnsureRangeCached(tmpRequest, tmpStream, NewServerCallContext()));

        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        tmpStream.Captured.Count.ShouldBe(0);
    }

    [Fact]
    public async Task EnsureRangeCached_StraddlingRange_IsRejected_WithZeroProgressEmitted()
    {
        // from in the past, to in the future. No partial execution.
        var tmpBoundary = TodayBoundaryUtc();
        var tmpFromUtc = tmpBoundary.AddDays(-1);
        var tmpToUtc = tmpBoundary.AddHours(2);

        var tmpProvider = new ThrowingBarsProvider();
        var tmpImpl = NewImplWith(tmpProvider);
        var tmpRequest = new EnsureRangeCachedRequest
        {
            FromTs = Timestamp.FromDateTime(DateTime.SpecifyKind(tmpFromUtc, DateTimeKind.Utc)),
            ToTs = Timestamp.FromDateTime(DateTime.SpecifyKind(tmpToUtc, DateTimeKind.Utc)),
        };
        tmpRequest.Symbols.Add("TSLA");
        tmpRequest.DataClasses.Add(DataClass.Bars);

        var tmpStream = new CapturingStreamWriter<EnsureRangeCachedProgress>();

        var tmpEx = await Should.ThrowAsync<RpcException>(
            () => tmpImpl.EnsureRangeCached(tmpRequest, tmpStream, NewServerCallContext()));

        tmpEx.StatusCode.ShouldBe(StatusCode.FailedPrecondition);
        // Validator fires before any provider touch.
        tmpProvider.WasInvoked.ShouldBeFalse();
        tmpStream.Captured.Count.ShouldBe(0);
    }

    // ─────────────────────────────────────────────────────────────────
    // Wiring helpers + stub providers.
    // ─────────────────────────────────────────────────────────────────

    private static HistoryServiceImpl NewImplWith(IHistoricalBarsProvider inBars)
        => new HistoryServiceImpl(
            NullLogger<HistoryServiceImpl>.Instance,
            inBars,
            quotes: new RecordingQuotesProvider(),
            macroProvider: null);

    private static GetBarsRequest NewBarsRequest(DateTime inFromUtc, DateTime inToUtc)
        => new()
        {
            Symbol = "TSLA",
            Timeframe = BarTimeframe.Minute,
            FromTs = Timestamp.FromDateTime(DateTime.SpecifyKind(inFromUtc, DateTimeKind.Utc)),
            ToTs = Timestamp.FromDateTime(DateTime.SpecifyKind(inToUtc, DateTimeKind.Utc)),
        };

    private static ServerCallContext NewServerCallContext()
        => new TestServerCallContext(CancellationToken.None);

    private sealed class TestServerCallContext : ServerCallContext
    {
        private readonly CancellationToken m_Ct;
        public TestServerCallContext(CancellationToken inCt) { m_Ct = inCt; }
        protected override string MethodCore => "/mbd.history.v1.HistoryService/Test";
        protected override string HostCore => "localhost";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.UtcNow.AddSeconds(30);
        protected override Metadata RequestHeadersCore => new();
        protected override CancellationToken CancellationTokenCore => m_Ct;
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new("", new Dictionary<string, List<AuthProperty>>());
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }

    /// <summary>Records that it was called and returns an empty result.</summary>
    private sealed class RecordingBarsProvider : IHistoricalBarsProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<BarsReadResult> GetBarsAsync(string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            DomainBarTimeframe inTimeframe, CancellationToken inCt = default)
        {
            WasInvoked = true;
            return Task.FromResult(new BarsReadResult(Array.Empty<DomainBar>(), CacheHit: false));
        }
        public Task<int> EnsureRangeCachedAsync(string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            DomainBarTimeframe inTimeframe,
            Func<BarsWarmupProgress, CancellationToken, Task>? inProgress = null, CancellationToken inCt = default)
            => Task.FromResult(0);
    }

    /// <summary>Throws if invoked — proves the validator short-circuits.</summary>
    private sealed class ThrowingBarsProvider : IHistoricalBarsProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<BarsReadResult> GetBarsAsync(string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            DomainBarTimeframe inTimeframe, CancellationToken inCt = default)
        {
            WasInvoked = true;
            throw new InvalidOperationException("Bars provider must not be invoked when validation rejects the range.");
        }
        public Task<int> EnsureRangeCachedAsync(string inSymbol, DateTime inFromUtc, DateTime inToUtc,
            DomainBarTimeframe inTimeframe,
            Func<BarsWarmupProgress, CancellationToken, Task>? inProgress = null, CancellationToken inCt = default)
            => throw new InvalidOperationException("Should not be invoked.");
    }

    private sealed class RecordingQuotesProvider : IOptionQuotesProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<OptionQuotesLookup> GetAtOrBeforeAsync(string inTicker, DateTime inTsUtc, CancellationToken inCt = default)
        {
            WasInvoked = true;
            return Task.FromResult(new OptionQuotesLookup(Quote: null, CacheHit: false, IsMissMarker: true));
        }
    }

    private sealed class ThrowingQuotesProvider : IOptionQuotesProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<OptionQuotesLookup> GetAtOrBeforeAsync(string inTicker, DateTime inTsUtc, CancellationToken inCt = default)
        {
            WasInvoked = true;
            throw new InvalidOperationException("Quotes provider must not be invoked when validation rejects.");
        }
    }

    private sealed class RecordingChainProvider : IOptionChainProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<OptionChainResult> GetChainAsync(string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
        {
            WasInvoked = true;
            return Task.FromResult(new OptionChainResult(Array.Empty<OptionContractRow>(), CacheHit: false, IsMissMarker: false));
        }
        public Task EnsureChainCachedAsync(string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
            => Task.CompletedTask;
        public Task<int> EnsureRangeCachedAsync(string inSymbol, DateOnly inFromDate, DateOnly inToDate, CancellationToken inCt)
            => Task.FromResult(0);
    }

    private sealed class ThrowingChainProvider : IOptionChainProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<OptionChainResult> GetChainAsync(string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
        {
            WasInvoked = true;
            throw new InvalidOperationException("Chain provider must not be invoked when validation rejects.");
        }
        public Task EnsureChainCachedAsync(string inSymbol, DateOnly inAsOfDate, CancellationToken inCt)
            => throw new InvalidOperationException("Should not be invoked.");
        public Task<int> EnsureRangeCachedAsync(string inSymbol, DateOnly inFromDate, DateOnly inToDate, CancellationToken inCt)
            => throw new InvalidOperationException("Should not be invoked.");
    }

    private sealed class RecordingMacroProvider : IMacroDataProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<IReadOnlyList<FredObservationRow>> GetSeriesAsync(string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            WasInvoked = true;
            return Task.FromResult<IReadOnlyList<FredObservationRow>>(Array.Empty<FredObservationRow>());
        }
        public Task EnsureRangeCachedAsync(string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            WasInvoked = true;
            return Task.CompletedTask;
        }
        public Task EnsureRangeCachedAsync(IEnumerable<string> seriesIds, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            WasInvoked = true;
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingDailyOptionsFlowProvider : IDailyOptionsFlowProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<IReadOnlyList<ProviderDailyOptionsFlowRow>> GetRangeAsync(
            string inSymbol, DateOnly inFrom, DateOnly inTo, CancellationToken inCt = default)
        {
            WasInvoked = true;
            return Task.FromResult<IReadOnlyList<ProviderDailyOptionsFlowRow>>(
                Array.Empty<ProviderDailyOptionsFlowRow>());
        }
        // PR 2 — write surface. Read-path tests don't exercise it; both
        // methods are no-ops here.
        public Task UpsertAsync(IReadOnlyList<ProviderDailyOptionsFlowRow> inRows, CancellationToken inCt = default)
            => Task.CompletedTask;
        public Task RecordMissAsync(string inSymbol, DateOnly inFromDate, DateOnly inToDate, string inReason, CancellationToken inCt = default)
            => Task.CompletedTask;
    }

    private sealed class ThrowingDailyOptionsFlowProvider : IDailyOptionsFlowProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<IReadOnlyList<ProviderDailyOptionsFlowRow>> GetRangeAsync(
            string inSymbol, DateOnly inFrom, DateOnly inTo, CancellationToken inCt = default)
        {
            WasInvoked = true;
            throw new InvalidOperationException(
                "DailyOptionsFlow provider must not be invoked when validation rejects.");
        }
        // PR 2 — write surface. Validation rejects before write; throw if
        // anyone reaches these methods to surface the bug.
        public Task UpsertAsync(IReadOnlyList<ProviderDailyOptionsFlowRow> inRows, CancellationToken inCt = default)
            => throw new InvalidOperationException("UpsertAsync must not be invoked when validation rejects.");
        public Task RecordMissAsync(string inSymbol, DateOnly inFromDate, DateOnly inToDate, string inReason, CancellationToken inCt = default)
            => throw new InvalidOperationException("RecordMissAsync must not be invoked when validation rejects.");
    }

    private sealed class RecordingDailyAtmIvProvider : IDailyAtmIvProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<IReadOnlyList<ProviderDailyAtmIvRow>> GetRangeAsync(
            string inSymbol, DateOnly inFrom, DateOnly inTo, CancellationToken inCt = default)
        {
            WasInvoked = true;
            return Task.FromResult<IReadOnlyList<ProviderDailyAtmIvRow>>(
                Array.Empty<ProviderDailyAtmIvRow>());
        }
        // Wave C / PR 6 — write surface. Read-path tests don't exercise it.
        public Task UpsertAsync(IReadOnlyList<ProviderDailyAtmIvRow> inRows, CancellationToken inCt = default)
            => Task.CompletedTask;
        public Task RecordMissAsync(string inSymbol, DateOnly inFromDate, DateOnly inToDate, string inReason, CancellationToken inCt = default)
            => Task.CompletedTask;
    }

    private sealed class ThrowingDailyAtmIvProvider : IDailyAtmIvProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<IReadOnlyList<ProviderDailyAtmIvRow>> GetRangeAsync(
            string inSymbol, DateOnly inFrom, DateOnly inTo, CancellationToken inCt = default)
        {
            WasInvoked = true;
            throw new InvalidOperationException(
                "DailyAtmIv provider must not be invoked when validation rejects.");
        }
        public Task UpsertAsync(IReadOnlyList<ProviderDailyAtmIvRow> inRows, CancellationToken inCt = default)
            => throw new InvalidOperationException("UpsertAsync must not be invoked when validation rejects.");
        public Task RecordMissAsync(string inSymbol, DateOnly inFromDate, DateOnly inToDate, string inReason, CancellationToken inCt = default)
            => throw new InvalidOperationException("RecordMissAsync must not be invoked when validation rejects.");
    }

    private sealed class ThrowingMacroProvider : IMacroDataProvider
    {
        public bool WasInvoked { get; private set; }
        public Task<IReadOnlyList<FredObservationRow>> GetSeriesAsync(string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            WasInvoked = true;
            throw new InvalidOperationException("Macro provider must not be invoked when validation rejects.");
        }
        public Task EnsureRangeCachedAsync(string seriesId, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            WasInvoked = true;
            throw new InvalidOperationException("Macro provider must not be invoked when validation rejects.");
        }
        public Task EnsureRangeCachedAsync(IEnumerable<string> seriesIds, DateOnly fromDate, DateOnly toDate, CancellationToken ct)
        {
            WasInvoked = true;
            throw new InvalidOperationException("Macro provider must not be invoked when validation rejects.");
        }
    }

    /// <summary>
    /// Captures every message written to the stream. Used by the
    /// EnsureRangeCached tests to assert that zero progress messages are
    /// emitted before a FAILED_PRECONDITION rejection.
    /// </summary>
    private sealed class CapturingStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Captured { get; } = new();
        public WriteOptions? WriteOptions { get; set; }
        public Task WriteAsync(T message)
        {
            Captured.Add(message);
            return Task.CompletedTask;
        }
    }
}
