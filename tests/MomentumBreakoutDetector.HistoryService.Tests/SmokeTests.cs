using MomentumBreakoutDetector.HistoryService;
using Shouldly;
using Xunit;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Phase 1, micro-PR #1 — single smoke test that proves the test project
/// builds and references the service project. Real test build-out lands
/// in micro-PR #8 (Testcontainers + integration suite).
/// </summary>
public class SmokeTests
{
    [Fact]
    public void HistoryServiceOptions_HasSaneDefaults()
    {
        var opts = new HistoryServiceOptions();
        opts.ConnectionString.ShouldNotBeNullOrWhiteSpace();
        opts.ConnectionString.ShouldContain("Host=");
        HistoryServiceOptions.SectionName.ShouldBe("History");
    }
}
