# tools/build.ps1 — build the mbd-history compose service with git metadata baked in.
#
# Usage (from any subdirectory of the repo):
#   pwsh tools/build.ps1                       # rebuild mbd-history with current HEAD metadata
#   pwsh tools/build.ps1 -Services mbd-history # explicit
#   pwsh tools/build.ps1 -NoBuild              # print env + exit (dry-run)
#
# Mirror of the MBD repo's tools/build.ps1 (PR #144). Adapted to default
# to the mbd-history service rather than `api`.

[CmdletBinding()]
param(
  [String[]] $Services = @("mbd-history"),
  [Switch] $NoBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

$GitCommit = (git rev-parse HEAD).Trim()
$GitBranch = (git rev-parse --abbrev-ref HEAD).Trim()
if ($GitBranch -eq "HEAD") { $GitBranch = "detached" }

$BuildTime = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

$Env:GIT_COMMIT = $GitCommit
$Env:GIT_BRANCH = $GitBranch
$Env:BUILD_TIME = $BuildTime

Write-Host "Repo root:    $RepoRoot"
Write-Host "GIT_COMMIT:   $GitCommit"
Write-Host "GIT_BRANCH:   $GitBranch"
Write-Host "BUILD_TIME:   $BuildTime"
Write-Host "Services:     $($Services -join ' ')"

if ($NoBuild) {
  Write-Host "(dry-run; -NoBuild specified, skipping docker build)"
  return
}

Write-Host ""
Write-Host "Running: docker compose build $($Services -join ' ')"
docker compose build @Services
if ($LASTEXITCODE -ne 0) {
  throw "docker compose build failed with exit code $LASTEXITCODE"
}

Write-Host ""
Write-Host "Build complete. To deploy:"
Write-Host "  docker compose up -d --no-deps --force-recreate $($Services -join ' ')"
