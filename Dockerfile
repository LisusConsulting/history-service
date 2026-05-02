# Multi-stage build for mbd-history.
#
# Stage 1: SDK image restores + builds + publishes the service.
# Stage 2: ASP.NET runtime image hosts the published output.
#
# Build context is the repo root so the SDK stage can copy:
#   Directory.Packages.props   (central package version management)
#   src/                       (HistoryService + Contracts)
#   external/                  (polygon-net-client submodule —
#                               source-level ProjectReference targets,
#                               see csproj <ProjectReference> entries)
# The tests project is intentionally excluded — CI runs tests on a separate
# `dotnet test` step against the source tree, not against the Docker image.

ARG DOTNET_VERSION=10.0
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build
WORKDIR /src

# Copy central props first so restore caches by package list, not source.
COPY Directory.Packages.props ./
COPY src/MomentumBreakoutDetector.HistoryService.Contracts/MomentumBreakoutDetector.HistoryService.Contracts.csproj src/MomentumBreakoutDetector.HistoryService.Contracts/
COPY src/MomentumBreakoutDetector.HistoryService/MomentumBreakoutDetector.HistoryService.csproj src/MomentumBreakoutDetector.HistoryService/

# polygon-net-client submodule — referenced via ProjectReference from the
# HistoryService csproj, so restore needs the csprojs (and the submodule's
# own Directory.Build.props / Directory.Packages.props which scope its
# package versions independently from the service tree).
COPY external/polygon-net-client/Directory.Build.props external/polygon-net-client/
COPY external/polygon-net-client/Directory.Packages.props external/polygon-net-client/
COPY external/polygon-net-client/TreyThomasCodes.Polygon.Models/TreyThomasCodes.Polygon.Models.csproj external/polygon-net-client/TreyThomasCodes.Polygon.Models/
COPY external/polygon-net-client/TreyThomasCodes.Polygon.RestClient/TreyThomasCodes.Polygon.RestClient.csproj external/polygon-net-client/TreyThomasCodes.Polygon.RestClient/

RUN dotnet restore src/MomentumBreakoutDetector.HistoryService/MomentumBreakoutDetector.HistoryService.csproj

# Now copy the rest of the source.
COPY src/ src/
COPY external/ external/

RUN dotnet publish src/MomentumBreakoutDetector.HistoryService/MomentumBreakoutDetector.HistoryService.csproj \
      -c Release \
      -o /app/publish \
      --no-restore \
      /p:UseAppHost=false

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:${DOTNET_VERSION} AS runtime
WORKDIR /app

# wget for the docker-compose healthcheck.
RUN apt-get update \
 && apt-get install -y --no-install-recommends wget \
 && rm -rf /var/lib/apt/lists/*

ARG GIT_COMMIT
ARG GIT_BRANCH
ARG BUILD_TIME
ENV History__GitCommit=${GIT_COMMIT} \
    History__GitBranch=${GIT_BRANCH} \
    History__BuildTime=${BUILD_TIME}

COPY --from=build /app/publish ./

# 8080 = gRPC (h2c), 8081 = HTTP/1 (/health + banner). Endpoints declared
# in appsettings.json's Kestrel section.
EXPOSE 8080 8081
ENTRYPOINT ["dotnet", "MomentumBreakoutDetector.HistoryService.dll"]
