# syntax=docker/dockerfile:1
#
# arc-ui container image.
#
#   1. deps    — module download only. Keyed on go.mod/go.sum alone, so editing
#                Go source never re-downloads the module graph.
#   2. build   — cross-compiles the static binary for $TARGETOS/$TARGETARCH and
#                stamps the version metadata in via -ldflags.
#   3. runtime — distroless static. No shell, no package manager, nonroot.
#
# SCAFFOLDING: the application this builds is a placeholder (see cmd/arc-ui).
# The *structure* is not a placeholder though — it is what the real build wants,
# so replacing the Go code should not mean rewriting this file.
#
# Build context is the repository root:
#
#     docker build -t arc-ui:dev .
#
# Both base images are pinned by digest, not just tag. A tag is a mutable
# pointer: `nonroot` moves whenever distroless rebuilds, which silently changes
# what ships in your image. The tag is kept alongside the digest purely so a
# human can read what it is, and so Dependabot's docker ecosystem can bump the
# pair together.

# ---------------------------------------------------------------------------
# 1. deps
# ---------------------------------------------------------------------------
# --platform=$BUILDPLATFORM keeps this stage on the runner's native
# architecture. With CGO_ENABLED=0 Go cross-compiles for free, so there is never
# a reason to emulate a foreign builder under QEMU — that costs minutes of wall
# clock and OOMs regularly, for zero benefit.
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm@sha256:116d58cbd88c1297624acc6e967a060012422bacf9930927e23fb719189c6f36 AS deps

WORKDIR /src

# Manifests before sources, so a code edit reuses this layer. The go.sum glob is
# deliberate: this module has no third-party dependencies yet, so the file does
# not exist. A plain `COPY go.mod go.sum ./` would fail the build today and a
# glob costs nothing once the file appears.
COPY go.mod go.sum* ./

# A cache mount rather than a plain `RUN go mod download`: on a cache miss the
# layer still rebuilds, but the module cache underneath survives, so it
# re-downloads only what actually changed. Blacksmith's builder keeps these
# mounts on its sticky disk across runs.
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download -x

# ---------------------------------------------------------------------------
# 2. build
# ---------------------------------------------------------------------------
FROM deps AS build

# Supplied automatically by BuildKit from the requested --platform.
ARG TARGETOS
ARG TARGETARCH

# Build metadata, injected into internal/version at link time. The defaults
# match that package's own defaults so a bare `docker build` still produces a
# runnable image.
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

COPY . .

# -trimpath strips local filesystem paths from the binary, which is both a
#   reproducibility and an information-disclosure win.
# -s -w drop the symbol table and DWARF data; nothing debugs this binary in
#   production, and it removes several MB from an image whose whole point is to
#   be minimal.
# CGO_ENABLED=0 is what makes the result runnable on distroless *static*.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/WindKube/actions-runners-controller-ui/internal/version.Version=${VERSION} \
        -X github.com/WindKube/actions-runners-controller-ui/internal/version.Commit=${COMMIT} \
        -X github.com/WindKube/actions-runners-controller-ui/internal/version.Date=${BUILD_DATE}" \
      -o /out/arc-ui \
      ./cmd/arc-ui

# ---------------------------------------------------------------------------
# 3. runtime
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a AS runtime

# Repeated here because ARGs do not cross stage boundaries. Only VERSION is
# needed, for the OCI label.
ARG VERSION=dev

# These are a fallback. The release workflow passes the authoritative set via
# docker/metadata-action, which overrides anything declared here.
LABEL org.opencontainers.image.title="arc-ui" \
      org.opencontainers.image.description="Read-only web UI for Actions Runner Controller" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/WindKube/actions-runners-controller-ui"

COPY --from=build /out/arc-ui /usr/local/bin/arc-ui

# 65532:65532 is distroless's `nonroot` user. Stated numerically rather than by
# name so a Kubernetes runAsUser check and a `docker run` agree on the value.
USER 65532:65532

EXPOSE 8080

# No HEALTHCHECK: this image has no shell and no curl to run one with, and the
# only orchestrator that matters here is Kubernetes, whose probes are configured
# on the Pod rather than baked into the image. /healthz is the endpoint to point
# them at.

ENTRYPOINT ["/usr/local/bin/arc-ui"]
CMD ["-addr", ":8080"]
