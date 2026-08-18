# syntax=docker/dockerfile:1.7
#
# arc-ui — multi-stage build.
#
#   1. deps      — module download only. Keyed on go.mod/go.sum alone, so editing
#                  Go source never re-downloads the module graph.
#   2. generate  — the code generators: `go tool templ generate` for the HTML
#                  templates and the Tailwind standalone binary for the CSS.
#   3. build     — cross-compiles the static binary for $TARGETOS/$TARGETARCH
#                  and stages the /data directory the runtime cannot create.
#   4. runtime   — distroless static. No shell, no package manager, nonroot.
#
# The build context is the repository root — which is also where this file lives,
# so the COPY paths below are plain. That matches what .github/workflows/ci.yml
# and release-image.yml pass (`context: .`, `file: Dockerfile`). Build it by hand
# with:
#
#     docker build -t arc-ui:local .
#
# compose.yaml and `task docker:build` both already do this.
#
# Every stage but the last is pinned to --platform=$BUILDPLATFORM: with
# CGO_ENABLED=0 Go cross-compiles for free, so nothing has to run under QEMU.
# Emulating an arm64 builder to run templ and Tailwind costs minutes of wall clock
# and OOMs regularly, for zero benefit — both generators only ever emit source.

ARG GO_VERSION=1.26
ARG TAILWIND_VERSION=v4.1.14

# ---------------------------------------------------------------------------
# 1. deps
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS deps

WORKDIR /src

# Manifests before sources, so a code edit reuses this layer. The go.mod `tool`
# directive puts templ and ent into the module graph, so this download also
# fetches the generators — no separate `go install` is needed, and they cannot
# drift from the libraries the binary links against.
COPY go.mod go.sum ./
RUN go mod download

# ---------------------------------------------------------------------------
# 2. generate — templ + Tailwind
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM deps AS generate

ARG TAILWIND_VERSION
ARG BUILDARCH

# Tailwind v4 ships a standalone binary; three traps live in this one RUN.
#
#  * The release asset for x86_64 is named `x64`, not `amd64`. Docker's
#    $BUILDARCH says `amd64`, so the unmapped URL 404s and the build failure
#    reads like a transient network error. Map it.
#  * The binary is a Bun build and is NOT statically linked — it needs glibc.
#    That is why this stage sits on the Debian golang image and pulls the plain
#    `linux-x64`/`linux-arm64` asset. The `-musl` variants exist for Alpine
#    builders only; using one here fails at exec time, not at download time.
#  * The version is pinned by ARG. An unpinned "latest" makes the generated CSS
#    non-reproducible between two builds of the same commit.
RUN set -eu; \
    arch="${BUILDARCH}"; \
    case "${arch}" in \
      amd64) arch=x64 ;; \
      arm64) arch=arm64 ;; \
      *) echo "unsupported build arch: ${arch}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /usr/local/bin/tailwindcss \
      "https://github.com/tailwindlabs/tailwindcss/releases/download/${TAILWIND_VERSION}/tailwindcss-linux-${arch}"; \
    chmod +x /usr/local/bin/tailwindcss

COPY . ./

# `go tool templ` runs the version pinned in go.mod, which is the same version the
# runtime library is compiled against. `go install .../templ@latest` would let the
# generator drift from the library and emit code that no longer builds.
RUN go tool templ generate ./internal/web/...

# Tailwind scans the .templ SOURCES (see assets/input.css). Auto-detection would
# skip them here for the same reason it does locally — generated files are
# gitignored — and emit a stylesheet with none of the app's utilities in it.
RUN tailwindcss -i assets/input.css -o internal/web/static/app.css --minify

# ---------------------------------------------------------------------------
# 3. build
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM generate AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/arc-ui ./cmd/arc-ui

# Distroless has no shell and no mkdir, so the SQLite directory has to be built
# here and copied over: COPY of a directory creates it in the target image.
RUN install -d -m 0755 -o 65532 -g 65532 /data

# ---------------------------------------------------------------------------
# 4. runtime
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

COPY --from=build /out/arc-ui /usr/local/bin/arc-ui
COPY --from=build --chown=65532:65532 /data /data

# Numeric UID/GID on purpose. Kubernetes' runAsNonRoot admission check has to
# decide before the container starts whether the user is root, and not every
# runtime can resolve a *name* out of the image's /etc/passwd to do it — a
# `USER nonroot` image gets rejected with "container has runAsNonRoot and image
# has non-numeric user" on those runtimes.
USER 65532:65532

ENV ARC_UI_HTTP_ADDR=0.0.0.0:8080 \
    ARC_UI_DB_PATH=/data/arc-ui.db

EXPOSE 8080

VOLUME ["/data"]

# Exec form only — there is no /bin/sh to interpret a shell-form CMD. For the
# same reason there is no HEALTHCHECK here: no curl, no wget. compose.yaml probes
# with the binary's own `healthcheck` subcommand instead.
ENTRYPOINT ["/usr/local/bin/arc-ui"]
CMD ["serve"]
