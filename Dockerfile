# Multi-stage Linux image for agregat.
# Ships static FFmpeg (GPL) so runtime does not depend on flaky apt mirrors.
# Optional GPU: docker run --gpus all -e FFMPEG_HWACCEL=cuda ...

ARG RUST_VERSION=1.89

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /app

RUN set -eux; \
    for i in 1 2 3 4 5; do \
      apt-get update && break || sleep 5; \
    done; \
    apt-get install -y --no-install-recommends pkg-config curl ca-certificates xz-utils; \
    rm -rf /var/lib/apt/lists/*

# Static FFmpeg for the runtime stage (linux amd64)
RUN set -eux; \
    curl -fsSL -o /tmp/ffmpeg.tar.xz \
      "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz"; \
    mkdir -p /opt/ffmpeg; \
    tar -xJf /tmp/ffmpeg.tar.xz -C /opt/ffmpeg --strip-components=1; \
    /opt/ffmpeg/bin/ffmpeg -version | head -n 2; \
    rm -f /tmp/ffmpeg.tar.xz

COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY src ./src

RUN cargo build --release --locked \
    && strip target/release/agregat

FROM debian:bookworm-slim AS runtime

# Minimal packages; retry because Docker Desktop DNS/TLS can be flaky
RUN set -eux; \
    for i in 1 2 3 4 5 6 7 8; do \
      apt-get update && \
      apt-get install -y --no-install-recommends ca-certificates curl && \
      break || sleep 8; \
    done; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --create-home --uid 10001 --shell /usr/sbin/nologin agregat

WORKDIR /app

COPY --from=builder /app/target/release/agregat /usr/local/bin/agregat
COPY --from=builder /opt/ffmpeg/bin/ffmpeg /usr/local/bin/ffmpeg
COPY --from=builder /opt/ffmpeg/bin/ffprobe /usr/local/bin/ffprobe
COPY config.docker.json /app/config.json

RUN mkdir -p /app/output \
    && chown -R agregat:agregat /app

USER agregat

ENV RUST_LOG=info \
    FFMPEG_PATH=/usr/local/bin/ffmpeg \
    FFMPEG_HWACCEL=none \
    AGREGAT_CONFIG=/app/config.json \
    PATH="/usr/local/bin:${PATH}"

EXPOSE 7528

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://127.0.0.1:7528/health || exit 1

VOLUME ["/app/output"]

ENTRYPOINT ["/usr/local/bin/agregat"]
CMD ["--port", "7528", "--thumb-dir", "/app/output"]
