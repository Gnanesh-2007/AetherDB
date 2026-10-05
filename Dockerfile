# ------------------------------------------------------------------------------
# AetherDB Multi-Stage Dockerfile
# Production-ready container image for AetherDB storage engine & CLI
# ------------------------------------------------------------------------------

FROM rust:1.80-slim-bookworm AS builder

WORKDIR /usr/src/aetherdb

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace manifests and code
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Compile optimized release binaries
ENV RUSTFLAGS="-C target-cpu=native"
RUN cargo build --release --bin aether-server --bin aether

# ------------------------------------------------------------------------------
# Runtime Image
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gosu \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create non-root aether user and persistent directories
RUN groupadd -g 10001 aether && \
    useradd -u 10001 -g aether -s /bin/sh -d /app aether && \
    mkdir -p /data /app && \
    chown -R aether:aether /data /app

# Copy binaries and entrypoint
COPY --from=builder /usr/src/aetherdb/target/release/aether-server /usr/local/bin/aether-server
COPY --from=builder /usr/src/aetherdb/target/release/aether /usr/local/bin/aether
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/aether-server /usr/local/bin/aether

# Expose Raft/TCP (8300) and HTTP REST Gateway (8301)
EXPOSE 8300 8301

VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["aether-server", "--node-id", "1", "--addr", "0.0.0.0:8300", "--http-addr", "0.0.0.0:8301", "--data-dir", "/data"]
