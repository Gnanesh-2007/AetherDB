# ------------------------------------------------------------------------------
# AetherDB Multi-Stage Dockerfile
# Builds high-performance, SIMD-accelerated distributed storage binary
# ------------------------------------------------------------------------------

FROM rust:1.80-slim-bookworm AS builder

WORKDIR /usr/src/aetherdb

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace source manifests and code
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build release binaries with native CPU SIMD optimization flags
ENV RUSTFLAGS="-C target-cpu=native"
RUN cargo build --release --bin aether-server --bin aether-cli --bin aether-bench --bin aether-cloud

# ------------------------------------------------------------------------------
# Runtime Image
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy compiled binaries from builder stage
COPY --from=builder /usr/src/aetherdb/target/release/aether-server /usr/local/bin/aether-server
COPY --from=builder /usr/src/aetherdb/target/release/aether-cli /usr/local/bin/aether-cli
COPY --from=builder /usr/src/aetherdb/target/release/aether-bench /usr/local/bin/aether-bench
COPY --from=builder /usr/src/aetherdb/target/release/aether-cloud /usr/local/bin/aether-cloud

# Expose TCP Binary protocol (8300), HTTP REST Gateway (8301), and AetherCloud Control Plane (8400)
EXPOSE 8300 8301 8400

# Create default data directory volume
RUN mkdir -p /app/data
VOLUME ["/app/data"]

# Default entrypoint starts AetherDB server
ENTRYPOINT ["aether-server"]
CMD ["--node-id", "1", "--addr", "0.0.0.0:8300", "--http-addr", "0.0.0.0:8301", "--data-dir", "/app/data"]
