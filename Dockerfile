# mlua-swarm (mse) — multi-stage build
#
# Published to ghcr.io/ynishi/mse via .github/workflows/release.yml
# (publish-docker-image job, triggered on version tag push).

FROM rust:1.77-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --package mlua-swarm-cli --bin mse

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/mse /usr/local/bin/mse
ENTRYPOINT ["/usr/local/bin/mse"]
CMD ["serve"]
