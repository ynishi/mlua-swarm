# mlua-swarm (mse) — multi-stage build
#
# Published to ghcr.io/ynishi/mse via .github/workflows/release.yml
# (publish-docker-image job, triggered on version tag push).
#
# The same image also serves as the OCI package for the MCP Registry
# listing (server.json). `mse mcp` speaks MCP over stdio; run with
# `docker run -i --rm ghcr.io/ynishi/mse:<version> mcp` to override the
# default `serve` (HTTP) CMD. The io.modelcontextprotocol.server.name
# LABEL below must stay in sync with server.json's top-level "name".

FROM rust:1.88-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --package mlua-swarm-cli --bin mse

FROM debian:bookworm-slim
LABEL io.modelcontextprotocol.server.name="io.github.ynishi/mlua-swarm"
LABEL org.opencontainers.image.source="https://github.com/ynishi/mlua-swarm"
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
LABEL org.opencontainers.image.description="MCP server for mlua-swarm: engine that compiles flow.ir Blueprints and dispatches agent steps."
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/mse /usr/local/bin/mse
ENTRYPOINT ["/usr/local/bin/mse"]
CMD ["serve"]
