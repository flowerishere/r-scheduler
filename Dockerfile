FROM rust:1.96.1-slim-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
# Optional Cargo source config, useful on networks requiring a registry mirror.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    --mount=type=secret,id=cargo_config,target=/usr/local/cargo/config.toml \
    cargo build --locked --release \
    && cp target/release/scheduler-service /build/scheduler-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/scheduler-service /usr/local/bin/scheduler-service
USER 10001:10001
ENV SCHEDULER_BIND=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["scheduler-service"]
CMD ["serve", "--role", "all"]
