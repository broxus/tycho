# syntax=docker/dockerfile:1.2
FROM rust:1.85.1-bookworm as builder
WORKDIR /build
COPY . .

# Use cache mounts for cargo registry and git to speed up builds
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --example network-node

FROM debian:bookworm-slim
RUN mkdir /app
RUN apt update && apt install iproute2 iputils-ping -y && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/examples/network-node /app/network-node