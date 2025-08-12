ARG BASE="localhost/tycho-rocksdb"

# syntax=docker/dockerfile:1.2
FROM $BASE AS builder
WORKDIR /build

# Install Rust using rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

COPY . .

# Set up Rust environment
ENV PATH="/root/.cargo/bin:${PATH}"

# Additional options to speedup link phase a bit
ENV RUSTFLAGS="-Clinker=clang -Clink-arg=-fuse-ld=lld -Clink-arg=-Wl,--no-rosegment ${RUSTFLAGS}"

# Use cache mounts for cargo registry and git to speed up builds
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --example network-node && \
    cargo build --release --bin tycho --features=debug

FROM fedora:42
RUN mkdir /app
RUN dnf update -y && dnf install -y iproute iputils && dnf clean all

COPY --from=builder /build/target/release/examples/network-node /app/network-node
COPY --from=builder /build/target/release/tycho /app/tycho
