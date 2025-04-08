# syntax=docker/dockerfile:1.2
FROM fedora:43 as builder
WORKDIR /build

# Install dependencies
ENV ROCKSDB_LIB_DIR /usr/lib
RUN dnf update -y && \
    dnf install -y awk clang curl rocksdb-devel rocksdb && \
    dnf clean all

# Install Rust using rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Set up Rust environment
ENV PATH="/root/.cargo/bin:${PATH}"

COPY . .

# Use cache mounts for cargo registry and git to speed up builds
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --example network-node && \
    cargo build --release --bin tycho --features=debug

FROM fedora:43
RUN mkdir /app
RUN dnf update -y && dnf install -y iproute iputils rocksdb && dnf clean all
COPY --from=builder /build/target/release/examples/network-node /app/network-node
COPY --from=builder /build/target/release/tycho /app/tycho