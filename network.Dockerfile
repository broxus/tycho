FROM rust:1.76-buster as builder
COPY . .
RUN cargo build --release --example network-node

FROM debian:buster-slim
RUN mkdir /app
COPY --from=builder /target/release/examples/network-node /app/network-node