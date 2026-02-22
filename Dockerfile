# syntax=docker/dockerfile:1.7

FROM rust:1.92.0-bookworm AS builder
WORKDIR /app

ARG GITHUB_REF
ARG GITHUB_REF_NAME
ARG GITHUB_REF_TYPE
ENV GITHUB_REF=${GITHUB_REF}
ENV GITHUB_REF_NAME=${GITHUB_REF_NAME}
ENV GITHUB_REF_TYPE=${GITHUB_REF_TYPE}

COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src

RUN cargo build --locked --release

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tunmux-speedtest /usr/local/bin/tunmux-speedtest

ENTRYPOINT ["tunmux-speedtest"]
