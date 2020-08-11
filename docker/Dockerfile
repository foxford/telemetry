## -----------------------------------------------------------------------------
## Build
## -----------------------------------------------------------------------------
FROM rust:1.45.0-slim-buster as build-stage

RUN apt update && apt install -y --no-install-recommends \
  pkg-config \
  libssl-dev

WORKDIR "/build"

# Build dependencies.
COPY Cargo.* /build/
RUN mkdir /build/src && echo "fn main() {}" > /build/src/main.rs
RUN cargo build --release

# Build app.
COPY src/ /build/src/
RUN touch src/main.rs && cargo build --release

## -----------------------------------------------------------------------------
## Package
## -----------------------------------------------------------------------------
FROM debian:buster

RUN apt update && apt install -y --no-install-recommends \
  ca-certificates \
  libssl1.1

COPY --from=build-stage "/build/target/release/telemetry" "/app/telemetry"

WORKDIR "/app"
ENTRYPOINT ["/app/telemetry"]
