FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/target/release/solve /solve
ENTRYPOINT ["/solve"]
