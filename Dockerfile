FROM rust:1.90 as builder

WORKDIR /app

COPY Cargo.toml .
COPY Cargo.lock .
COPY src src

RUN cargo build --release

FROM ubuntu:24.04

COPY --from=builder /app/target/release/data-checker /usr/local/bin/data-checker
CMD ["data-checker"]