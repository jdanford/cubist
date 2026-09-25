FROM rust:1-bookworm AS cubist
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --bin cubist

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}" \
    VIRTUAL_ENV="/opt/venv"
RUN uv pip install "moto[server]" boto3

COPY --from=cubist /app/target/debug/cubist /usr/local/bin/cubist
COPY test/roundtrip.py /usr/local/bin/cubist-roundtrip
RUN chmod +x /usr/local/bin/cubist-roundtrip

CMD ["cubist-roundtrip"]
