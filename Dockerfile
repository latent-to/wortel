# wortel RPC facade sidecar (see docs/rpc-facade.md).
#
#   docker build -t wortel-serve .
#   docker run -v /path/to/chains/<chain-id>/db/full:/db:ro -p 9955:9955 \
#     wortel-serve --db /db --upstream ws://node:9944 --listen 0.0.0.0:9955
#
# The wheel is built from source in the builder stage — the image is
# self-contained; no PyPI release involved.

FROM rust:1.88-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir --break-system-packages maturin
WORKDIR /src
COPY Cargo.toml Cargo.lock pyproject.toml README.md ./
COPY src ./src
COPY python ./python
# polkadot-sdk deps make this the slow step (~tens of minutes uncached).
# Jobs capped so a build on the node host leaves CPU for the node.
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
RUN maturin build --release --out /wheels

FROM python:3.12-slim-bookworm
COPY --from=builder /wheels/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl websockets && rm -rf /tmp/wheels
EXPOSE 9955
ENTRYPOINT ["python", "-m", "wortel.serve"]
CMD ["--help"]
