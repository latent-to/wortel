FROM rust:1.88-bookworm AS build

RUN --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' \
      > /etc/apt/apt.conf.d/keep-cache \
    && apt-get update \
    && apt-get install --no-install-recommends -y \
      clang \
      libclang-dev \
      pkg-config \
      python3-dev \
      python3-pip \
    && pip3 install --break-system-packages maturin

WORKDIR /src
COPY Cargo.toml Cargo.lock pyproject.toml ./
COPY src ./src
COPY python ./python

# polkadot-sdk deps make this the slow step (~tens of minutes uncached).
# Jobs capped so a build on the node host leaves CPU for the node.
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    maturin build --release --out /wheels --interpreter /usr/bin/python3 \
    && pip3 install --target /app /wheels/*.whl 'websockets>=13'

FROM gcr.io/distroless/python3-debian12
COPY --from=build /app /app
COPY --from=gcr.io/distroless/cc-debian12 /lib /lib
COPY --from=gcr.io/distroless/cc-debian12 /usr/lib /usr/lib
USER nonroot
ENV PYTHONPATH=/app
ENV WORTEL_DB=/data/subtensor/chains/bittensor/db/full
ENTRYPOINT ["/usr/bin/python3", "-u", "-m", "wortel.serve"]
