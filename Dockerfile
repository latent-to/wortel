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
      pkg-config

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY serve ./serve

# polkadot-sdk deps make this the slow step (~tens of minutes uncached).
# Jobs capped so a build on the node host leaves CPU for the node.
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release -p wortel-serve \
    && cp target/release/wortel-serve /wortel-serve

FROM gcr.io/distroless/cc-debian12
COPY --from=build /wortel-serve /wortel-serve
USER nonroot
ENV WORTEL_DB=/data/subtensor/chains/bittensor/db/full
ENTRYPOINT ["/wortel-serve"]
