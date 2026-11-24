ARG BASE_IMAGE=ubuntu:22.04
FROM ${BASE_IMAGE} AS build
ARG REFRESH_PACKAGE_INDEX=1
RUN if [ "$REFRESH_PACKAGE_INDEX" = 1 ]; then apt-get update; fi \
    && apt-get install -y --no-install-recommends \
       build-essential ca-certificates curl libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Makefile LICENSE ./
COPY src/ src/
COPY test/ test/
RUN make -j4 EXTRA_CFLAGS=-Werror && make test-config && make -j4 test

FROM ${BASE_IMAGE}
ARG REFRESH_PACKAGE_INDEX=1
RUN if [ "$REFRESH_PACKAGE_INDEX" = 1 ]; then apt-get update; fi \
    && apt-get install -y --no-install-recommends libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/valkey-proxy /usr/local/bin/valkey-proxy
COPY --from=build /src/run-test /usr/local/bin/run-test
USER 60001:60001
ENTRYPOINT ["/usr/local/bin/valkey-proxy"]
