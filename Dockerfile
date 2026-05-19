FROM golang:1.24-bookworm AS builder

WORKDIR /src

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        libstdc++6 \
        libzmq3-dev \
        pkg-config && \
    rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ENV CGO_ENABLED=1 \
    GOOS=linux \
    GOARCH=amd64

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/manindexer .

FROM builder AS test

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go test ./basicprotocols/metaso ./man ./database/mongodb ./adapter/microvisionchain ./adapter/opcat ./common ./pebblestore -count=1

FROM scratch AS artifact

COPY --from=builder /out/manindexer /manindexer-linux-amd64

FROM debian:bookworm-slim AS runtime

WORKDIR /man

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libstdc++6 \
        libzmq5 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/manindexer /man/manindexer
COPY config_example.toml /man/config.toml
COPY jieba_dict /man/jieba_dict

RUN chmod +x /man/manindexer

CMD ["/man/manindexer", "-config=/man/config.toml", "-test=0", "-chain=btc,mvc,opcat"]
