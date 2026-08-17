# syntax=docker/dockerfile:1
FROM golang:1.26-bookworm AS library-builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags='-checklinkname=0' -buildmode=c-shared -o /out/libhuginn_messenger.so .

FROM scratch AS library
COPY --from=library-builder /out/libhuginn_messenger.so /libhuginn_messenger.so
COPY --from=library-builder /out/libhuginn_messenger.h /libhuginn_messenger.h

FROM golang:1.26-alpine AS builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /huginn .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /huginn /huginn

COPY /thick-conf.conf /config.conf

ENTRYPOINT ["/huginn"]
