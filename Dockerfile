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

COPY --from=builder /thick-conf.conf /thick-conf.conf

ENTRYPOINT ["/huginn"]
