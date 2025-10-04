# syntax=docker/dockerfile:1

FROM golang:1.21 AS builder
WORKDIR /app
COPY go.mod go.sum ./
# Copy the rest of the source
COPY cmd ./cmd
COPY internal ./internal
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /worker ./cmd/worker

FROM alpine:3.19
RUN addgroup -S worker && adduser -S worker -G worker
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=builder /worker /usr/local/bin/worker
USER worker
ENTRYPOINT ["/usr/local/bin/worker"]
