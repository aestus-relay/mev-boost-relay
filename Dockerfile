# syntax=docker/dockerfile:1
FROM golang:1.18 as builder
ARG VERSION
WORKDIR /build
ADD . /build/

# Set up blst environment variables
ENV CGO_CFLAGS="-O -D__BLST_PORTABLE__"
ENV CGO_CFLAGS_ALLOW="-O -D__BLST_PORTABLE__"

# Build
RUN --mount=type=cache,target=/root/.cache/go-build GOOS=linux go build -trimpath -ldflags "-s -X cmd.Version=$VERSION -X main.Version=$VERSION" -v -o mev-boost-relay .

# Copy to executable container
FROM alpine
RUN apk add --no-cache libstdc++ libc6-compat
WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/mev-boost-relay /app/mev-boost-relay
ENTRYPOINT ["/app/mev-boost-relay"]
