FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o alterguard .

# Final stage
FROM alpine:latest

# Install pt-online-schema-change and dependencies
RUN apk add --no-cache \
    perl \
    perl-dbi \
    perl-dbd-mysql \
    perl-term-readkey \
    mysql-client \
    ca-certificates \
    && wget -O /usr/local/bin/pt-online-schema-change \
    https://raw.githubusercontent.com/percona/percona-toolkit/3.x/bin/pt-online-schema-change \
    && chmod +x /usr/local/bin/pt-online-schema-change

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/alterguard .

# Copy example configs
COPY --from=builder /app/examples ./examples

# Create non-root user
RUN adduser -D -s /bin/sh alterguard
USER alterguard

ENTRYPOINT ["./alterguard"]
