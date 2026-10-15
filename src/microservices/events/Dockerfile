FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy sources first so go mod tidy can resolve everything
COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -o events-service .

FROM alpine:3.18
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/events-service .
EXPOSE 8082
CMD ["./events-service"]
