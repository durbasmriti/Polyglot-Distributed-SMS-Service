
# Build stage
FROM golang:1.25-alpine AS build

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /sms-storage .

# Runtime stage
FROM alpine:3.22

RUN apk --no-cache add ca-certificates

COPY --from=build /sms-storage /usr/local/bin/sms-storage

EXPOSE 8081

ENTRYPOINT ["sms-storage"]
