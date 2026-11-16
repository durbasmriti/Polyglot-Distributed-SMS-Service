
# Polyglot Distributed SMS Service

A distributed, event-driven SMS processing system built using **Java Spring Boot, Go, Apache Kafka, Redis, MongoDB and Docker**.

The project demonstrates a polyglot microservices architecture in which Java handles SMS requests and processing, while a Go service independently consumes SMS events, persists them in MongoDB and provides an API for retrieving SMS history.

## Architecture

```text
                    Client
                      |
                      v
             Java Spring Boot Service
                  Port 8080
                      |
               Redis Blocklist
                      |
              Mock SMS Processing
                      |
                      v
                Apache Kafka
                 sms-events
                      |
                      v
               Go Storage Service
                  Port 8081
                 /          \
                v            v
            MongoDB       REST API
          SMS History    /history/{userId}
```

## Services

| Service | Technology | Responsibility |
|---|---|---|
| SMS Sender | Java, Spring Boot | Accepts SMS requests, checks the Redis blocklist and processes SMS requests |
| Message Broker | Apache Kafka | Transfers SMS events asynchronously between services |
| SMS Storage | Go | Consumes Kafka events and stores them in MongoDB |
| Blocklist | Redis | Stores blocked user identifiers |
| Database | MongoDB | Persists SMS events and provides SMS history |

## Technology Stack

- **Backend:** Java 17, Spring Boot 3.5
- **Backend:** Go
- **Message Broker:** Apache Kafka 3.9
- **Cache / Blocklist:** Redis 7
- **Database:** MongoDB 7
- **Containerization:** Docker and Docker Compose
- **Testing:** JUnit, Mockito, Go testing package

## Project Structure

```text
Polyglot-Distributed-SMS-Service/
│
├── src/
│   ├── main/java/com/durba/sms_sender/
│   │   ├── config/
│   │   ├── controller/
│   │   ├── dto/
│   │   ├── entity/
│   │   └── service/
│   │
│   └── test/
│
├── sms-storage/
│   ├── config/
│   ├── consumer/
│   ├── handler/
│   ├── model/
│   ├── repository/
│   ├── main.go
│   ├── go.mod
│   └── Dockerfile
│
├── Dockerfile
├── docker-compose.yml
├── pom.xml
└── README.md
```

## How It Works

1. A client sends an SMS request to the Java REST API.
2. The Java service checks Redis to determine whether the user is blocked.
3. If the user is blocked, the service returns HTTP 403 Forbidden.
4. Otherwise, the service simulates SMS vendor processing and generates a status.
5. The resulting SMS event is published to the Kafka topic `sms-events`.
6. The Go service consumes the event from Kafka.
7. The Go service stores the event in MongoDB.
8. Clients can retrieve a user's SMS history through the Go REST API.

**Note:** SMS vendor processing is currently mocked. The project does not send real SMS messages through an external provider.

### Prerequisites

Install the following:

- Docker Desktop
- Docker Compose
- Git

For local development outside Docker, also install:

- Java 17
- Go 1.25
- Maven (optional, since Maven Wrapper is included)

### Run Using Docker Compose

Clone the repository:

```bash
git clone https://github.com/durbasmriti/Polyglot-Distributed-SMS-Service.git

cd Polyglot-Distributed-SMS-Service
```

Build and start all services:

```bash
docker compose up --build -d
```

Check running containers:

```bash
docker compose ps
```

View application logs:

```bash
docker compose logs -f sms-sender
docker compose logs -f sms-storage
```

Stop the services:

```bash
docker compose down
```

To stop the services and remove the MongoDB Docker volume as well:

```bash
docker compose down -v
```

**Warning:** The second command permanently removes the Compose-managed MongoDB data volume.

## API Documentation

### 1. Send SMS

**Endpoint:**

```http
POST /v1/sms/send
```

**Base URL:**

```text
http://localhost:8080
```

**Request body:**

```json
{
  "userId": "user_123",
  "phoneNumber": "9812879384",
  "message": "Hello from SMS Service"
}
```

**Example using PowerShell:**

```powershell
$body = @{
    userId = "user_123"
    phoneNumber = "9812879384"
    message = "Hello from SMS Service"
} | ConvertTo-Json

Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/v1/sms/send" `
    -ContentType "application/json" `
    -Body $body
```

**Possible responses:**

Successful request:

```text
SMS Processed with status: SUCCESS
```

Failed mock vendor processing:

```text
SMS Processed with status: FAIL
```

Blocked user:

```http
HTTP 403 Forbidden
```

### 2. Retrieve SMS History

**Endpoint:**

```http
GET /v1/sms/history/{userId}
```

**Base URL:**

```text
http://localhost:8081
```

**Example:**

```http
GET http://localhost:8081/v1/sms/history/user_123
```

**PowerShell:**

```powershell
Invoke-RestMethod `
    -Method Get `
    -Uri "http://localhost:8081/v1/sms/history/user_123"
```

The response contains the stored SMS events for the specified user, including:

```json
[
  {
    "userId": "user_123",
    "phoneNumber": "9812879384",
    "message": "Hello from SMS Service",
    "status": "SUCCESS",
    "createdAt": "2026-11-16T15:00:00Z"
  }
]
```

The timestamp above is an illustrative example.

## Redis Blocklist

The Java service checks Redis for a key in the following format:

```text
blocked:{userId}
```

To block a user:

```bash
docker compose exec redis redis-cli SET blocked:user_123 true
```

To verify:

```bash
docker compose exec redis redis-cli GET blocked:user_123
```

To unblock a user:

```bash
docker compose exec redis redis-cli DEL blocked:user_123
```

Blocked users receive HTTP 403 Forbidden when attempting to send SMS.

## Configuration

The services use environment variables to support configuration across local and Docker environments.

### Java Service

| Variable | Default |
|---|---|
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |

### Go Service

| Variable | Default |
|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `sms-events` |
| `KAFKA_GROUP_ID` | `sms-storage-group` |
| `HTTP_PORT` | `8081` |

Docker Compose supplies the internal service addresses automatically.

## Testing

### Java Tests

Run from the project root:

```powershell
.\mvnw.cmd test
```

### Go Tests

Run from the `sms-storage` directory:

```bash
go test -v ./...
```

### End-to-End Verification

The complete application can be verified by:

1. Sending an SMS request through the Java API.
2. Checking the Go service logs for successful Kafka event consumption.
3. Retrieving the SMS history through the Go API.
4. Adding a user to the Redis blocklist and verifying the HTTP 403 response.

## Docker Ports

| Component | Host Port |
|---|---:|
| Java SMS Sender | 8080 |
| Go SMS Storage | 8081 |
| Kafka | 9092 |
| Redis | 6379 |
| MongoDB | 27017 |



