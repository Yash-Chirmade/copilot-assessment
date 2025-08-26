# Kafka Event Pipeline Demo

## Stack
- Go
- Kafka
- MS SQL
- Redis (DLQ)
- Docker Compose

## Services
- Producer: Publishes 4 event types to Kafka
- Consumer: Consumes, upserts to MS SQL, pushes errors to Redis DLQ
- API: Read endpoints for users and orders

## Run
```sh
docker-compose up --build
```

## Endpoints
- `GET /users/{id}`: user + last 5 orders
- `GET /orders/{id}`: order + payment status

## Env
- KAFKA_BROKER
- SQLSERVER_CONN
- REDIS_ADDR

## SQL Schema
See `schema.sql` for table definitions.

## Sample Requests
See `sample.http` for curl examples.
