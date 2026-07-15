# API Gateway with Authentication, Rate Limiting, and Circuit Breaking

This is a work-in-progress production-style API gateway built with Python 3.12 and FastAPI made for the sole purpose of learning how API gateways work.

## Current phase

This repository currently contains only the project structure and runnable placeholder services:

- `gateway/` contains the FastAPI gateway package split into `auth`, `rate_limiter`, `circuit_breaker`, `proxy`, `service_registry`, `metrics`, and `api`.
- `services/dummy_backend/` contains a minimal FastAPI backend used three times in Docker Compose.
- `docker-compose.yml` wires together the gateway, Redis, Postgres, and the three dummy services.

## What works now

- All containers start.
- The gateway exposes placeholder HTTP endpoints.
- Each backend service echoes its own name after a fake delay.

## Next phase

- JWT verification in the gateway;
- Redis-backed token bucket rate limiting;
- Redis-backed circuit breaker state;
- Real proxy forwarding and service registry reads from Postgres;
- Unit and integration tests;
