# Flash Sale Engine — Distributed Locking POC

A minimal but production-shaped demo of how to prevent **inventory overselling** under high concurrency using Redis distributed locks and RabbitMQ for async order processing.

## Architecture

```
Browser / k6 load-test
        │
        ▼
 ┌─────────────┐   Redis SET NX   ┌───────────────────┐
 │  Flash Sale │ ◄──────────────► │  Redis (lock +    │
 │     API     │                  │   inventory store) │
 └──────┬──────┘                  └───────────────────┘
        │ publish order
        ▼
 ┌──────────────┐
 │   RabbitMQ   │  "orders" queue
 └──────┬───────┘
        │ consume
        ▼
 ┌──────────────┐
 │    Worker    │  (simulate DB write / email)
 └──────────────┘
```

### Why Redis for locking?

`SET key value NX PX <ttl>` is atomic — only one caller wins the race. The lock is released via a Lua script that checks ownership before deleting, preventing a slow request from releasing another request's lock.

### Why RabbitMQ instead of Kafka?

Kafka needs a lot of RAM and is overkill here. RabbitMQ's `durable: true` queue + `persistent` message delivery gives sufficient durability for a portfolio demo.

---

## Run locally

```bash
docker-compose up --build
```

Seed inventory, then fire concurrent buys:

```bash
# Seed 10 units of item "sneaker-001"
curl -X POST "http://localhost:3000/restock/sneaker-001?quantity=10"

# Buy one (run this in a loop to see locking in action)
curl -X POST "http://localhost:3000/buy/sneaker-001?userId=alice"

# Check remaining stock
curl http://localhost:3000/inventory/sneaker-001
```

RabbitMQ management UI: http://localhost:15672 (guest / guest)

---

## Load test with k6

```js
// k6 script — save as load-test.js, run: k6 run load-test.js
import http from "k6/http";
import { check } from "k6";

export const options = { vus: 50, iterations: 200 };

export default function () {
  const res = http.post("http://localhost:3000/buy/sneaker-001?userId=user-" + __VU);
  check(res, {
    "bought or sold-out or locked": (r) =>
      [200, 409, 429].includes(r.status),
  });
}
```

Watch the worker logs — orders fill in even after inventory hits 0, proving the lock held.

---

## Deploy to Render

This repo ships a `render.yaml` Blueprint. Click **New → Blueprint** in the Render dashboard, point it at this repo, and the entire stack deploys in one shot.

> **Production note:** Attach a Render Persistent Disk to `rabbitmq-internal` so the queue survives restarts.
