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

# Buy one (run in a loop to see locking in action)
curl -X POST "http://localhost:3000/buy/sneaker-001?userId=alice"

# Check remaining stock
curl http://localhost:3000/inventory/sneaker-001
```

RabbitMQ management UI: http://localhost:15672 (guest / guest)

---

## Deploy to Render — fully on the free tier

The included `render.yaml` is wired to deploy at **$0/month** by swapping the
private services for free managed equivalents:

| Component | Free option used |
|---|---|
| Redis | **Render Key Value** (free 25 MB tier) — auto-wired via blueprint |
| RabbitMQ | **CloudAMQP** "Little Lemur" plan (free, 1M msgs/month) — provide URL |
| API | Render free `web` service (Docker) |
| Worker | Render free `web` service (Docker) — exposes `/health` so it qualifies |

### One-time setup

1. **Sign up for CloudAMQP**: https://www.cloudamqp.com — create a free "Little Lemur" instance, copy the **AMQP URL** (looks like `amqps://user:pass@xxx.cloudamqp.com/vhost`).
2. **Render dashboard**: New → Blueprint Instance → connect this repo.
3. When Render asks for the value of `RABBITMQ_URL` (marked `sync: false` in the blueprint), paste the CloudAMQP URL into both the API and worker services.
4. Render will provision the Key Value instance and wire `REDIS_URL` automatically.

### Free-tier caveats

- Free web services **spin down after 15 min of inactivity** — first request after sleep takes ~30s.
- Free Key Value tops out at **25 MB** — fine for locks + counters, not for full caches.
- CloudAMQP "Little Lemur" allows **20 concurrent connections / 1M msgs / month**.

---

## Load test with k6

```js
import http from "k6/http";
import { check } from "k6";

export const options = { vus: 50, iterations: 200 };

export default function () {
  const res = http.post("http://localhost:3000/buy/sneaker-001?userId=user-" + __VU);
  check(res, {
    "bought, sold-out, or locked": (r) => [200, 409, 429].includes(r.status),
  });
}
```

Watch the worker logs — orders fill in even after inventory hits 0, proving the lock held.
