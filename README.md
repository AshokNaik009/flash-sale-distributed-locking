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
 │   RabbitMQ   │  "orders" queue   (CloudAMQP free tier)
 └──────┬───────┘
        │ consume
        ▼
 ┌──────────────┐
 │    Worker    │  (simulate DB write / email)
 └──────────────┘
```

### Why Redis for locking?

`SET key value PX <ttl> NX` is atomic — only one caller wins the race. The lock is released via a Lua script that checks ownership before deleting, preventing a slow request from releasing another request's lock.

### Why CloudAMQP instead of self-hosting RabbitMQ?

Render private services (`pserv`) require a paid plan. CloudAMQP's "Little Lemur" plan gives a free managed AMQP broker (1 M msgs/month, 20 connections), reachable from anywhere via TLS.

---

## Run locally

```bash
# 1. Copy env template and fill in your CloudAMQP URL
cp .env.example .env
# edit .env → set RABBITMQ_URL

# 2. Boot Redis + API + Worker (RabbitMQ is remote)
docker-compose up --build

# 3. Try it out
curl -X POST "http://localhost:3000/restock/sneaker-001?quantity=10"
curl -X POST "http://localhost:3000/buy/sneaker-001?userId=alice"
curl http://localhost:3000/inventory/sneaker-001
```

Worker logs (`docker-compose logs -f worker`) will show orders being consumed from CloudAMQP.

---

## Deploy to Render — fully on the free tier (no credit card)

> **Why not Blueprint?** Render's Blueprint flow forces a card on file even when every service in it is free. We avoid that by creating each service manually through the dashboard with the **Node native runtime** (Docker runtime also requires a card; Node native does not).

### 1. Create the Redis (Render Key Value)

- Dashboard → **New → Key Value**
- Plan: **Free** (25 MB)
- Region: pick the one closest to where your other services will live
- Copy the **Internal Connection String** (looks like `redis://red-xxxxx:6379`) — you'll need it in step 2

### 2. Create the API web service

- Dashboard → **New → Web Service** → connect this GitHub repo
- Settings:
  - **Name:** `flash-sale-api`
  - **Region:** same as Key Value
  - **Branch:** `main`
  - **Root Directory:** `api`
  - **Runtime:** `Node`
  - **Build Command:** `npm install && npm run build`
  - **Start Command:** `npm start`
  - **Plan:** Free
- **Environment variables:**
  - `REDIS_URL` → paste the Key Value internal connection string
  - `RABBITMQ_URL` → your CloudAMQP `amqps://...` URL

### 3. Create the worker as a second web service

> Background workers (`type: worker`) require a paid plan, so we deploy the consumer as a `web` service. It exposes a tiny `/health` endpoint to satisfy Render's health checks while the AMQP consumer runs in the background.

- Dashboard → **New → Web Service** → same repo
- Settings:
  - **Name:** `order-processor`
  - **Root Directory:** `worker`
  - **Runtime:** `Node`
  - **Build Command:** `npm install && npm run build`
  - **Start Command:** `npm start`
  - **Plan:** Free
- **Environment variables:**
  - `RABBITMQ_URL` → same CloudAMQP URL

### Free-tier caveats

- Free web services **spin down after ~15 min of inactivity** — first request after sleep takes ~30 s.
- Free Key Value caps at **25 MB** — fine for locks + counters.
- CloudAMQP "Little Lemur" gives **20 concurrent connections / 1 M msgs/month**.

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

---

## Files

```
.
├── api/                  # Fastify API — Redis lock + AMQP publisher
│   ├── src/main.ts
│   ├── package.json
│   ├── tsconfig.json
│   └── Dockerfile        # used by docker-compose only
├── worker/               # Order processor — AMQP consumer + /health
│   ├── src/worker.ts
│   ├── package.json
│   ├── tsconfig.json
│   └── Dockerfile        # used by docker-compose only
├── docker-compose.yml    # local dev: Redis + API + Worker
├── render.yaml           # reference Blueprint (requires paid plan to deploy)
├── .env.example          # template for local env
└── .gitignore            # blocks .env, node_modules, dist
```
