# Flash Sale Engine — Distributed Locking POC

A minimal but production-shaped demo of how to prevent **inventory overselling** under high concurrency using Redis distributed locks and RabbitMQ for async order processing.

**Live deployment** (Render free tier):
- API → https://flash-sale-api-y407.onrender.com
- Worker → https://order-processor-mxcq.onrender.com

> Free-tier services spin down after ~15 min of inactivity — first request wakes them and takes ~30 s.

## Architecture

```
Browser / k6 load-test
        │
        ▼
 ┌─────────────┐   Redis SET NX   ┌─────────────────────┐
 │  Flash Sale │ ◄──────────────► │  Render Key Value   │
 │     API     │                  │ (lock + inventory)  │
 └──────┬──────┘                  └─────────────────────┘
        │ publish order
        ▼
 ┌──────────────┐
 │   CloudAMQP  │  "orders" queue   (free Little Lemur tier)
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

### Why the worker is a `web` service, not a `background_worker`

Render's `background_worker` type is paid only. To stay on the free tier, the consumer is deployed as a `web` service that runs the AMQP consumer loop **and** exposes a tiny `/health` endpoint to satisfy Render's health checks.

---

## How a `/buy` actually works

### State in Redis (two keys per item)

```
lock:sneaker-001        = <random UUID>   ← the turnstile, single owner
inventory:sneaker-001   = 9               ← the counter
```

The **lock is one slot per item** (not per request). The UUID is the lock's *value*, used to prove ownership at release time.

### The critical section (what runs while the lock is held)

```
1. SET lock:item <uuid> PX 10000 NX   ← atomic — only one winner
   ├─ nil → return 429 (someone else holds it)
   └─ OK  → continue
2. GET inventory:item                  → if 0 → return 409 (sold out)
3. DECR inventory:item                 ← atomic decrement
4. publish order to CloudAMQP queue    ← uses pooled channel, sub-ms
5. Lua DEL lock if value still matches
```

Total time: **~5 ms** (with the AMQP channel pool). Throughput per item ≈ 200 sales/sec.

### The three response codes

| Code | Meaning | What the client should do |
|---|---|---|
| `200` | You got a unit; order queued for fulfillment | Show success |
| `409` | Lock acquired but inventory was 0 | Show "sold out" |
| `429` | Lost the lock race; never reached inventory | **Retry with backoff** |

The lock is mutual exclusion, **not a queue**. Bounced (429) requests are gone unless the *client* retries — that's how a virtual queue forms outside the server.

### Why the 10-second TTL

Crash safety net — if the lock holder dies before step 5, Redis auto-expires the key after 10 s so other buyers aren't permanently blocked. Happy-path lock-held time is ~5 ms; the 10 s is the worst-case escape hatch.

### Why an async queue (sync vs async split)

Inside the lock we do only fast Redis ops + a tiny publish. The slow stuff (charge card, write DB, send email — seconds each) runs in the worker, **after** the lock is released. The queue is a **durable buffer** between fast inventory decisions and slow fulfillment. If the worker dies, messages pile up; when it restarts, it drains them — customers who got 200 are guaranteed to be fulfilled eventually.

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

> **Why not Blueprint?** Render's Blueprint flow requires a card on file even when every service in it is free. Same for Docker runtime. We use the **Node native runtime** + create services individually (via CLI or dashboard) to stay free.

### Prerequisites

1. **CloudAMQP** account → create a free **Little Lemur** instance → copy the AMQP URL (looks like `amqps://user:pass@host.lmq.cloudamqp.com/vhost`).
2. **Render CLI** installed and logged in:
   ```bash
   curl -fsSL https://github.com/render-oss/cli/releases/latest/download/cli_install.sh | sh
   render login
   render workspace set <your-workspace-id>
   ```

### 1. Create the Render Key Value (dashboard — CLI doesn't support this type)

- https://dashboard.render.com/new/redis
- **Name:** `flash-sale-redis`
- **Region:** Oregon (or wherever your other services live)
- **Plan:** Free (25 MB)
- Copy the **Internal Connection String** (e.g. `redis://red-xxxxx:6379`)

### 2. Create both web services via CLI

```bash
# API
render services create \
  --type web_service \
  --name flash-sale-api \
  --runtime node \
  --repo https://github.com/<you>/flash-sale-distributed-locking \
  --branch main \
  --root-directory api \
  --build-command "npm install && npm run build" \
  --start-command "npm start" \
  --plan free \
  --region oregon \
  --health-check-path /health \
  --env-var "REDIS_URL=redis://red-xxxxx:6379" \
  --env-var "RABBITMQ_URL=amqps://user:pass@host.lmq.cloudamqp.com/vhost" \
  --output json --confirm

# Worker
render services create \
  --type web_service \
  --name order-processor \
  --runtime node \
  --repo https://github.com/<you>/flash-sale-distributed-locking \
  --branch main \
  --root-directory worker \
  --build-command "npm install && npm run build" \
  --start-command "npm start" \
  --plan free \
  --region oregon \
  --health-check-path /health \
  --env-var "RABBITMQ_URL=amqps://user:pass@host.lmq.cloudamqp.com/vhost" \
  --output json --confirm
```

### 3. Watch the deploy

```bash
render deploys list <service-id>      # status
render logs <service-id> --tail        # live logs
```

### Free-tier caveats

- Web services **spin down after ~15 min of inactivity** — first request after sleep takes ~30 s.
- Key Value caps at **25 MB** — fine for locks + counters, not for full caches.
- CloudAMQP "Little Lemur" allows **20 concurrent connections / 1 M msgs/month**.

---

## Load test with k6

```js
import http from "k6/http";
import { check } from "k6";

export const options = { vus: 50, iterations: 200 };

export default function () {
  const res = http.post("https://flash-sale-api-y407.onrender.com/buy/sneaker-001?userId=user-" + __VU);
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
