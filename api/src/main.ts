import Fastify from "fastify";
import Redis from "ioredis";
import amqplib from "amqplib";
import { randomUUID } from "crypto";

const app = Fastify({ logger: true });

// Render Key Value gives a connection URL via REDIS_URL.
// Locally docker-compose sets it to redis://redis:6379.
const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");

// CloudAMQP gives a full amqps:// URL via RABBITMQ_URL.
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const LOCK_TTL_MS = 10_000;
const QUEUE_NAME = "orders";

// Lua: only delete the lock key if we still own it (atomic)
const RELEASE_LOCK_LUA = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`;

async function acquireLock(itemId: string, lockId: string): Promise<boolean> {
  const result = await redis.set(
    `lock:${itemId}`,
    lockId,
    "PX",
    LOCK_TTL_MS,
    "NX"
  );
  return result === "OK";
}

async function releaseLock(itemId: string, lockId: string): Promise<void> {
  await redis.eval(RELEASE_LOCK_LUA, 1, `lock:${itemId}`, lockId);
}

// Pooled AMQP channel — opened once at startup and reused across all requests.
// Without this, each /buy request paid a TLS+AMQP handshake to CloudAMQP
// (~200 ms RTT), which kept the Redis lock held long enough that concurrent
// requests almost always lost the race and got 429.
let channelPromise: Promise<amqplib.Channel> | null = null;

async function getChannel(): Promise<amqplib.Channel> {
  if (channelPromise) return channelPromise;
  channelPromise = (async () => {
    const conn = await amqplib.connect(RABBITMQ_URL);
    conn.on("error", (err) => {
      app.log.error({ err }, "[amqp] connection error");
      channelPromise = null;
    });
    conn.on("close", () => {
      app.log.warn("[amqp] connection closed");
      channelPromise = null;
    });
    const channel = await conn.createChannel();
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    app.log.info("[amqp] channel ready");
    return channel;
  })();
  // If the connect fails, clear the cached promise so the next request retries.
  channelPromise.catch(() => { channelPromise = null; });
  return channelPromise;
}

async function publishOrder(order: object): Promise<void> {
  const channel = await getChannel();
  channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(order)), {
    persistent: true,
  });
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

app.get("/health", async () => ({ status: "ok" }));

app.post<{ Params: { itemId: string }; Querystring: { quantity?: string } }>(
  "/restock/:itemId",
  async (req, reply) => {
    const quantity = Number(req.query.quantity ?? 100);
    await redis.set(`inventory:${req.params.itemId}`, quantity);
    return reply.send({ itemId: req.params.itemId, inventory: quantity });
  }
);

app.get<{ Params: { itemId: string } }>(
  "/inventory/:itemId",
  async (req, reply) => {
    const raw = await redis.get(`inventory:${req.params.itemId}`);
    if (raw === null)
      return reply.code(404).send({ error: "Item not found" });
    return reply.send({ itemId: req.params.itemId, inventory: Number(raw) });
  }
);

app.post<{
  Params: { itemId: string };
  Querystring: { userId?: string };
}>("/buy/:itemId", async (req, reply) => {
  const { itemId } = req.params;
  const userId = req.query.userId ?? "anonymous";
  const lockId = randomUUID();

  const locked = await acquireLock(itemId, lockId);
  if (!locked) {
    return reply.code(429).send({
      error: "Item is being processed by another request, please retry.",
    });
  }

  try {
    const raw = await redis.get(`inventory:${itemId}`);
    if (raw === null)
      return reply.code(404).send({ error: "Item not found" });

    const inventory = Number(raw);
    if (inventory <= 0)
      return reply.code(409).send({ error: "Sold out" });

    await redis.decr(`inventory:${itemId}`);

    const order = {
      orderId: randomUUID(),
      itemId,
      userId,
      timestamp: Date.now(),
    };

    await publishOrder(order);

    return reply.send({
      success: true,
      orderId: order.orderId,
      remainingInventory: inventory - 1,
    });
  } finally {
    await releaseLock(itemId, lockId);
  }
});

const port = Number(process.env.PORT ?? 3000);
app.listen({ port, host: "0.0.0.0" }, (err) => {
  if (err) {
    app.log.error(err);
    process.exit(1);
  }
  // Pre-warm the AMQP channel so the first /buy request doesn't pay
  // the connection handshake cost. Failure here is non-fatal — the next
  // /buy will retry via getChannel().
  getChannel().catch((e) => app.log.error({ err: e }, "[amqp] warm-up failed"));
});
