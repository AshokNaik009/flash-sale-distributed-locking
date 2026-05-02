import amqplib, { ConsumeMessage } from "amqplib";
import http from "http";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
const QUEUE_NAME = "orders";
const RETRY_DELAY_MS = 5_000;

interface Order {
  orderId: string;
  itemId: string;
  userId: string;
  timestamp: number;
}

let isReady = false;

async function processOrder(order: Order): Promise<void> {
  console.log(
    `[worker] Processing order ${order.orderId} — item: ${order.itemId}, user: ${order.userId}`
  );
  // Simulate async work (DB write, email, etc.)
  await new Promise((r) => setTimeout(r, 100));
  console.log(`[worker] Order ${order.orderId} complete`);
}

async function startConsumer(): Promise<void> {
  while (true) {
    try {
      const conn = await amqplib.connect(RABBITMQ_URL);
      const channel = await conn.createChannel();

      await channel.assertQueue(QUEUE_NAME, { durable: true });
      channel.prefetch(1);

      isReady = true;
      console.log("[worker] Ready — waiting for orders...");

      channel.consume(QUEUE_NAME, async (msg: ConsumeMessage | null) => {
        if (!msg) return;
        try {
          const order: Order = JSON.parse(msg.content.toString());
          await processOrder(order);
          channel.ack(msg);
        } catch (err) {
          console.error("[worker] Failed to process message, requeueing:", err);
          channel.nack(msg, false, true);
        }
      });

      // Keep alive until connection drops
      await new Promise<void>((_, reject) => {
        conn.on("error", reject);
        conn.on("close", () => reject(new Error("Connection closed")));
      });
    } catch (err) {
      isReady = false;
      console.error(
        `[worker] Connection error — retrying in ${RETRY_DELAY_MS}ms:`,
        err
      );
      await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
    }
  }
}

// Render's free tier requires "web" services to bind to a port. We expose a
// minimal /health endpoint so Render's health checks pass while the consumer
// loop runs in the background.
const port = Number(process.env.PORT ?? 3000);
http
  .createServer((_req, res) => {
    res.writeHead(isReady ? 200 : 503, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: isReady ? "ok" : "starting", role: "worker" }));
  })
  .listen(port, () => {
    console.log(`[worker] Health server on :${port}`);
  });

startConsumer();
