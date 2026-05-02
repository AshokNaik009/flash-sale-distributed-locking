import amqplib, { ConsumeMessage } from "amqplib";

const RABBITMQ_URL =
  `amqp://${process.env.RABBITMQ_USER ?? "guest"}:` +
  `${process.env.RABBITMQ_PASS ?? "guest"}@` +
  `${process.env.RABBITMQ_HOST ?? "localhost"}:` +
  `${process.env.RABBITMQ_PORT ?? 5672}`;

const QUEUE_NAME = "orders";
const RETRY_DELAY_MS = 5_000;

interface Order {
  orderId: string;
  itemId: string;
  userId: string;
  timestamp: number;
}

async function processOrder(order: Order): Promise<void> {
  console.log(
    `[worker] Processing order ${order.orderId} — item: ${order.itemId}, user: ${order.userId}`
  );
  // Simulate async work (DB write, email, etc.)
  await new Promise((r) => setTimeout(r, 100));
  console.log(`[worker] Order ${order.orderId} complete`);
}

async function startWorker(): Promise<void> {
  while (true) {
    try {
      const conn = await amqplib.connect(RABBITMQ_URL);
      const channel = await conn.createChannel();

      await channel.assertQueue(QUEUE_NAME, { durable: true });
      channel.prefetch(1); // process one message at a time

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
      console.error(
        `[worker] Connection error — retrying in ${RETRY_DELAY_MS}ms:`,
        err
      );
      await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
    }
  }
}

startWorker();
