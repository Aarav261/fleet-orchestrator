// Booking & Dispatch service. Two decoupled halves:
//   POST /requests  ->  validate, queue to SQS, return 202 (fast; absorbs demand bursts)
//   worker loop     ->  poll SQS, claim nearest available vehicle, write trip + audit, dispatch
//
// Scales with RIDER DEMAND: autoscale on SQS queue depth (/metrics). Replicas are competing
// consumers on one queue, so adding replicas adds throughput.
//
// All config comes from the root .env (no in-code defaults); CLI overrides still win.
import "dotenv/config";
import express from "express";
import { MongoClient } from "mongodb";
import mqtt from "mqtt";
import {
  SQSClient, CreateQueueCommand, SendMessageCommand,
  ReceiveMessageCommand, DeleteMessageCommand, GetQueueAttributesCommand,
} from "@aws-sdk/client-sqs";
import fs from "node:fs";
import path from "node:path";

// IoT Core client id = fixed service identity (must match the cert file name), so it's a constant.
const CLIENT_ID = "svc-dispatch";
// SQS_ENDPOINT is optional: set it for local ElasticMQ, leave it unset for real AWS SQS.
const {
  IOT_ENDPOINT, IOT_CERT_DIR, MONGO_URL, SQS_ENDPOINT, SQS_QUEUE_NAME,
  AWS_REGION, DISPATCH_PORT, DISPATCH_WORKERS,
} = process.env;
for (const [k, v] of Object.entries({
  IOT_ENDPOINT, IOT_CERT_DIR, MONGO_URL, SQS_QUEUE_NAME,
  AWS_REGION, DISPATCH_PORT, DISPATCH_WORKERS,
})) {
  if (!v) { console.error(`[dispatch] ${k} is required — set it in .env`); process.exit(1); }
}
const PORT = parseInt(DISPATCH_PORT, 10);
const WORKERS = parseInt(DISPATCH_WORKERS, 10); // concurrent poll loops per replica
const INSTANCE = process.env.HOSTNAME || "dispatch-local";

const mongo = new MongoClient(MONGO_URL);
await mongo.connect();
const db = mongo.db();
const vehicles = db.collection("vehicles");
const trips = db.collection("trips");
const audit = db.collection("dispatch_audit");
await vehicles.createIndex({ location: "2dsphere" });

// Credentials come from the SDK default chain: env vars (ElasticMQ dummy keys),
// ~/.aws/credentials with session token (Learner Lab host), or the LabRole task role (ECS).
const sqs = new SQSClient({ endpoint: SQS_ENDPOINT || undefined, region: AWS_REGION });
// idempotent: ensure the queue exists, then resolve its URL
const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: SQS_QUEUE_NAME }));
const QUEUE_URL = QueueUrl;
console.log(`[dispatch ${INSTANCE}] queue ${QUEUE_URL}`);

const mqttClient = mqtt.connect(`mqtts://${IOT_ENDPOINT}:8883`, {
  clientId: CLIENT_ID,
  cert: fs.readFileSync(path.join(IOT_CERT_DIR, `${CLIENT_ID}.cert.pem`)),
  key: fs.readFileSync(path.join(IOT_CERT_DIR, `${CLIENT_ID}.private.key`)),
  ca: fs.readFileSync(path.join(IOT_CERT_DIR, "AmazonRootCA1.pem")),
  reconnectPeriod: 2000,
});
mqttClient.on("connect", () => console.log(`[dispatch ${INSTANCE}] mqtt connected via IoT Core`));
mqttClient.on("error", (e) => console.error("[dispatch] mqtt error:", e.message));

// --- metrics ---
let enqueued = 0, matched = 0, unmatched = 0, errors = 0, queueDepth = 0;
const matchLatencies = [];
setInterval(async () => {
  try {
    const a = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: QUEUE_URL, AttributeNames: ["ApproximateNumberOfMessages"] }));
    queueDepth = parseInt(a.Attributes?.ApproximateNumberOfMessages || "0", 10);
  } catch { /* ignore */ }
}, 1000);

// --- ingress: accept + enqueue (fast) ---
const app = express();
app.use(express.json());
app.post("/requests", async (req, res) => {
  const { userId, origin, destination } = req.body || {};
  if (!userId || !origin?.coordinates || !destination?.coordinates) {
    return res.status(400).json({ error: "userId, origin.coordinates, destination.coordinates required" });
  }
  const trip = { userId, origin, destination, status: "queued", requested_at: new Date() };
  try {
    const { insertedId } = await trips.insertOne(trip);
    await sqs.send(new SendMessageCommand({ QueueUrl: QUEUE_URL, MessageBody: JSON.stringify({ tripId: insertedId.toString(), origin }) }));
    enqueued++;
    res.status(202).json({ tripId: insertedId.toString(), status: "queued" });
  } catch (e) {
    errors++; res.status(503).json({ error: "enqueue failed", detail: e.message });
  }
});
app.get("/healthz", (_req, res) => res.json({ ok: true, instance: INSTANCE }));
app.get("/metrics", (_req, res) => {
  matchLatencies.sort((a, b) => a - b);
  const p = (q) => matchLatencies.length ? matchLatencies[Math.floor(q * matchLatencies.length)] : 0;
  res.json({ instance: INSTANCE, enqueued, matched, unmatched, errors, queueDepth, matchP50ms: p(0.5), matchP95ms: p(0.95) });
});
app.listen(PORT, () => console.log(`[dispatch ${INSTANCE}] http on :${PORT} workers=${WORKERS}`));

// --- worker: long-poll SQS and match ---
// Pick the nearest bookable vehicle and claim it. The 5 nearest ($near, available, SOC>15) are
// candidates; the status-guarded update is the claim — only one worker can flip a given vehicle
// available->busy, so concurrent workers never double-assign. Fall to the next candidate on a lost race.
async function claimNearest(origin) {
  const candidates = await vehicles.find({
    status: "available", soc: { $gt: 15 },
    location: { $near: { $geometry: origin } },
  }).limit(5).project({ vehicleId: 1, location: 1 }).toArray();

  for (const c of candidates) {
    const claim = await vehicles.updateOne({ vehicleId: c.vehicleId, status: "available" }, { $set: { status: "busy" } });
    if (claim.modifiedCount === 1) return c;
  }
  return null;
}

async function handle(msg) {
  const t0 = Date.now();
  const { tripId, origin } = JSON.parse(msg.Body);
  const { ObjectId } = await import("mongodb");
  const _id = new ObjectId(tripId);
  const chosen = await claimNearest(origin);
  if (!chosen) {
    unmatched++;  // no bookable vehicle; mark unmatched (demo: no retry)
    await trips.updateOne({ _id }, { $set: { status: "unmatched" } });
    return;
  }
  const distanceM = haversine(origin.coordinates, chosen.location.coordinates);
  await trips.updateOne({ _id }, { $set: { status: "matched", vehicleId: chosen.vehicleId, matched_at: new Date() } });
  await audit.insertOne({
    tripId, vehicleId: chosen.vehicleId, candidates_evaluated: 5,
    distance_m: Math.round(distanceM), decided_at: new Date(), decided_by: INSTANCE,
  });
  mqttClient.publish(`fleet/dispatch/${chosen.vehicleId}`, JSON.stringify({ command: "accept_trip", tripId, destination: origin }));
  matched++;
  // After a simulated ride, complete the trip and return the vehicle to the pool (busy -> available).
  const tripMs = 15_000 + Math.random() * 30_000;
  setTimeout(() => {
    trips.updateOne({ _id }, { $set: { status: "completed", completed_at: new Date() } }).catch(() => errors++);
    vehicles.updateOne({ vehicleId: chosen.vehicleId, status: "busy" }, { $set: { status: "available" } }).catch(() => errors++);
  }, tripMs);
  matchLatencies.push(Date.now() - t0);
  if (matchLatencies.length > 5000) matchLatencies.splice(0, 2500);
}

async function pollLoop(id) {
  for (;;) {
    try {
      const r = await sqs.send(new ReceiveMessageCommand({ QueueUrl: QUEUE_URL, MaxNumberOfMessages: 10, WaitTimeSeconds: 2 }));
      for (const m of r.Messages || []) {
        try { await handle(m); await sqs.send(new DeleteMessageCommand({ QueueUrl: QUEUE_URL, ReceiptHandle: m.ReceiptHandle })); }
        catch (e) { errors++; console.error(`[dispatch w${id}] handle:`, e.message); }
      }
    } catch (e) { errors++; await sleep(500); }
  }
}
function haversine([lon1, lat1], [lon2, lat2]) {
  const dLat = ((lat2 - lat1) * Math.PI) / 180, dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return 2 * 6_371_000 * Math.asin(Math.sqrt(a));
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
for (let i = 0; i < WORKERS; i++) pollLoop(i);
