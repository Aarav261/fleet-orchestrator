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
import { randomUUID } from "node:crypto";

// All replicas share the svc-dispatch cert, but each needs its own client id: IoT Core drops an
// existing connection when another connects with the same id. FleetBackendPolicy allows svc-dispatch-*.
const CERT_NAME = "svc-dispatch";
const CLIENT_ID = `${CERT_NAME}-${randomUUID().slice(0, 8)}`;
// SQS_ENDPOINT is optional: set it for local ElasticMQ, leave it unset for real AWS SQS.
const {
  IOT_ENDPOINT, IOT_CERT_DIR, MONGO_URL, SQS_ENDPOINT, SQS_QUEUE_NAME,
  AWS_REGION, DISPATCH_PORT, DISPATCH_WORKERS,
} = process.env;
for (const [k, v] of Object.entries({
  IOT_ENDPOINT, MONGO_URL, SQS_QUEUE_NAME,
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
await trips.createIndex({ status: 1, due_at: 1 }); // trip-completion sweep

// Credentials come from the SDK default chain: env vars (ElasticMQ dummy keys),
// ~/.aws/credentials with session token (Learner Lab host), or the LabRole task role (ECS).
const sqs = new SQSClient({ endpoint: SQS_ENDPOINT || undefined, region: AWS_REGION });
// idempotent: ensure the queue exists, then resolve its URL
const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: SQS_QUEUE_NAME }));
const QUEUE_URL = QueueUrl;
console.log(`[dispatch ${INSTANCE}] queue ${QUEUE_URL}`);

// Cert/key/CA: env vars on ECS (injected from Secrets Manager), otherwise files in IOT_CERT_DIR.
const pem = (envVar, file) => process.env[envVar] || fs.readFileSync(path.join(IOT_CERT_DIR, file));
const mqttClient = mqtt.connect(`mqtts://${IOT_ENDPOINT}:8883`, {
  clientId: CLIENT_ID,
  cert: pem("IOT_CERT", `${CERT_NAME}.cert.pem`),
  key: pem("IOT_KEY", `${CERT_NAME}.private.key`),
  ca: pem("IOT_CA", "AmazonRootCA1.pem"),
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
// Only vehicles heard from in the last 60 s count: an idle vehicle heartbeats every 30 s, so two
// missed heartbeats means it is offline, even if its last known status was "available".
const ONLINE_MS = 60_000;
async function claimNearest(origin) {
  const candidates = await vehicles.find({
    status: "available", soc: { $gt: 15 }, updatedAt: { $gt: new Date(Date.now() - ONLINE_MS) },
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
  // Simulated ride length. due_at lives in the trip (not an in-memory timer) so any replica's
  // sweep can complete it, even if this replica is scaled in mid-trip.
  const due_at = new Date(Date.now() + 15_000 + Math.random() * 30_000);
  await trips.updateOne({ _id }, { $set: { status: "matched", vehicleId: chosen.vehicleId, matched_at: new Date(), due_at } });
  await audit.insertOne({
    tripId, vehicleId: chosen.vehicleId, candidates_evaluated: 5,
    distance_m: Math.round(distanceM), decided_at: new Date(), decided_by: INSTANCE,
  });
  mqttClient.publish(`fleet/dispatch/${chosen.vehicleId}`, JSON.stringify({ command: "accept_trip", tripId, destination: origin }));
  matched++;
  matchLatencies.push(Date.now() - t0);
  if (matchLatencies.length > 5000) matchLatencies.splice(0, 2500);
}

// Complete overdue trips and return their vehicles to the pool (busy -> available). Every replica
// sweeps; the status-guarded update means only one replica completes a given trip.
async function completeDueTrips() {
  const due = await trips.find({ status: "matched", due_at: { $lte: new Date() } }).project({ vehicleId: 1 }).toArray();
  for (const t of due) {
    const done = await trips.updateOne({ _id: t._id, status: "matched" }, { $set: { status: "completed", completed_at: new Date() } });
    if (done.modifiedCount === 1) await vehicles.updateOne({ vehicleId: t.vehicleId, status: "busy" }, { $set: { status: "available" } });
  }
}
setInterval(() => completeDueTrips().catch(() => errors++), 2000);

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
