// Telemetry Ingest service. Subscribes to fleet/telemetry/#, appends each reading to the
// telemetry log and keeps live vehicle state current. Alerts are Node-RED's job (IoT rule ->
// fleet/alert-candidates -> Node-RED), so this hot path does one thing per message.
//
// Scales with FLEET SIZE: autoscale on message volume (/metrics msgsPerSec is that signal).
//
// All config comes from the root .env (no in-code defaults); CLI overrides still win.
import "dotenv/config";
import express from "express";
import mqtt from "mqtt";
import { MongoClient } from "mongodb";
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

// All replicas share the svc-ingest cert, but each needs its own client id: IoT Core drops an
// existing connection when another connects with the same id. FleetBackendPolicy allows svc-ingest-*.
const CERT_NAME = "svc-ingest";
const CLIENT_ID = `${CERT_NAME}-${randomUUID().slice(0, 8)}`;
const { IOT_ENDPOINT, IOT_CERT_DIR, MONGO_URL, INGEST_PORT } = process.env;
for (const [k, v] of Object.entries({ IOT_ENDPOINT, MONGO_URL, INGEST_PORT })) {
  if (!v) { console.error(`[ingest] ${k} is required — set it in .env`); process.exit(1); }
}
const PORT = parseInt(INGEST_PORT, 10);
const INSTANCE = process.env.HOSTNAME || "ingest-local";

const mongo = new MongoClient(MONGO_URL);
await mongo.connect();
const db = mongo.db();
const telemetry = db.collection("telemetry");
const vehicles = db.collection("vehicles");
await vehicles.createIndex({ location: "2dsphere" });
await vehicles.createIndex({ vehicleId: 1 }, { unique: true });
await telemetry.createIndex({ vehicleId: 1, ts: -1 });
console.log(`[ingest ${INSTANCE}] mongo connected, indexes ensured`);

// --- metrics ---
let processed = 0, errors = 0;
let window = 0; // messages in the current 1 s window
let msgsPerSec = 0;
setInterval(() => { msgsPerSec = window; window = 0; }, 1000);

// Autoscaling signal: on ECS this CloudWatch Embedded Metric Format line becomes the metric
// Fleet/IngestMsgsPerSec (average per task), with no SDK call. Skipped off ECS - it's only log noise there.
let emfLast = 0;
if (process.env.ECS_CONTAINER_METADATA_URI_V4) setInterval(() => {
  const rate = (processed - emfLast) / 10; emfLast = processed;
  console.log(JSON.stringify({
    _aws: { Timestamp: Date.now(), CloudWatchMetrics: [{ Namespace: "Fleet", Dimensions: [["Service"]],
      Metrics: [{ Name: "IngestMsgsPerSec", Unit: "Count/Second" }] }] },
    Service: "ingest", IngestMsgsPerSec: rate,
  }));
}, 10_000);

// Batch telemetry writes once a second, not one insert per message (survives load).
let buffer = [];
async function flush() {
  if (!buffer.length) return;
  const batch = buffer; buffer = [];
  try { await telemetry.insertMany(batch, { ordered: false }); }
  catch (e) { errors++; console.error("[ingest] bulk insert error:", e.message); }
}
setInterval(flush, 1000);

// Graceful scale-in: ECS sends SIGTERM (30 s grace) before stopping a task. Stop receiving first,
// so IoT Core routes the shared subscription to the remaining replicas, then write what is buffered.
async function shutdown(sig) {
  await new Promise((r) => client.end(false, r));
  console.log(`[ingest ${INSTANCE}] ${sig}: processed ${processed}, flushing ${buffer.length} buffered readings`);
  await flush();
  await mongo.close();
  process.exit(0);
}
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);

// Cert/key/CA: env vars on ECS (injected from Secrets Manager), otherwise files in IOT_CERT_DIR.
const pem = (envVar, file) => process.env[envVar] || fs.readFileSync(path.join(IOT_CERT_DIR, file));
const client = mqtt.connect(`mqtts://${IOT_ENDPOINT}:8883`, {
  clientId: CLIENT_ID,
  cert: pem("IOT_CERT", `${CERT_NAME}.cert.pem`),
  key: pem("IOT_KEY", `${CERT_NAME}.private.key`),
  ca: pem("IOT_CA", "AmazonRootCA1.pem"),
  reconnectPeriod: 2000,
});
client.on("connect", () => {
  // Shared subscription: IoT Core load-balances telemetry across all ingest replicas in the
  // "ingest" group, so adding a replica splits the load instead of duplicating every message.
  client.subscribe("$share/ingest/fleet/telemetry/#", (err) => {
    console.log(err ? `[ingest] subscribe error ${err.message}` : `[ingest ${INSTANCE}] ${CLIENT_ID} subscribed $share/ingest/fleet/telemetry/# via IoT Core`);
  });
});
client.on("error", (e) => console.error("[ingest] mqtt error:", e.message));

client.on("message", async (_topic, raw) => {
  let t;
  try { t = JSON.parse(raw.toString()); } catch { errors++; return; }
  processed++; window++;
  buffer.push({ ...t, ts: new Date(t.ts) });

  // Update live vehicle state. Status is split-ownership: telemetry may set physical states
  // (charging/fault) but must never overwrite busy, which dispatch owns. Once the physical state
  // clears, the vehicle returns to available (else it would stay charging/fault forever).
  // $literal: telemetry is untrusted, so its values are never evaluated as pipeline expressions.
  const physical = t.status === "charging" || t.status === "fault";
  const status = physical ? t.status : {
    $cond: [{ $in: [{ $ifNull: ["$status", "charging"] }, ["charging", "fault"]] }, "available", "$status"],
  };
  const set = { location: t.location, speed: t.speed, soc: t.soc, faults: t.faults };
  for (const k in set) set[k] = { $literal: set[k] };
  vehicles.updateOne({ vehicleId: t.vehicleId }, [{ $set: { ...set, status, updatedAt: "$$NOW" } }], { upsert: true })
    .catch((e) => { errors++; console.error("[ingest] upsert:", e.message); });
});

const app = express();
app.get("/healthz", (_req, res) => res.json({ ok: true, instance: INSTANCE }));
app.get("/metrics", (_req, res) => res.json({
  instance: INSTANCE, processed, errors, msgsPerSec, bufferDepth: buffer.length,
}));
// Prometheus-style text too, in case the dashboard/experiments prefer scraping.
app.get("/metrics.txt", (_req, res) => {
  res.type("text/plain").send(
    `ingest_processed_total ${processed}\ningest_msgs_per_sec ${msgsPerSec}\ningest_errors_total ${errors}\n`,
  );
});
app.listen(PORT, () => console.log(`[ingest ${INSTANCE}] metrics on :${PORT}`));
