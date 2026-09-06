// Telemetry Ingest service.
// Subscribes to fleet/telemetry/#, upserts live vehicle state + appends telemetry,
// raises alerts (low battery / fault), and exposes /metrics for autoscaling.
//
// Scales with FLEET SIZE. In the plan its autoscaling trigger is message volume;
// the /metrics msgsPerSec below is exactly that signal.

import "dotenv/config";
import express from "express";
import mqtt from "mqtt";
import { MongoClient } from "mongodb";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// AWS IoT Core (mTLS) — the only broker. Connects with the svc-ingest backend cert, whose policy
// (FleetBackendPolicy) permits the fleet/telemetry/# wildcard subscribe.
const IOT_ENDPOINT = process.env.IOT_ENDPOINT;
const IOT_CERT_DIR = process.env.IOT_CERT_DIR || path.resolve(__dirname, "../../infra/iot/certs");
const CLIENT_ID = process.env.IOT_CLIENT_ID || "svc-ingest";
if (!IOT_ENDPOINT) { console.error("[ingest] IOT_ENDPOINT is required (AWS IoT Core data endpoint)"); process.exit(1); }

const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017/fleet";
const PORT = parseInt(process.env.PORT || "3001", 10);
const INSTANCE = process.env.HOSTNAME || "ingest-local";

const mongo = new MongoClient(MONGO_URL);
await mongo.connect();
const db = mongo.db();
const telemetry = db.collection("telemetry");
const vehicles = db.collection("vehicles");
const alerts = db.collection("alerts");
await vehicles.createIndex({ location: "2dsphere" });
await vehicles.createIndex({ vehicleId: 1 }, { unique: true });
await telemetry.createIndex({ vehicleId: 1, ts: -1 });
console.log(`[ingest ${INSTANCE}] mongo connected, indexes ensured`);

// --- metrics ---
let processed = 0, errors = 0, alertsRaised = 0;
let window = 0; // messages in the current 1 s window
let msgsPerSec = 0;
setInterval(() => { msgsPerSec = window; window = 0; }, 1000);

// buffer telemetry writes to avoid one insert per message under load
let buffer = [];
setInterval(async () => {
  if (!buffer.length) return;
  const batch = buffer; buffer = [];
  try { await telemetry.insertMany(batch, { ordered: false }); }
  catch (e) { errors++; console.error("[ingest] bulk insert error:", e.message); }
}, 1000);

const client = mqtt.connect(`mqtts://${IOT_ENDPOINT}:8883`, {
  clientId: CLIENT_ID,
  cert: fs.readFileSync(path.join(IOT_CERT_DIR, `${CLIENT_ID}.cert.pem`)),
  key: fs.readFileSync(path.join(IOT_CERT_DIR, `${CLIENT_ID}.private.key`)),
  ca: fs.readFileSync(path.join(IOT_CERT_DIR, "AmazonRootCA1.pem")),
  reconnectPeriod: 2000,
});
client.on("connect", () => {
  client.subscribe("fleet/telemetry/#", (err) => {
    console.log(err ? `[ingest] subscribe error ${err.message}` : `[ingest ${INSTANCE}] subscribed fleet/telemetry/# via IoT Core`);
  });
});
client.on("error", (e) => console.error("[ingest] mqtt error:", e.message));

client.on("message", async (_topic, raw) => {
  let t;
  try { t = JSON.parse(raw.toString()); } catch { errors++; return; }
  processed++; window++;
  buffer.push({ ...t, ts: new Date(t.ts) });

  // live vehicle state (upsert). Status ownership is split: the vehicle asserts
  // only physical states (charging, fault); the Booking service owns available/busy.
  // So we never let telemetry clobber a booking-owned status.
  const set = { vehicleId: t.vehicleId, location: t.location, speed: t.speed, soc: t.soc, faults: t.faults, updatedAt: new Date() };
  const update = { $set: set };
  if (t.status === "charging" || t.status === "fault") set.status = t.status;
  else update.$setOnInsert = { status: "available" };
  vehicles.updateOne({ vehicleId: t.vehicleId }, update, { upsert: true })
    .catch((e) => { errors++; console.error("[ingest] upsert:", e.message); });

  // alerts
  if (t.soc < 15) raiseAlert(t.vehicleId, "low_battery", "warning");
  if (t.status === "fault") raiseAlert(t.vehicleId, "safety_fault", "critical");
});

function raiseAlert(vehicleId, type, severity) {
  alertsRaised++;
  alerts.insertOne({ vehicleId, type, severity, timestamp: new Date() }).catch(() => errors++);
}

const app = express();
app.get("/healthz", (_req, res) => res.json({ ok: true, instance: INSTANCE }));
app.get("/metrics", (_req, res) => res.json({
  instance: INSTANCE, processed, errors, alertsRaised, msgsPerSec, bufferDepth: buffer.length,
}));
// Prometheus-style text too, in case the dashboard/experiments prefer scraping.
app.get("/metrics.txt", (_req, res) => {
  res.type("text/plain").send(
    `ingest_processed_total ${processed}\ningest_msgs_per_sec ${msgsPerSec}\ningest_errors_total ${errors}\ningest_alerts_total ${alertsRaised}\n`,
  );
});
app.listen(PORT, () => console.log(`[ingest ${INSTANCE}] metrics on :${PORT}`));
