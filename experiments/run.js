// Scalability experiment harness. Cross-platform (Node, no jq/bash).
// Assumes infra (mosquitto/mongo/elasticmq) is up and a Telemetry Ingest service
// is reachable at ING. Manages its own Dispatch consumers for the scaling tests.
//
// Usage: node run.js [exp1|exp2|exp3|exp4|all]
//
// Writes CSVs to ./results/ for plotting.

import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";

const ING = process.env.ING || "http://localhost:3011"; // ingest metrics
const DISP = process.env.DISP || "http://localhost:3002"; // dispatch primary (enqueue + metrics)
const SIM = new URL("../simulator/simulator.js", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");
const RIDERS = new URL("../simulator/rider-load.js", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");
const DISPATCH = new URL("../services/dispatch/index.js", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");
const RESULTS = new URL("./results/", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");

const j = async (u) => (await fetch(u)).json();

// spawn a process, collect stdout; resolve with {code, out} on exit
function run(script, env, { collect = true } = {}) {
  const child = spawn(process.execPath, [script], { env: { ...process.env, ...env } });
  let out = "";
  if (collect) { child.stdout.on("data", (d) => (out += d)); child.stderr.on("data", () => {}); }
  const done = new Promise((res) => child.on("close", (code) => res({ code, out })));
  return { child, done };
}
// parse the last JSON metric line printed by sim/riders
const lastJSON = (s) => {
  const lines = s.trim().split("\n").filter((l) => l.trim().startsWith("{"));
  return lines.length ? JSON.parse(lines.at(-1)) : null;
};

// ---------------------------------------------------------------------------
// Experiment 1 — Ingest capacity: sweep fleet size N, measure sustained
// ingest throughput (msgs/s) and buffer depth (saturation indicator).
async function exp1() {
  console.log("\n=== EXP1 Ingest capacity ===");
  const rows = [["N", "offeredMsgsPerSec", "ingestMsgsPerSec", "bufferDepth"]];
  for (const N of [10, 50, 100, 200, 400, 800]) {
    const before = (await j(`${ING}/metrics`)).processed;
    const sim = run(SIM, { N: String(N), RATE_MS: "1000", EDGE_FILTER: "off", DURATION_S: "16" });
    await sleep(11000);
    // sample steady-state throughput 3x
    let sum = 0, buf = 0;
    for (let i = 0; i < 3; i++) { const m = await j(`${ING}/metrics`); sum += m.msgsPerSec; buf = m.bufferDepth; await sleep(1000); }
    await sim.done;
    const after = (await j(`${ING}/metrics`)).processed;
    const ingestRate = Math.round(sum / 3);
    rows.push([N, N, ingestRate, buf]);
    console.log(`  N=${N} offered=${N}/s ingest=${ingestRate}/s buffer=${buf} processed+=${after - before}`);
  }
  csv("exp1_ingest.csv", rows);
}

// ---------------------------------------------------------------------------
// Experiment 3 — Edge-filter efficiency: same fleet, filter off vs on.
async function exp3() {
  console.log("\n=== EXP3 Edge-filter efficiency ===");
  const rows = [["mode", "generated", "published", "bytes", "suppressedPct"]];
  for (const mode of ["off", "on"]) {
    const sim = run(SIM, { N: "100", RATE_MS: "1000", EDGE_FILTER: mode, DURATION_S: "20" });
    const { out } = await sim.done;
    const m = lastJSON(out);
    rows.push([mode, m.generated, m.published, m.bytes, m.suppressedPct]);
    console.log(`  filter=${mode} generated=${m.generated} published=${m.published} bytes=${m.bytes} suppressed=${m.suppressedPct}%`);
  }
  csv("exp3_edge.csv", rows);
}

// ---------------------------------------------------------------------------
// Dispatch consumer management for exp2/exp4. Each extra consumer binds its own
// port but competes on the SAME SQS queue (competing-consumers pattern).
const CONSUMER_WORKERS = process.env.CONSUMER_WORKERS || "1";
function startConsumers(count, basePort = 3102) {
  const kids = [];
  for (let i = 0; i < count; i++) {
    const { child } = run(DISPATCH, { PORT: String(basePort + i), WORKERS: CONSUMER_WORKERS, HOSTNAME: `dispatch-${i + 2}` }, { collect: false });
    kids.push(child);
  }
  return kids;
}
const killAll = (kids) => kids.forEach((k) => { try { k.kill(); } catch {} });

// Experiment 2 — Rider burst + horizontal scaling. Run the SAME burst against
// 1 dispatch replica, then 3 replicas; record queue depth over time.
async function exp2() {
  console.log("\n=== EXP2 Rider burst + horizontal scaling ===");
  // refresh vehicle pool so there are available cars to match
  const warm = run(SIM, { N: "300", RATE_MS: "1000", EDGE_FILTER: "off", DURATION_S: "8" });
  await warm.done;

  const rows = [["replicas", "t", "queueDepth", "matchP95ms"]];
  // Run the 4-replica phase first (drains to ~0), then 1-replica, so each phase
  // starts from an empty queue and the two curves are directly comparable.
  for (const replicas of [4, 1]) {
    const extra = replicas > 1 ? startConsumers(replicas - 1) : [];
    // wait until the queue has drained from any prior phase (max ~40s)
    for (let w = 0; w < 40; w++) { const d = (await j(`${DISP}/metrics`).catch(() => ({}))).queueDepth ?? 0; if (d < 20) break; await sleep(1000); }
    await sleep(1500);
    // fire a heavy burst: base 20 rps -> 200 rps after 3s, for 25s. One worker per
    // replica, so a single replica (~caps ~100/s) saturates and the queue backs up.
    const riders = run(RIDERS, { DISPATCH_URL: DISP, RPS: "20", BURST: "10", BURST_AT: "3", DURATION: "25" }, { collect: true });
    for (let t = 0; t < 30; t++) {
      const m = await j(`${DISP}/metrics`).catch(() => ({}));
      rows.push([replicas, t, m.queueDepth ?? 0, m.matchP95ms ?? 0]);
      await sleep(1000);
    }
    const { out } = await riders.done;
    killAll(extra);
    const rm = lastJSON(out);
    console.log(`  replicas=${replicas} sent=${rm?.sent} ok=${rm?.ok} maxQ=${Math.max(...rows.filter(r => r[0] === replicas && typeof r[2] === "number").map(r => r[2]))}`);
    await sleep(2000);
  }
  csv("exp2_scaling.csv", rows);
}

// Experiment 4 — Fault tolerance: kill a dispatch consumer mid-burst; show the
// SQS queue buffers requests and every booking is eventually matched (no loss).
async function exp4() {
  console.log("\n=== EXP4 Fault-tolerant booking ===");
  const warm = run(SIM, { N: "300", RATE_MS: "1000", EDGE_FILTER: "off", DURATION_S: "8" });
  await warm.done;
  const extra = startConsumers(2); // 3 total consumers (primary + 2)
  await sleep(1500);

  const rows = [["t", "queueDepth", "enqueued", "matched", "event"]];
  const riders = run(RIDERS, { DISPATCH_URL: DISP, RPS: "200", BURST: "1", DURATION: "20" }, { collect: true });
  let killed = false;
  for (let t = 0; t < 35; t++) {
    const m = await j(`${DISP}/metrics`).catch(() => ({}));
    let event = "";
    if (t === 8 && !killed) { extra[0].kill(); killed = true; event = "KILL consumer"; console.log("  >> killed a dispatch consumer at t=8s"); }
    rows.push([t, m.queueDepth ?? 0, m.enqueued ?? 0, m.matched ?? 0, event]);
    await sleep(1000);
  }
  const { out } = await riders.done;
  killAll(extra);
  const rm = lastJSON(out);
  const final = await j(`${DISP}/metrics`);
  console.log(`  sent=${rm?.sent} enqueued=${final.enqueued} matched+unmatched=${final.matched + final.unmatched} queueDepth=${final.queueDepth}`);
  csv("exp4_fault.csv", rows);
}

function csv(name, rows) {
  const path = RESULTS + name;
  writeFileSync(path, rows.map((r) => r.join(",")).join("\n") + "\n");
  console.log(`  wrote ${path}`);
}

const which = process.argv[2] || "all";
const map = { exp1, exp2, exp3, exp4 };
if (which === "all") { for (const k of ["exp1", "exp3", "exp2", "exp4"]) await map[k](); }
else if (map[which]) await map[which]();
else console.error("unknown:", which);
process.exit(0);
