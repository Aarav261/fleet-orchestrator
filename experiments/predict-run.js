// HD experiment: reactive vs hybrid (predictive + reactive) autoscaling for dispatch.
// A repeating 12-min demand cycle (2 min low, 5 min peak, 5 min low), run as alternating arms:
//   reactive  predictor rule disabled -> only the SQS backlog target-tracking policy
//   hybrid    predictor rule enabled  -> MinCapacity raised ahead of the forecast peak, reactive on top
// Each cycle starts cold: dispatch reset to 1 task. Cycles sit on a fixed clock (start + k*12 min) so
// the predictor's "one period ago" lines up with the previous cycle's demand. The first cycle is
// reactive, so hybrid always has a cycle of history. Fleet 15 throughout (ingest stays at 1 task).
// Usage (repo root, ~50 min + 2 min settle): node experiments/predict-run.js   [CYCLES=4]
const { spawn, execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const ROOT = path.join(__dirname, "..");
const OUT = path.join(__dirname, "results", `predict-${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}`);
fs.mkdirSync(OUT, { recursive: true });

const aws = (...a) => {
  const out = execFileSync("aws", [...a, "--region", "us-east-1", "--output", "json"], { encoding: "utf8" });
  return out.trim() ? JSON.parse(out) : {};
};
const outputs = Object.fromEntries(aws("cloudformation", "describe-stacks", "--stack-name", "fleet").Stacks[0].Outputs
  .map((o) => [o.OutputKey, o.OutputValue]));
const res = (id) => aws("cloudformation", "describe-stack-resource", "--stack-name", "fleet", "--logical-resource-id", id)
  .StackResourceDetail.PhysicalResourceId;
const CLUSTER = outputs.ClusterName;
const DISPATCH = res("DispatchService"); // ARN
const RESOURCE_ID = `service/${CLUSTER}/${DISPATCH.split("/").pop()}`;
const QUEUE_URL = res("RiderQueue");
const RULE = outputs.PredictorRule;
const ALB = outputs.DashboardUrl.replace(/\/$/, "");

const CYCLE = [{ seg: "low1", min: 2, rps: 2 }, { seg: "peak", min: 5, rps: 15 }, { seg: "low2", min: 5, rps: 2 }];
const PERIOD_MS = CYCLE.reduce((s, c) => s + c.min, 0) * 60_000; // must equal the stack's PredictPeriodMin
const CYCLES = parseInt(process.env.CYCLES || "4", 10);
const armOf = (k) => (k % 2 === 0 ? "reactive" : "hybrid");

const log = (m) => console.log(`[${new Date().toISOString().slice(11, 19)}] ${m}`);
const sleep = (ms) => new Promise((ok) => setTimeout(ok, ms));
const node = (script, env) => spawn(process.execPath, [path.join(ROOT, script)],
  { cwd: ROOT, env: { ...process.env, ...env }, stdio: ["ignore", "ignore", "inherit"] });

function reset() { // cold start for the next cycle: predictor off, floor 1, one task
  aws("events", "disable-rule", "--name", RULE);
  aws("application-autoscaling", "register-scalable-target", "--service-namespace", "ecs",
    "--scalable-dimension", "ecs:service:DesiredCount", "--resource-id", RESOURCE_ID, "--min-capacity", "1");
  aws("ecs", "update-service", "--cluster", CLUSTER, "--service", DISPATCH, "--desired-count", "1");
}

const csv = fs.createWriteStream(path.join(OUT, "samples.csv"));
csv.write("time,cycle,arm,seg,rps,t_in_cycle_s,dispatch_desired,dispatch_running,dispatch_min,queue_visible\n");
function sample(k, c, tIn) {
  const s = aws("ecs", "describe-services", "--cluster", CLUSTER, "--services", DISPATCH).services[0];
  const q = aws("sqs", "get-queue-attributes", "--queue-url", QUEUE_URL, "--attribute-names", "ApproximateNumberOfMessages")
    .Attributes.ApproximateNumberOfMessages;
  const min = aws("application-autoscaling", "describe-scalable-targets", "--service-namespace", "ecs",
    "--resource-ids", RESOURCE_ID).ScalableTargets[0].MinCapacity;
  const row = [new Date().toISOString(), k, armOf(k), c.seg, c.rps, Math.round(tIn / 1000),
    s.desiredCount, s.runningCount, min, q];
  csv.write(row.join(",") + "\n");
  return row;
}

(async () => {
  log(`cluster ${CLUSTER}, rule ${RULE}, results -> ${path.relative(ROOT, OUT)}`);
  reset();
  for (;;) { // wait until dispatch is at 1 task and the queue is empty before the clock starts
    const s = aws("ecs", "describe-services", "--cluster", CLUSTER, "--services", DISPATCH).services[0];
    if (s.runningCount === 1) break;
    log(`  waiting for dispatch to settle (${s.runningCount} running)`);
    await sleep(15_000);
  }
  const t0 = Date.now();
  const totalMin = (CYCLES * PERIOD_MS) / 60_000;
  const sim = node("simulator/fleet-simulator.js", { N: "15", EDGE_FILTER: "off", RATE_MS: "1000", DURATION_S: String(totalMin * 60 + 60) });

  for (let k = 0; k < CYCLES; k++) {
    const cycleStart = t0 + k * PERIOD_MS;
    if (k > 0) reset();
    if (armOf(k) === "hybrid") aws("events", "enable-rule", "--name", RULE);
    log(`cycle ${k} (${armOf(k)})`);
    let segStart = cycleStart;
    for (const c of CYCLE) {
      const segEnd = segStart + c.min * 60_000;
      const riders = node("simulator/rider-load.js", { DISPATCH_URL: ALB, RPS: String(c.rps), BURST: "1", BURST_AT: "9999",
        DURATION: String(Math.round((segEnd - Date.now()) / 1000)) });
      while (Date.now() < segEnd) {
        const r = sample(k, c, Date.now() - cycleStart);
        log(`  ${c.seg.padEnd(4)} dispatch ${r[7]}/${r[6]} (min ${r[8]})  queue ${r[9]}`);
        await sleep(Math.min(10_000, Math.max(0, segEnd - Date.now())));
      }
      riders.kill();
      segStart = segEnd;
    }
  }
  sim.kill();
  reset();
  csv.end();

  // Evidence: scaling activities (who changed the task count and why) and the predictor's forecasts.
  const start = new Date(t0).toISOString(), end = new Date().toISOString();
  const acts = aws("application-autoscaling", "describe-scaling-activities", "--service-namespace", "ecs",
    "--resource-id", RESOURCE_ID).ScalingActivities.filter((a) => a.StartTime >= start)
    .map((a) => ({ start: a.StartTime, status: a.StatusCode, description: a.Description, cause: a.Cause }));
  fs.writeFileSync(path.join(OUT, "scaling-activities.json"), JSON.stringify(acts, null, 1));
  const q = (id, ns, metric, dims, stat) => ({ Id: id, MetricStat: { Metric: { Namespace: ns, MetricName: metric,
    Dimensions: Object.entries(dims).map(([Name, Value]) => ({ Name, Value })) }, Period: 60, Stat: stat } });
  const queries = [
    q("predicted_rps", "Fleet", "PredictedRps", { Service: "dispatch" }, "Maximum"),
    q("predicted_min_tasks", "Fleet", "PredictedMinTasks", { Service: "dispatch" }, "Maximum"),
    q("sent", "AWS/SQS", "NumberOfMessagesSent", { QueueName: QUEUE_URL.split("/").pop() }, "Sum"),
  ];
  fs.writeFileSync(path.join(OUT, "queries.json"), JSON.stringify(queries));
  const cw = aws("cloudwatch", "get-metric-data", "--start-time", start, "--end-time", end,
    "--metric-data-queries", `file://${path.join(OUT, "queries.json").replaceAll("\\", "/")}`);
  fs.writeFileSync(path.join(OUT, "cloudwatch.json"), JSON.stringify(cw.MetricDataResults, null, 1));
  log(`done: ${acts.length} scaling activities, results in ${path.relative(ROOT, OUT)}`);
})();
