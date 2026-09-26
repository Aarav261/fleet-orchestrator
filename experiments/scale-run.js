// Autoscaling experiment on the deployed stack: one timeline that shows the two services scaling
// on two independent signals.
//   A1-A3  fleet 15 -> 35 -> 55 vehicles, no riders   -> ingest scales out, dispatch stays at 1
//   B      fleet 15, 10 riders/s                      -> dispatch scales out, ingest scales in
//   C      fleet 15, no riders                        -> dispatch scales in
// Edge filter is off so offered telemetry = N msgs/s. Samples task counts + SQS depth every 15 s,
// then pulls the CloudWatch series and scaling activities for the report.
// Usage (repo root, ~45 min, needs a lab session with >= 1 h left): node experiments/scale-run.js
const { spawn, execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const ROOT = path.join(__dirname, "..");
const OUT = path.join(__dirname, "results", `scale-${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}`);
fs.mkdirSync(OUT, { recursive: true });

const aws = (...a) => JSON.parse(execFileSync("aws", [...a, "--region", "us-east-1", "--output", "json"], { encoding: "utf8" }));
const res = (id) => aws("cloudformation", "describe-stack-resource", "--stack-name", "fleet", "--logical-resource-id", id)
  .StackResourceDetail.PhysicalResourceId;
const CLUSTER = res("Cluster");
const SVC = { ingest: res("IngestService"), dispatch: res("DispatchService") }; // ARNs
const QUEUE_URL = res("RiderQueue");
const ALB = aws("cloudformation", "describe-stacks", "--stack-name", "fleet").Stacks[0].Outputs
  .find((o) => o.OutputKey === "DashboardUrl").OutputValue.replace(/\/$/, "");

const PHASES = [
  { name: "A1", min: 5, N: 15, rps: 0 },
  { name: "A2", min: 6, N: 35, rps: 0 },
  { name: "A3", min: 7, N: 55, rps: 0 },
  { name: "B", min: 7, N: 15, rps: 10 },
  { name: "C", min: 20, N: 15, rps: 0, untilSettled: true }, // ends early once both are back to 1 task
];

const log = (m) => console.log(`[${new Date().toISOString().slice(11, 19)}] ${m}`);
const node = (script, env) => spawn(process.execPath, [path.join(ROOT, script)],
  { cwd: ROOT, env: { ...process.env, ...env }, stdio: ["ignore", "ignore", "inherit"] });

const csv = fs.createWriteStream(path.join(OUT, "samples.csv"));
csv.write("time,phase,fleet,rps,ingest_desired,ingest_running,dispatch_desired,dispatch_running,queue_visible,matched,unmatched\n");
async function sample(p) {
  const s = aws("ecs", "describe-services", "--cluster", CLUSTER, "--services", SVC.ingest, SVC.dispatch).services;
  const by = (arn) => s.find((x) => x.serviceArn === arn);
  const q = aws("sqs", "get-queue-attributes", "--queue-url", QUEUE_URL, "--attribute-names", "ApproximateNumberOfMessages")
    .Attributes.ApproximateNumberOfMessages;
  let m = {};
  try { m = await (await fetch(`${ALB}/metrics`)).json(); } catch { /* ALB hiccup: leave blank */ }
  const row = [new Date().toISOString(), p.name, p.N, p.rps, by(SVC.ingest).desiredCount, by(SVC.ingest).runningCount,
    by(SVC.dispatch).desiredCount, by(SVC.dispatch).runningCount, q, m.matched ?? "", m.unmatched ?? ""];
  csv.write(row.join(",") + "\n");
  return row;
}

(async () => {
  const start = new Date();
  log(`cluster ${CLUSTER}, results -> ${path.relative(ROOT, OUT)}`);
  for (const p of PHASES) {
    log(`phase ${p.name}: fleet=${p.N} riders=${p.rps}/s for up to ${p.min} min`);
    const kids = [node("simulator/fleet-simulator.js", { N: String(p.N), EDGE_FILTER: "off", RATE_MS: "1000", DURATION_S: String(p.min * 60) })];
    if (p.rps) kids.push(node("simulator/rider-load.js", { DISPATCH_URL: ALB, RPS: String(p.rps), BURST: "1", BURST_AT: "9999", DURATION: String(p.min * 60) }));
    const end = Date.now() + p.min * 60_000;
    while (Date.now() < end) {
      const r = await sample(p);
      log(`  ingest ${r[5]}/${r[4]}  dispatch ${r[7]}/${r[6]}  queue ${r[8]}`);
      if (p.untilSettled && r[4] === 1 && r[6] === 1 && r[5] === 1 && r[7] === 1) { log("  both back to 1 task"); break; }
      await new Promise((ok) => setTimeout(ok, 15_000));
    }
    for (const k of kids) k.kill(); // ponytail: a phase gap of ~1-2 s while the next simulator reconnects
  }
  csv.end();

  // CloudWatch series (1-min) for the graphs, plus the scaling activities as timestamped evidence.
  const q = (id, ns, metric, dims, stat) => ({ Id: id, MetricStat: { Metric: { Namespace: ns, MetricName: metric,
    Dimensions: Object.entries(dims).map(([Name, Value]) => ({ Name, Value })) }, Period: 60, Stat: stat } });
  const svcName = (arn) => arn.split("/").pop();
  const queries = [
    q("ingest_msgs_per_task", "Fleet", "IngestMsgsPerSec", { Service: "ingest" }, "Average"),
    q("ingest_tasks", "ECS/ContainerInsights", "RunningTaskCount", { ClusterName: CLUSTER, ServiceName: svcName(SVC.ingest) }, "Average"),
    q("dispatch_tasks", "ECS/ContainerInsights", "RunningTaskCount", { ClusterName: CLUSTER, ServiceName: svcName(SVC.dispatch) }, "Average"),
    q("queue_visible", "AWS/SQS", "ApproximateNumberOfMessagesVisible", { QueueName: QUEUE_URL.split("/").pop() }, "Maximum"),
  ];
  fs.writeFileSync(path.join(OUT, "queries.json"), JSON.stringify(queries));
  const cw = aws("cloudwatch", "get-metric-data", "--start-time", start.toISOString(), "--end-time", new Date().toISOString(),
    "--metric-data-queries", `file://${path.join(OUT, "queries.json").replaceAll("\\", "/")}`);
  fs.writeFileSync(path.join(OUT, "cloudwatch.json"), JSON.stringify(cw.MetricDataResults, null, 1));
  const acts = Object.values(SVC).flatMap((arn) => aws("application-autoscaling", "describe-scaling-activities",
    "--service-namespace", "ecs", "--resource-id", `service/${svcName(CLUSTER)}/${svcName(arn)}`).ScalingActivities)
    .filter((a) => new Date(a.StartTime) >= start)
    .map((a) => ({ start: a.StartTime, resource: a.ResourceId.split("/").pop(), status: a.StatusCode, description: a.Description, cause: a.Cause }));
  fs.writeFileSync(path.join(OUT, "scaling-activities.json"), JSON.stringify(acts, null, 1));
  log(`done: ${acts.length} scaling activities, results in ${path.relative(ROOT, OUT)}`);
})();
