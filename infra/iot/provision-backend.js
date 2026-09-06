// Register backend-service IoT identities (ingest, dispatch, node-red): one cert per service
// attached to FleetBackendPolicy. Backend services are not vehicles — they get broader but still
// scoped access (see backend-policy.json: telemetry/dispatch/alerts). Idempotent on existing certs.
// Usage: node provision-backend.js [svc-name ...]   (default: svc-ingest svc-dispatch svc-nodered)
const { execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const REGION = "us-east-1";
const POLICY = "FleetBackendPolicy";
const args = process.argv.slice(2);
const services = args.length ? args : ["svc-ingest", "svc-dispatch", "svc-nodered"];
const certsDir = path.join(__dirname, "certs");
fs.mkdirSync(certsDir, { recursive: true });

const aws = (a) => execFileSync("aws", a, { encoding: "utf8" });

for (const name of services) {
  const certPath = path.join(certsDir, `${name}.cert.pem`);
  if (fs.existsSync(certPath)) { console.log(`${name}: exists, skip`); continue; }
  aws(["iot", "create-thing", "--thing-name", name, "--region", REGION]);
  const out = JSON.parse(aws(["iot", "create-keys-and-certificate", "--set-as-active",
    "--certificate-pem-outfile", certPath,
    "--public-key-outfile", path.join(certsDir, `${name}.public.key`),
    "--private-key-outfile", path.join(certsDir, `${name}.private.key`),
    "--region", REGION]));
  aws(["iot", "attach-policy", "--policy-name", POLICY, "--target", out.certificateArn, "--region", REGION]);
  aws(["iot", "attach-thing-principal", "--thing-name", name, "--principal", out.certificateArn, "--region", REGION]);
  console.log(`${name}: provisioned`);
}
console.log("done");
