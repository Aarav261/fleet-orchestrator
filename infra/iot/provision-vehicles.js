// One-time: register N vehicles as AWS IoT Things, each with its own X.509 cert bound to
// FleetVehiclePolicy. IDs match the simulator (veh-0000 .. veh-<N-1>). Idempotent — skips a
// vehicle whose cert already exists. Shells out to the configured aws CLI (no SDK dependency).
//
// Usage: node provision-vehicles.js [count]        (default 10)
// Certs land in ./certs/<id>.{cert.pem,private.key,public.key}; download AmazonRootCA1.pem once.
const { execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const REGION = "us-east-1";
const POLICY = "FleetVehiclePolicy";
const count = parseInt(process.argv[2] || "10", 10);
const certsDir = path.join(__dirname, "certs");
fs.mkdirSync(certsDir, { recursive: true });

const aws = (args) => execFileSync("aws", args, { encoding: "utf8" });
const vehId = (i) => `veh-${String(i).padStart(4, "0")}`; // matches simulator/simulator.js

for (let i = 0; i < count; i++) {
  const name = vehId(i);
  const certPath = path.join(certsDir, `${name}.cert.pem`);
  if (fs.existsSync(certPath)) { console.log(`${name}: exists, skip`); continue; }

  aws(["iot", "create-thing", "--thing-name", name, "--region", REGION]);
  const out = JSON.parse(aws(["iot", "create-keys-and-certificate", "--set-as-active",
    "--certificate-pem-outfile", certPath,
    "--public-key-outfile", path.join(certsDir, `${name}.public.key`),
    "--private-key-outfile", path.join(certsDir, `${name}.private.key`),
    "--region", REGION]));
  const arn = out.certificateArn;
  aws(["iot", "attach-policy", "--policy-name", POLICY, "--target", arn, "--region", REGION]);
  aws(["iot", "attach-thing-principal", "--thing-name", name, "--principal", arn, "--region", REGION]);
  console.log(`${name}: provisioned`);
}
console.log(`done (${count} vehicles requested)`);
// ponytail: per-vehicle certs, fine to a few hundred. For large-N ingest load tests switch to one
// shared cert + a clientId-not-thing-bound policy; keep per-vehicle X.509 for the security demo.
