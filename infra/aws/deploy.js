// Deploy the platform to AWS: ECR images + Secrets Manager here, everything else in fleet.yaml
// (CloudFormation stack "fleet"). Re-run after code changes (new image tag rolls the services) or
// after a lab reset. Reads IOT_ENDPOINT and MONGO_URL from the root .env and the backend certs from
// infra/iot/certs. Learner Lab: every role is LabRole (no custom IAM).
// Usage (repo root): node infra/aws/deploy.js
// Tear down:         aws cloudformation delete-stack --stack-name fleet   (ECR + secrets are kept)
const { execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const ROOT = path.join(__dirname, "..", "..");
process.loadEnvFile(path.join(ROOT, ".env"));
const { IOT_ENDPOINT, MONGO_URL } = process.env;
const CERTS = path.join(ROOT, "infra", "iot", "certs");

const REGION = "us-east-1";
const STACK = "fleet";
// Fargate is not available in every AZ (us-east-1e is the usual gap); the ALB needs >= 2 AZs.
const AZS = ["us-east-1a", "us-east-1b", "us-east-1c"];
const IMAGES = { ingest: "services/ingest", dispatch: "services/dispatch", dashboard: "dashboard" };
const TAG = `v${Date.now()}`; // unique per run, so CloudFormation sees new task definitions

const aws = (...a) => {
  const out = execFileSync("aws", [...a, "--region", REGION, "--output", "json"], { encoding: "utf8" });
  return out.trim() ? JSON.parse(out) : {};
};
const tryAws = (...a) => { try { return aws(...a); } catch { return null; } };
const sh = (cmd, args, input) => execFileSync(cmd, args, { stdio: input ? ["pipe", "inherit", "inherit"] : "inherit", input });
const log = (m) => console.log(`[deploy] ${m}`);

const ACCOUNT = aws("sts", "get-caller-identity").Account;
const REGISTRY = `${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com`;
const VPC = aws("ec2", "describe-vpcs", "--filters", "Name=isDefault,Values=true").Vpcs[0].VpcId;
const SUBNETS = aws("ec2", "describe-subnets", "--filters", `Name=vpc-id,Values=${VPC}`, `Name=availability-zone,Values=${AZS.join(",")}`)
  .Subnets.map((s) => s.SubnetId);

// --- secrets: kept out of the stack so certs never appear as stack parameters -----------------------
function secret(name, value) {
  const s = JSON.stringify(value);
  const found = tryAws("secretsmanager", "describe-secret", "--secret-id", name);
  if (found) { aws("secretsmanager", "put-secret-value", "--secret-id", name, "--secret-string", s); return found.ARN; }
  return aws("secretsmanager", "create-secret", "--name", name, "--secret-string", s).ARN;
}
const read = (f) => fs.readFileSync(path.join(CERTS, f), "utf8");
const certSecret = (svc) => secret(`fleet/${svc}`, { cert: read(`${svc}.cert.pem`), key: read(`${svc}.private.key`) });
log("secrets");
const SHARED = secret("fleet/shared", { mongo_url: MONGO_URL, iot_ca: read("AmazonRootCA1.pem") });
const INGEST_SECRET = certSecret("svc-ingest");
const DISPATCH_SECRET = certSecret("svc-dispatch");

// --- images: build with podman, push to ECR (the stack can't build images) --------------------------
log("ECR login");
sh("podman", ["login", "--username", "AWS", "--password-stdin", REGISTRY],
  execFileSync("aws", ["ecr", "get-login-password", "--region", REGION], { encoding: "utf8" }));
for (const [name, dir] of Object.entries(IMAGES)) {
  const repo = `fleet-${name}`;
  if (!tryAws("ecr", "describe-repositories", "--repository-names", repo)) aws("ecr", "create-repository", "--repository-name", repo);
  const image = `${REGISTRY}/${repo}:${TAG}`;
  log(`build + push ${image}`);
  sh("podman", ["build", "--platform", "linux/amd64", "-t", image, path.join(ROOT, dir)]);
  sh("podman", ["push", image]);
}

// --- stack ------------------------------------------------------------------------------------------
log(`stack ${STACK}`);
sh("aws", ["cloudformation", "deploy", "--region", REGION, "--stack-name", STACK, "--no-fail-on-empty-changeset",
  "--template-file", path.join(__dirname, "fleet.yaml"),
  "--parameter-overrides", `VpcId=${VPC}`, `Subnets=${SUBNETS.join(",")}`, `ImageTag=${TAG}`, `IotEndpoint=${IOT_ENDPOINT}`,
  `SharedSecretArn=${SHARED}`, `IngestSecretArn=${INGEST_SECRET}`, `DispatchSecretArn=${DISPATCH_SECRET}`]);
const outputs = aws("cloudformation", "describe-stacks", "--stack-name", STACK).Stacks[0].Outputs;
for (const o of outputs) log(`${o.OutputKey}: ${o.OutputValue}`);
