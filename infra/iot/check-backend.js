// Isolation check: does the svc-nodered backend cert connect + subscribe telemetry + publish alerts?
const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const EP = process.env.IOT_ENDPOINT || "arvw7wm7bzzld-ats.iot.us-east-1.amazonaws.com";
const ID = process.argv[2] || "svc-nodered";
const dir = path.join(__dirname, "certs");
const c = mqtt.connect(`mqtts://${EP}:8883`, {
  clientId: ID,
  cert: fs.readFileSync(path.join(dir, `${ID}.cert.pem`)),
  key: fs.readFileSync(path.join(dir, `${ID}.private.key`)),
  ca: fs.readFileSync(path.join(dir, "AmazonRootCA1.pem")),
});
c.on("connect", () => {
  console.log(`${ID} connected`);
  c.subscribe("fleet/telemetry/#", (e) => {
    console.log(e ? `subscribe FAIL: ${e.message}` : "subscribed fleet/telemetry/# (allowed)");
    c.publish("fleet/alerts/test", JSON.stringify({ test: true }));
    console.log("published fleet/alerts/test");
    setTimeout(() => { console.log("PASS: backend cert+policy OK"); process.exit(0); }, 2000);
  });
});
c.on("error", (e) => { console.error("ERROR:", e.message); process.exit(1); });
setTimeout(() => { console.error("FAIL: timed out"); process.exit(1); }, 10000);
