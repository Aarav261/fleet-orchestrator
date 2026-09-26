// Node-RED settings for the fleet flow (run locally with podman compose).
const { MongoClient } = require("mongodb");

module.exports = {
  flowFile: "/usr/src/node-red/flows.json",
  uiPort: 1880,
  credentialSecret: false,
  // One shared Atlas client for every function node (connects lazily on first use).
  functionGlobalContext: { mongo: new MongoClient(process.env.MONGO_URL) },
  logging: { console: { level: "info", metrics: false, audit: false } },
};
