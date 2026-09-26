# AWS IoT Core (task 4.3 — cloud MQTT broker)

Replaces the local Mosquitto broker for the AWS deployment. Same topics as local:
`fleet/telemetry/<id>` (vehicle → platform), `fleet/dispatch/<id>` (platform → vehicle).

- **Environment:** AWS Academy Learner Lab, region `us-east-1`. Credentials rotate each lab
  session — re-paste to `~/.aws/credentials` (`AWS Details → AWS CLI → Show`).
- **Data endpoint:** `arvw7wm7bzzld-ats.iot.us-east-1.amazonaws.com` (mqtts, port 8883).
  Re-fetch with `aws iot describe-endpoint --endpoint-type iot:Data-ATS`.
- **Policy `FleetVehiclePolicy`** (`vehicle-policy.json`): per-vehicle least-privilege — a cert may
  connect only as its own Thing name, publish only `fleet/telemetry/<its-id>`, and subscribe only
  `fleet/dispatch/<its-id>`. This is the per-vehicle X.509 security story.

## Provision vehicle credentials
```bash
node provision-vehicles.js 10   # creates veh-0000..veh-0009 Things + certs, attaches the policy
```
Certs land in `certs/` (gitignored — private keys). `AmazonRootCA1.pem` must be present:
```bash
curl -o certs/AmazonRootCA1.pem https://www.amazontrust.com/repository/AmazonRootCA1.pem
```

## Point the simulator at IoT Core (pending)
The simulator currently connects to Mosquitto over plain MQTT. To use IoT Core it needs mTLS —
env-gated so local runs still default to Mosquitto. Ingest (subscribes `fleet/telemetry/#`) needs a
separate backend-service policy/cert, since the per-vehicle policy above is scoped to one vehicle.
