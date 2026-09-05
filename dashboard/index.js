// Operator dashboard: serves one live page and a /api/state endpoint aggregating
// fleet state, active trips, and alerts from MongoDB.
import express from "express";
import { MongoClient } from "mongodb";

const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017/fleet";
const PORT = parseInt(process.env.PORT || "3000", 10);

const mongo = new MongoClient(MONGO_URL);
await mongo.connect();
const db = mongo.db();

const app = express();
app.get("/api/state", async (_req, res) => {
  const [byStatus, tripsByStatus, recentTrips, recentAlerts, vehicleCount] = await Promise.all([
    db.collection("vehicles").aggregate([{ $group: { _id: "$status", n: { $sum: 1 } } }]).toArray(),
    db.collection("trips").aggregate([{ $group: { _id: "$status", n: { $sum: 1 } } }]).toArray(),
    db.collection("trips").find().sort({ requested_at: -1 }).limit(10).toArray(),
    db.collection("alerts").find().sort({ timestamp: -1 }).limit(10).toArray(),
    db.collection("vehicles").countDocuments(),
  ]);
  res.json({
    vehicleCount,
    vehiclesByStatus: Object.fromEntries(byStatus.map((s) => [s._id, s.n])),
    tripsByStatus: Object.fromEntries(tripsByStatus.map((s) => [s._id, s.n])),
    recentTrips, recentAlerts,
  });
});

app.get("/", (_req, res) => res.type("html").send(PAGE));

const PAGE = `<!doctype html><meta charset=utf-8><title>Fleet Operator Dashboard</title>
<style>
 body{font:14px system-ui,sans-serif;margin:0;background:#0f1220;color:#e6e8f0}
 header{padding:14px 20px;background:#161a2e;border-bottom:1px solid #262b45}
 h1{font-size:18px;margin:0}
 .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;padding:20px}
 .card{background:#161a2e;border:1px solid #262b45;border-radius:10px;padding:14px}
 .card h2{font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:#8b90b5;margin:0 0 10px}
 .kpi{font-size:30px;font-weight:700}
 .row{display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid #21263f}
 .pill{display:inline-block;padding:1px 8px;border-radius:20px;font-size:12px}
 .available{background:#123d2a;color:#54e39b}.busy{background:#3d2f12;color:#e3c054}
 .charging{background:#12303d;color:#54c7e3}.fault{background:#3d1220;color:#e35470}
 table{width:100%;border-collapse:collapse;font-size:12px}td{padding:3px 0;border-bottom:1px solid #21263f}
 small{color:#8b90b5}
</style>
<header><h1>🚕 Driverless Taxi — Operator Dashboard</h1><small id=t></small></header>
<div class=grid>
 <div class=card><h2>Vehicles</h2><div class=kpi id=vcount>–</div><div id=vstatus></div></div>
 <div class=card><h2>Trips by status</h2><div id=tstatus></div></div>
 <div class=card style="grid-column:span 2"><h2>Recent trips</h2><table id=trips></table></div>
 <div class=card style="grid-column:span 2"><h2>Recent alerts</h2><table id=alerts></table></div>
</div>
<script>
const badge=s=>'<span class="pill '+s+'">'+s+'</span>';
async function tick(){
 const r=await fetch('/api/state');const d=await r.json();
 vcount.textContent=d.vehicleCount;
 vstatus.innerHTML=Object.entries(d.vehiclesByStatus).map(([k,v])=>'<div class=row>'+badge(k)+'<b>'+v+'</b></div>').join('');
 tstatus.innerHTML=Object.entries(d.tripsByStatus).map(([k,v])=>'<div class=row><span>'+k+'</span><b>'+v+'</b></div>').join('')||'<small>none yet</small>';
 trips.innerHTML=d.recentTrips.map(t=>'<tr><td>'+(t.vehicleId||'—')+'</td><td>'+badge(t.status)+'</td><td><small>'+new Date(t.requested_at).toLocaleTimeString()+'</small></td></tr>').join('')||'<small>none yet</small>';
 alerts.innerHTML=d.recentAlerts.map(a=>'<tr><td>'+a.vehicleId+'</td><td>'+badge(a.type.includes('fault')?'fault':'charging')+'</td><td>'+a.severity+'</td><td><small>'+new Date(a.timestamp).toLocaleTimeString()+'</small></td></tr>').join('')||'<small>none yet</small>';
 t.textContent='updated '+new Date().toLocaleTimeString();
}
tick();setInterval(tick,2000);
</script>`;

app.listen(PORT, () => console.log(`[dashboard] http://localhost:${PORT}`));
