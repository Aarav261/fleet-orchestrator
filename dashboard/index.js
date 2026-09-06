// Operator dashboard: serves one live page and a /api/state endpoint aggregating
// fleet state, active trips, and alerts from MongoDB.
import "dotenv/config";
import express from "express";
import { MongoClient } from "mongodb";

const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017/fleet";
const PORT = parseInt(process.env.PORT || "3000", 10);

const mongo = new MongoClient(MONGO_URL);
await mongo.connect();
const db = mongo.db();

const app = express();
app.get("/api/state", async (_req, res) => {
  const since = new Date(Date.now() - 15 * 60 * 1000);
  const [byStatus, tripsByStatus, recentTrips, recentAlerts, vehicleCount, alertsByType, throughput] =
    await Promise.all([
      db.collection("vehicles").aggregate([{ $group: { _id: "$status", n: { $sum: 1 } } }]).toArray(),
      db.collection("trips").aggregate([{ $group: { _id: "$status", n: { $sum: 1 } } }]).toArray(),
      db.collection("trips").find().sort({ requested_at: -1 }).limit(12).toArray(),
      db.collection("alerts").find().sort({ timestamp: -1 }).limit(12).toArray(),
      db.collection("vehicles").countDocuments(),
      db.collection("alerts").aggregate([{ $group: { _id: "$type", n: { $sum: 1 } } }]).toArray(),
      // telemetry messages per minute over the last 15 min (throughput sparkline)
      db.collection("telemetry").aggregate([
        { $match: { ts: { $gte: since } } },
        { $group: { _id: { $dateTrunc: { date: "$ts", unit: "minute" } }, n: { $sum: 1 } } },
        { $sort: { _id: 1 } },
      ]).toArray(),
    ]);
  res.json({
    vehicleCount,
    vehiclesByStatus: Object.fromEntries(byStatus.map((s) => [s._id, s.n])),
    tripsByStatus: Object.fromEntries(tripsByStatus.map((s) => [s._id, s.n])),
    alertsByType: Object.fromEntries(alertsByType.map((s) => [s._id, s.n])),
    throughput: throughput.map((t) => ({ t: t._id, n: t.n })),
    recentTrips, recentAlerts,
  });
});

app.get("/", (_req, res) => res.type("html").send(PAGE));

const PAGE = `<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Fleet Operator Dashboard</title>
<style>
 :root{
  --bg:#0b0e17;--panel:#131826;--panel2:#0f1420;--border:#222a3d;--text:#e7e9f2;--muted:#8891ad;
  --avail:#35d08a;--busy:#f0b43f;--charging:#48b7e6;--fault:#f0637a;--matched:#48b7e6;
  --completed:#9b8cff;--queued:#8891ad;--unmatched:#f0637a;
 }
 *{box-sizing:border-box}
 body{font:14px/1.5 system-ui,-apple-system,Segoe UI,sans-serif;margin:0;background:
   radial-gradient(1200px 600px at 80% -10%,#161d33 0,transparent 60%),var(--bg);color:var(--text);
   min-height:100vh}
 header{position:sticky;top:0;z-index:5;display:flex;align-items:center;gap:14px;
   padding:14px 22px;background:rgba(13,17,28,.85);backdrop-filter:blur(8px);
   border-bottom:1px solid var(--border)}
 header h1{font-size:17px;margin:0;font-weight:650;letter-spacing:.2px}
 .live{display:flex;align-items:center;gap:7px;margin-left:auto;color:var(--muted);font-size:12.5px}
 .dot{width:8px;height:8px;border-radius:50%;background:var(--avail);box-shadow:0 0 0 0 var(--avail);
   animation:pulse 2s infinite}
 .dot.stale{background:var(--fault);animation:none}
 @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(53,208,138,.5)}70%{box-shadow:0 0 0 7px rgba(53,208,138,0)}100%{box-shadow:0 0 0 0 rgba(53,208,138,0)}}
 .wrap{padding:20px 22px;max-width:1200px;margin:0 auto;display:flex;flex-direction:column;gap:16px}
 .kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
 .tile{background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--border);
   border-radius:12px;padding:14px 16px}
 .tile .lbl{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
 .tile .num{font-size:32px;font-weight:700;margin-top:4px;line-height:1}
 .tile .sub{font-size:12px;color:var(--muted);margin-top:4px}
 .tile .num.accent{color:var(--avail)}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
 @media(max-width:820px){.grid{grid-template-columns:1fr}}
 .card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:16px}
 .card h2{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin:0 0 12px}
 .span2{grid-column:1/-1}
 .bar{display:flex;height:14px;border-radius:7px;overflow:hidden;background:#0c111c;margin-bottom:12px}
 .bar span{display:block}
 .legend{display:flex;flex-wrap:wrap;gap:10px 16px}
 .legend .it{display:flex;align-items:center;gap:7px;font-size:13px}
 .legend .sw{width:10px;height:10px;border-radius:3px}
 .legend b{margin-left:auto;font-variant-numeric:tabular-nums}
 table{width:100%;border-collapse:collapse;font-size:12.5px}
 th{text-align:left;font-weight:500;color:var(--muted);font-size:11px;text-transform:uppercase;
   letter-spacing:.05em;padding:0 0 6px}
 td{padding:6px 0;border-top:1px solid #1b2233;vertical-align:middle}
 .pill{display:inline-block;padding:2px 9px;border-radius:20px;font-size:11.5px;font-weight:500}
 .available{background:#123d2a;color:var(--avail)}.busy{background:#3a2e10;color:var(--busy)}
 .charging{background:#0f2f3d;color:var(--charging)}.fault{background:#3d1522;color:var(--fault)}
 .matched{background:#0f2f3d;color:var(--matched)}.completed{background:#221f3d;color:var(--completed)}
 .queued{background:#1b2233;color:var(--queued)}.unmatched{background:#3d1522;color:var(--unmatched)}
 .warning{background:#3a2e10;color:var(--busy)}.critical{background:#3d1522;color:var(--fault)}
 .mono{font-variant-numeric:tabular-nums}
 small{color:var(--muted)}
 svg{display:block;width:100%;height:64px}
 .empty{color:var(--muted);font-size:12.5px;padding:6px 0}
</style>
<header>
 <span style="font-size:20px">🚕</span>
 <h1>Driverless Taxi — Fleet Operator Dashboard</h1>
 <div class=live><span class=dot id=dot></span><span id=t>connecting…</span></div>
</header>
<div class=wrap>
 <div class=kpis id=kpis></div>
 <div class=card>
   <h2>Telemetry throughput — last 15 min (msgs/min)</h2>
   <div id=spark class=empty>waiting for data…</div>
 </div>
 <div class=grid>
   <div class=card>
     <h2>Fleet status</h2>
     <div class=bar id=vbar></div>
     <div class=legend id=vlegend></div>
   </div>
   <div class=card>
     <h2>Trips by status</h2>
     <div class=bar id=tbar></div>
     <div class=legend id=tlegend></div>
   </div>
   <div class="card span2">
     <h2>Recent trips</h2>
     <table><thead><tr><th>Trip</th><th>Vehicle</th><th>Status</th><th style=text-align:right>Requested</th></tr></thead>
     <tbody id=trips></tbody></table>
   </div>
   <div class="card span2">
     <h2>Recent alerts</h2>
     <table><thead><tr><th>Vehicle</th><th>Type</th><th>Severity</th><th style=text-align:right>Time</th></tr></thead>
     <tbody id=alerts></tbody></table>
   </div>
 </div>
</div>
<script>
const C={available:'--avail',busy:'--busy',charging:'--charging',fault:'--fault',
 matched:'--matched',completed:'--completed',queued:'--queued',unmatched:'--unmatched'};
const cvar=k=>getComputedStyle(document.documentElement).getPropertyValue(C[k]||'--muted').trim()||'#8891ad';
const badge=s=>'<span class="pill '+s+'">'+s+'</span>';
const ago=d=>{const s=(Date.now()-new Date(d))/1000|0;if(s<60)return s+'s ago';
 if(s<3600)return (s/60|0)+'m ago';return (s/3600|0)+'h ago';};
const sum=o=>Object.values(o).reduce((a,b)=>a+b,0);

function stackBar(barEl,legendEl,obj){
 const total=sum(obj)||1;
 const entries=Object.entries(obj).sort((a,b)=>b[1]-a[1]);
 barEl.innerHTML=entries.map(([k,v])=>'<span style="width:'+(100*v/total)+'%;background:'+cvar(k)+'"></span>').join('');
 legendEl.innerHTML=entries.map(([k,v])=>
   '<div class=it><span class=sw style="background:'+cvar(k)+'"></span>'+k+'<b class=mono>'+v+'</b></div>').join('')
   ||'<span class=empty>none yet</span>';
}

const hexA=(hex,a)=>{const m=hex.replace('#','');const n=parseInt(m.length===3?m.replace(/(.)/g,'$1$1'):m,16);
 return 'rgba('+(n>>16&255)+','+(n>>8&255)+','+(n&255)+','+a+')';};
function sparkline(series){
 const el=document.getElementById('spark');
 if(!series.length){el.className='empty';el.textContent='waiting for data…';return;}
 const vals=series.map(p=>p.n),max=Math.max(...vals,1),W=100,H=64,n=series.length;
 const x=i=>n<2?W/2:i*W/(n-1),y=v=>H-3-(v/max)*(H-10);
 const line=n<2?('0,'+y(vals[0])+' '+W+','+y(vals[0])):vals.map((v,i)=>x(i)+','+y(v)).join(' ');
 const area='0,'+H+' '+line+' '+W+','+H;
 const c=cvar('matched');
 el.className='';
 el.innerHTML=
  '<svg viewBox="0 0 100 64" preserveAspectRatio="none">'+
  '<polygon points="'+area+'" fill="'+hexA(c,0.18)+'"></polygon>'+
  '<polyline points="'+line+'" fill="none" stroke="'+c+'" stroke-width="1.5" '+
   'vector-effect="non-scaling-stroke" stroke-linejoin="round"></polyline>'+
  '</svg>'+
  '<div style="display:flex;justify-content:space-between"><small>'+
   (n<2?'1 min so far':n+' min')+'</small>'+
  '<small class=mono>peak '+max+'/min · now '+vals[vals.length-1]+'/min</small></div>';
}

function kpis(d){
 const v=d.vehiclesByStatus,tr=d.tripsByStatus;
 const active=(tr.queued||0)+(tr.matched||0);
 const tiles=[
  ['Fleet size',d.vehicleCount,'vehicles',''],
  ['Available',v.available||0,'ready to dispatch','accent'],
  ['On trip',v.busy||0,'currently serving',''],
  ['Active bookings',active,'queued + matched',''],
  ['Completed',tr.completed||0,'trips finished',''],
  ['Open alerts',sum(d.alertsByType),(d.alertsByType.safety_fault||0)+' faults',''],
 ];
 document.getElementById('kpis').innerHTML=tiles.map(([l,n,s,a])=>
  '<div class=tile><div class=lbl>'+l+'</div><div class="num '+a+'">'+n+'</div><div class=sub>'+s+'</div></div>'
 ).join('');
}

async function tick(){
 const dot=document.getElementById('dot');
 try{
  const d=await (await fetch('/api/state')).json();
  kpis(d);
  stackBar(document.getElementById('vbar'),document.getElementById('vlegend'),d.vehiclesByStatus);
  stackBar(document.getElementById('tbar'),document.getElementById('tlegend'),d.tripsByStatus);
  sparkline(d.throughput);
  document.getElementById('trips').innerHTML=d.recentTrips.map(t=>
   '<tr><td class=mono><small>'+String(t._id).slice(-6)+'</small></td><td>'+(t.vehicleId||'—')+
   '</td><td>'+badge(t.status)+'</td><td style=text-align:right><small>'+ago(t.requested_at)+'</small></td></tr>'
  ).join('')||'<tr><td colspan=4 class=empty>no trips yet</td></tr>';
  document.getElementById('alerts').innerHTML=d.recentAlerts.map(a=>
   '<tr><td>'+a.vehicleId+'</td><td>'+badge(a.type&&a.type.includes('fault')?'fault':'charging')+
   '</td><td>'+badge(a.severity)+'</td><td style=text-align:right><small>'+ago(a.timestamp)+'</small></td></tr>'
  ).join('')||'<tr><td colspan=4 class=empty>no alerts</td></tr>';
  dot.className='dot';
  document.getElementById('t').textContent='live · '+new Date().toLocaleTimeString();
 }catch(e){
  dot.className='dot stale';
  document.getElementById('t').textContent='disconnected — retrying';
 }
}
tick();setInterval(tick,2000);
</script>`;

app.listen(PORT, () => console.log(`[dashboard] http://localhost:${PORT}`));
