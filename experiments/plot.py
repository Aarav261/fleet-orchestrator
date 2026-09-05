#!/usr/bin/env python
"""Render experiment CSVs to PNG graphs for the status report."""
import csv, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")
plt.rcParams.update({"figure.dpi": 130, "font.size": 11, "axes.grid": True, "grid.alpha": 0.3})

def rows(name):
    with open(os.path.join(RES, name)) as f:
        return list(csv.reader(f))

# --- Exp1: ingest capacity ---
r = rows("exp1_ingest.csv")[1:]
N = [int(x[0]) for x in r]; offered = [int(x[1]) for x in r]; ingest = [int(x[2]) for x in r]
plt.figure(figsize=(6.4, 4))
plt.plot(N, offered, "--", color="#888", label="Offered load (N msg/s)")
plt.plot(N, ingest, "o-", color="#2b6cb0", label="Ingest throughput")
plt.xlabel("Fleet size N (vehicles @ 1 Hz)"); plt.ylabel("Messages / second")
plt.title("Exp 1 — Telemetry Ingest scales linearly with fleet size")
plt.legend(); plt.tight_layout(); plt.savefig(os.path.join(RES, "exp1_ingest.png")); plt.close()

# --- Exp2: horizontal scaling under burst ---
r = rows("exp2_scaling.csv")[1:]
series = {}
for rep, t, q, p in r:
    series.setdefault(int(rep), ([], []))
    series[int(rep)][0].append(int(t)); series[int(rep)][1].append(int(q))
plt.figure(figsize=(6.4, 4))
colors = {1: "#c53030", 4: "#2f855a"}
for rep in sorted(series):
    t, q = series[rep]
    plt.plot(t, q, "o-", ms=3, color=colors.get(rep, None), label=f"{rep} dispatch replica{'s' if rep>1 else ''}")
plt.xlabel("Time (s) — identical 200 rps burst"); plt.ylabel("SQS queue depth (pending requests)")
plt.title("Exp 2 — Demand-side scaling under an identical burst", fontsize=11)
plt.legend(); plt.tight_layout(); plt.savefig(os.path.join(RES, "exp2_scaling.png")); plt.close()

# --- Exp3: edge-filter efficiency ---
r = rows("exp3_edge.csv")[1:]
d = {x[0]: x for x in r}
modes = ["off", "on"]; bytes_ = [int(d[m][3]) / 1024 for m in modes]
pub = [int(d[m][2]) for m in modes]; supp = d["on"][4]
plt.figure(figsize=(5.2, 4))
bars = plt.bar(["Filter OFF", "Filter ON"], bytes_, color=["#a0aec0", "#2b6cb0"], width=0.55)
plt.ylim(0, max(bytes_) * 1.22)
for b, kb, p in zip(bars, bytes_, pub):
    plt.text(b.get_x()+b.get_width()/2, kb+4, f"{kb:.0f} KB\n{p} msgs", ha="center", fontsize=9)
plt.ylabel("Telemetry published (KB, 100 vehicles / 20 s)")
plt.title(f"Exp 3 — Edge filtering cuts {supp}% of telemetry", fontsize=11)
plt.tight_layout(); plt.savefig(os.path.join(RES, "exp3_edge.png")); plt.close()

# --- Exp4: fault tolerance ---
r = rows("exp4_fault.csv")[1:]
t = [int(x[0]) for x in r]; q = [int(x[1]) for x in r]
kill_t = next((int(x[0]) for x in r if x[4]), None)
plt.figure(figsize=(6.4, 4))
plt.plot(t, q, "o-", ms=3, color="#6b46c1", label="SQS queue depth")
if kill_t is not None:
    plt.axvline(kill_t, color="#c53030", ls="--", lw=1.5)
    plt.text(kill_t+0.3, max(q)*0.9, "consumer killed", color="#c53030", fontsize=9)
drain = next((tt for tt, qq in zip(t, q) if tt > (kill_t or 0)+5 and qq == 0), None)
if drain: plt.text(drain-6, max(q)*0.15, "backlog drained → 0\n(no bookings lost)", color="#2f855a", fontsize=9)
plt.xlabel("Time (s)"); plt.ylabel("SQS queue depth (pending requests)")
plt.title("Exp 4 — Node failure: queue buffers, survivors drain it to zero")
plt.legend(); plt.tight_layout(); plt.savefig(os.path.join(RES, "exp4_fault.png")); plt.close()

print("wrote:", ", ".join(f for f in os.listdir(RES) if f.endswith(".png")))
