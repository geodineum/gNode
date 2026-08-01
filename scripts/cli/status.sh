#!/bin/bash
# geodineum status — the constellation at a glance.
#
#   geodineum status            # table: nodes, resources, what runs where, services
#   geodineum status --json     # same facts, machine-shaped
#
# Reads only ValKey (nodes publish everything; no SSH, no host inspection):
# constellation entities, the heartbeat family (CONTRACTS/heartbeat.md — the
# daemon's carries la1/cores/mem), registration intents, the tool catalogue.
# Agents don't run this: they send `constellation_status` to the unified
# stream and the daemon serves the same JSON under its own credentials.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLI="$(dirname "$SCRIPT_DIR")/valkey-cli-secure.sh"
NS="${GNODE_TOPOLOGY_NAMESPACE:-geodineum}"
MODE=table
[[ "${1:-}" == "--json" ]] && MODE=json

vk() { VALKEY_USER=gnode_daemon "$VCLI" "$@" 2>/dev/null; }

T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
vk HGETALL "{${NS}}:gnode:constellation:entities" > "$T/ents"   || true
vk HGETALL "{${NS}}:gnode:registrations"          > "$T/reg"    || true
vk HLEN    "{ecosystem}:gnode:services:entities"  > "$T/tools"  || true
: > "$T/hb"
while IFS= read -r k; do
    [[ -n "$k" ]] || continue
    printf '%s\t%s\n' "$k" "$(vk GET "$k" | tr -d '\n')" >> "$T/hb"
done < <(vk --scan --pattern "{${NS}}:gnode:heartbeat:*")
: > "$T/svc_ent"
while IFS= read -r site; do
    [[ -n "$site" ]] || continue
    printf '%s\t%s\n' "$site" "$(vk HEXISTS "{${site}}:gnode:services:entities" "$site" | tr -d '[:space:]')" >> "$T/svc_ent"
done < <(awk 'NR % 2 == 1' "$T/reg")

MODE="$MODE" NS="$NS" python3 - "$T" <<'PYEOF'
import json, os, sys, time
T = sys.argv[1]
now = int(time.time())

def pairs(path):
    lines = [l.rstrip("\n") for l in open(path)] if os.path.exists(path) else []
    lines = [l for l in lines if l != ""]
    return dict(zip(lines[::2], lines[1::2]))

nodes_reg = pairs(f"{T}/ents")           # node_id -> entity json
intents   = pairs(f"{T}/reg")            # site   -> intent json
svc_ent = {}
for line in open(f"{T}/svc_ent"):
    if "\t" in line:
        s, v = line.rstrip("\n").split("\t", 1); svc_ent[s] = v == "1"
try:
    tools = int(open(f"{T}/tools").read().strip() or 0)
except Exception:
    tools = 0

# Heartbeats: key = {ns}:gnode:heartbeat:<env>:<comp>:<node> (flat legacy = 5 segs)
hb = []
for line in open(f"{T}/hb"):
    if "\t" not in line: continue
    k, v = line.rstrip("\n").split("\t", 1)
    seg = k.split(":")
    if len(seg) < 6: continue                       # legacy flat key — ignore
    env, comp, node = seg[3], seg[4], ":".join(seg[5:])
    try: d = json.loads(v)
    except Exception: d = {}
    hb.append({"env": env, "comp": comp, "node": node,
               "age_s": max(0, now - int(d.get("ts", 0))) if d.get("ts") else None,
               "la1": d.get("la1"), "cores": d.get("cores"),
               "mem_used_mb": d.get("mem_used_mb"), "mem_total_mb": d.get("mem_total_mb")})

nodes = {}
for nid in nodes_reg:
    nodes[nid] = {"node": nid, "hb_age_s": None, "la1": None, "cores": None,
                  "mem_used_mb": None, "mem_total_mb": None, "runs": []}
for h in hb:
    n = nodes.setdefault(h["node"], {"node": h["node"], "hb_age_s": None, "la1": None,
                                     "cores": None, "mem_used_mb": None, "mem_total_mb": None, "runs": []})
    if h["comp"] not in n["runs"]:
        n["runs"].append(h["comp"])
    if h["age_s"] is not None and (n["hb_age_s"] is None or h["age_s"] < n["hb_age_s"]):
        n["hb_age_s"] = h["age_s"]
    if h["comp"] == "gnode-daemon":                  # the node authority carries resources
        for f in ("la1", "cores", "mem_used_mb", "mem_total_mb"):
            n[f] = h.get(f)

services = []
for site, raw in sorted(intents.items()):
    try: it = json.loads(raw)
    except Exception: it = {}
    mine = [h for h in hb if h["comp"] == site and h["age_s"] is not None]
    best = min(mine, key=lambda h: h["age_s"]) if mine else None
    services.append({"service": site,
                     "profile": it.get("profile"), "env": it.get("environment"),
                     "entity": svc_ent.get(site, False),
                     "hb_age_s": best["age_s"] if best else None,
                     "hb_node": best["node"] if best else None})

out = {"constellation": os.environ.get("NS", "geodineum"), "ts": now,
       "nodes": sorted(nodes.values(), key=lambda n: n["node"]),
       "services": services,
       "tools": tools,
       "components_live": sum(1 for h in hb if h["age_s"] is not None and h["age_s"] <= 120)}

if os.environ.get("MODE") == "json":
    print(json.dumps(out)); sys.exit(0)

def age(a):  return "—" if a is None else (f"{a}s" if a < 120 else f"{a//60}m!")
def mem(u, t):
    if u is None or t is None: return "—"
    return f"{u/1024:.1f}/{t/1024:.0f}G"
def load(l, c): return "—" if l is None else f"{l}/{c or '?'}c"

print(f"{out['constellation']} constellation · {time.strftime('%Y-%m-%d %H:%MZ', time.gmtime(now))}"
      f" · nodes {len(out['nodes'])} · tools {tools} · live components {out['components_live']}")
print(f"{'NODE':<17}{'HB':<6}{'LOAD':<10}{'MEM':<12}RUNS")
for n in out["nodes"]:
    print(f"{n['node']:<17}{age(n['hb_age_s']):<6}{load(n['la1'], n['cores']):<10}"
          f"{mem(n['mem_used_mb'], n['mem_total_mb']):<12}{' '.join(n['runs']) or '—'}")
print(f"{'SERVICE':<14}{'PROFILE':<10}{'ENV':<12}{'ENTITY':<8}HB")
for s in out["services"]:
    where = f"{age(s['hb_age_s'])}@{s['hb_node']}" if s["hb_node"] else "—"
    print(f"{s['service']:<14}{s['profile'] or '?':<10}{s['env'] or '?':<12}"
          f"{'✓' if s['entity'] else '✗':<8}{where}")
PYEOF
