#!/bin/bash
# geodineum status — the constellation at a glance.
#
#   geodineum status            # table: nodes, resources, what runs where, services
#   geodineum status --json     # same facts, machine-shaped
#
# Reads only ValKey (nodes publish everything; no SSH, no host inspection):
# constellation entities, the heartbeat family (CONTRACTS/heartbeat.md — the
# daemon's carries la1/cores/mem), registration intents, the one topology by layer.
# Agents don't run this: they send `constellation_status` to the unified
# stream and the daemon serves the same JSON under its own credentials.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLI="$(dirname "$SCRIPT_DIR")/valkey-cli-secure.sh"
source "$(dirname "${BASH_SOURCE[0]}")/constellation.sh"
NS="$(gnode_cli_constellation)"
MODE=table
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    cat <<'HLP'
Usage: geodineum status [--json]

The constellation at a glance: nodes (heartbeat age, load, memory, what runs
where), services (shape, env, health, placements, heartbeat), components, and per-site
visitor counters. Reads only ValKey. --json emits the same facts for machines.
HLP
    exit 0
fi
[[ "${1:-}" == "--json" ]] && MODE=json

vk() { VALKEY_USER=gnode_daemon "$VCLI" "$@" 2>/dev/null; }

T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
TK="{${NS}}:gnode:constellation"
# One read of the constellation: every entity of every shape, decoded by shape below.
vk HGETALL "${TK}:entities" > "$T/ents" || true
vk HGETALL "{${NS}}:gnode:registrations" > "$T/reg" || true

# The scan must fail LOUDLY. With stderr swallowed, an ACL that denies SCAN
# renders a confident empty table — "0 live components" — indistinguishable
# from a quiet estate, which is worse than no status command at all.
if ! VALKEY_USER=gnode_daemon "$VCLI" --scan --pattern "{${NS}}:gnode:heartbeat:*" > "$T/hbkeys" 2> "$T/scanerr"; then
    echo "status: heartbeat scan failed as gnode_daemon:" >&2
    cat "$T/scanerr" >&2
    echo "status: refusing to render a table that would read as 'nothing running'" >&2
    exit 1
fi
: > "$T/hb"
while IFS= read -r k; do
    [[ -n "$k" ]] || continue
    printf '%s\t%s\n' "$k" "$(vk GET "$k" | tr -d '\n')" >> "$T/hb"
done < "$T/hbkeys"
vk SMEMBERS "gnode:sites:registry" > "$T/sites" || true

MODE="$MODE" NS="$NS" python3 - "$T" <<'PYEOF'
import json, os, sys, time
T = sys.argv[1]
now = int(time.time())

def pairs(path):
    lines = [l.rstrip("\n") for l in open(path)] if os.path.exists(path) else []
    lines = [l for l in lines if l != ""]
    return dict(zip(lines[::2], lines[1::2]))

def dec(raw):
    try: return json.loads(raw)
    except Exception: return {}

ents = {k: dec(v) for k, v in pairs(f"{T}/ents").items()}
intents = {k: dec(v) for k, v in pairs(f"{T}/reg").items()}
sites = {l.strip() for l in open(f"{T}/sites") if l.strip()} if os.path.exists(f"{T}/sites") else set()
HEALTH = {0.0: "unknown", 0.25: "dead", 0.5: "degraded", 1.0: "healthy"}
SHAPES = {"1": "node", "2": "component", "3.1": "site", "3.2": "app", "3.3": "daemon", "4.1": "component@node", "4.2": "service@node"}
def slot(e, i):
    pd = e.get("pd") or []
    return pd[i] if len(pd) > i else None
def health_of(e):
    v = slot(e, 17)
    return None if v is None else HEALTH.get(round(float(v), 4), str(v))

layer = lambda e: str(e.get("sh", ""))[:1]
nodes_reg = {k: e for k, e in ents.items() if e.get("sh") == "1"}
components = {k: e for k, e in ents.items() if e.get("sh") == "2"}
services = {k: e for k, e in ents.items() if layer(e) == "3"}
placements = {k: e for k, e in ents.items() if layer(e) == "4"}

# Heartbeats: key = {ns}:gnode:heartbeat:<env>:<comp>:<node>
hb = []
for line in open(f"{T}/hb"):
    if "\t" not in line: continue
    k, v = line.rstrip("\n").split("\t", 1)
    seg = k.split(":")
    if len(seg) < 6: continue
    env, comp, node = seg[3], seg[4], ":".join(seg[5:])
    d = dec(v)
    hb.append({"env": env, "comp": comp, "node": node,
               "age_s": max(0, now - int(d.get("ts", 0))) if d.get("ts") else None,
               "la1": d.get("la1"), "cores": d.get("cores"),
               "mem_used_mb": d.get("mem_used_mb"), "mem_total_mb": d.get("mem_total_mb")})

nodes = {}
for nid, e in nodes_reg.items():
    nodes[nid] = {"node": nid, "health": health_of(e), "load": slot(e, 16), "hb_age_s": None, "la1": None, "cores": None,
                  "mem_used_mb": None, "mem_total_mb": None, "runs": []}
for h in hb:
    n = nodes.setdefault(h["node"], {"node": h["node"], "health": None, "load": None, "hb_age_s": None, "la1": None,
                                     "cores": None, "mem_used_mb": None, "mem_total_mb": None, "runs": []})
    if h["comp"] not in n["runs"]:
        n["runs"].append(h["comp"])
    if h["age_s"] is not None and (n["hb_age_s"] is None or h["age_s"] < n["hb_age_s"]):
        n["hb_age_s"] = h["age_s"]
    if h["comp"] == "gnode-daemon":                  # the node authority carries resources
        for f in ("la1", "cores", "mem_used_mb", "mem_total_mb"):
            n[f] = h.get(f)

# A service's placements (4.2, `of` = the service) carry its health per node.
placed = {}
for pid, p in placements.items():
    if p.get("sh") == "4.2":
        placed.setdefault(p.get("of"), []).append({"node": p.get("on"), "health": health_of(p), "load": slot(p, 16), "weight": p.get("w")})

rows = []
for sid in sorted(set(services) | sites | set(intents)):
    e = services.get(sid, {})
    it = intents.get(sid, {})
    mine = [h for h in hb if h["comp"] == sid and h["age_s"] is not None]
    best = min(mine, key=lambda h: h["age_s"]) if mine else None
    rows.append({"service": sid,
                 "shape": SHAPES.get(str(e.get("sh")), e.get("sh")) if e else None,
                 "profile": (e.get("m") or {}).get("type") or it.get("profile"),
                 "env": e.get("env") or it.get("environment"),
                 "entity": bool(e),
                 "health": health_of(e) if e else None,
                 "load": slot(e, 16) if e else None,
                 "placements": placed.get(sid, []),
                 "hb_age_s": best["age_s"] if best else None,
                 "hb_node": best["node"] if best else None})

out = {"constellation": os.environ.get("NS", "geodineum"), "ts": now,
       "nodes": sorted(nodes.values(), key=lambda n: n["node"]),
       "services": rows,
       "components": len(components),
       "placements": len(placements),
       "components_live": sum(1 for h in hb if h["age_s"] is not None and h["age_s"] <= 120)}

if os.environ.get("MODE") == "json":
    print(json.dumps(out)); sys.exit(0)

def age(a):  return "—" if a is None else (f"{a}s" if a < 120 else f"{a//60}m!")
def mem(u, t):
    if u is None or t is None: return "—"
    return f"{u/1024:.1f}/{t/1024:.0f}G"
def load(l, c): return "—" if l is None else f"{l}/{c or '?'}c"

print(f"{out['constellation']} constellation · {time.strftime('%Y-%m-%d %H:%MZ', time.gmtime(now))}"
      f" · nodes {len(out['nodes'])} · components {out['components']} · services {len(services)} · placements {out['placements']} · live heartbeats {out['components_live']}")
print(f"{'NODE':<17}{'HEALTH':<9}{'HB':<6}{'LOAD':<10}{'MEM':<12}RUNS")
for n in out["nodes"]:
    print(f"{n['node']:<17}{n['health'] or '—':<9}{age(n['hb_age_s']):<6}{load(n['la1'], n['cores']):<10}"
          f"{mem(n['mem_used_mb'], n['mem_total_mb']):<12}{' '.join(n['runs']) or '—'}")
print(f"{'SERVICE':<20}{'SHAPE':<8}{'ENV':<12}{'HEALTH':<9}{'ENTITY':<8}{'PLACED ON':<24}HB")
for s in out["services"]:
    where = f"{age(s['hb_age_s'])}@{s['hb_node']}" if s["hb_node"] else "—"
    pl = " ".join(f"{p['node']}:{p['health'] or '?'}" for p in s["placements"]) or "—"
    print(f"{s['service']:<20}{s['shape'] or '—':<8}{s['env'] or '—':<12}{s['health'] or '—':<9}"
          f"{'✓' if s['entity'] else '✗':<8}{pl:<24}{where}")
PYEOF

# Visitor counters (beacon-fed aggregates) — the fold lives in the installer
# CLI (`geodineum visitors`); ride along here best-effort, never fatally.
if [[ "$MODE" != "json" ]] && command -v geodineum >/dev/null 2>&1; then
    geodineum visitors --compact 2>/dev/null || true
fi
