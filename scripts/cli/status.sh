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
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    cat <<'HLP'
Usage: geodineum status [--json]

The constellation at a glance: nodes (heartbeat age, load, memory, what runs
where), services (profile, env, entity, heartbeat), tool count, and per-site
visitor counters. Reads only ValKey. --json emits the same facts for machines.
HLP
    exit 0
fi
[[ "${1:-}" == "--json" ]] && MODE=json

vk() { VALKEY_USER=gnode_daemon "$VCLI" "$@" 2>/dev/null; }

T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
vk HGETALL "{${NS}}:gnode:constellation:entities" > "$T/ents"   || true
vk HGETALL "{${NS}}:gnode:registrations"          > "$T/reg"    || true
vk HLEN    "{ecosystem}:gnode:services:entities"  > "$T/tools"  || true

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
# Services = UNION of the sites registry (every onboarded site — the ten
# WordPress sites predate the intent mechanism) and the intent hash. A site
# IS a service; listing only intents silently hid most of production.
vk SMEMBERS "gnode:sites:registry" > "$T/sites" || true
vk HGET "{${NS}}:gnode:schema:service" dimension_values > "$T/dimvals" || true
: > "$T/svc_ent"
{ cat "$T/sites"; awk 'NR % 2 == 1' "$T/reg"; } | sort -u | while IFS= read -r site; do
    [[ -n "$site" ]] || continue
    printf '%s\t%s\n' "$site" "$(vk HGET "{${site}}:gnode:services:entities" "$site" | tr -d '\n')" >> "$T/svc_ent"
done

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
svc_ent = {}                              # site -> entity json string ('' = absent)
for line in open(f"{T}/svc_ent"):
    if "\t" in line:
        s, v = line.rstrip("\n").split("\t", 1); svc_ent[s] = v
try:
    dimvals = json.load(open(f"{T}/dimvals"))
except Exception:
    dimvals = {}
env_vocab = dimvals.get("environment", {})   # label -> float, from the PUBLISHED schema

def env_label(x):
    if x is None: return None
    best = min(env_vocab.items(), key=lambda kv: abs(kv[1] - x), default=None)
    return best[0] if best and abs(best[1] - x) < 0.05 else f"{x:g}"
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
for site in sorted(set(svc_ent) | set(intents)):
    try: it = json.loads(intents.get(site, "") or "{}")
    except Exception: it = {}
    ent_raw = svc_ent.get(site, "")
    env = it.get("environment")
    if env is None and ent_raw:
        # Legacy site (no intent): environment from the stored entity's own
        # capability map, labeled via the published schema vocabulary.
        try:
            env = env_label(json.loads(ent_raw).get("c", {}).get("environment"))
        except Exception:
            env = None
    mine = [h for h in hb if h["comp"] == site and h["age_s"] is not None]
    best = min(mine, key=lambda h: h["age_s"]) if mine else None
    services.append({"service": site,
                     "profile": it.get("profile"), "env": env,
                     "entity": bool(ent_raw),
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
print(f"{'SERVICE':<20}{'PROFILE':<10}{'ENV':<12}{'ENTITY':<8}HB")
for s in out["services"]:
    where = f"{age(s['hb_age_s'])}@{s['hb_node']}" if s["hb_node"] else "—"
    print(f"{s['service']:<20}{s['profile'] or '—':<10}{s['env'] or '—':<12}"
          f"{'✓' if s['entity'] else '✗':<8}{where}")
PYEOF

# Visitor counters (beacon-fed aggregates) — the fold lives in the installer
# CLI (`geodineum visitors`); ride along here best-effort, never fatally.
if [[ "$MODE" != "json" ]] && command -v geodineum >/dev/null 2>&1; then
    geodineum visitors --compact 2>/dev/null || true
fi
