#!/bin/bash
# geodineum status — the constellation at a glance; the one status route.
#
#   geodineum status                 every constellation this node serves: nodes, resources,
#                                    what runs where, services (--constellation <c>: just one)
#   geodineum status <id>            one entity: what it is, where it runs, its health,
#                                    intent, heartbeats, and this host's registration files
#   geodineum status --json          the same facts, one JSON object per constellation per line
#   (the installer CLI adds: status <component> · status --config · status --visitors)
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
MODE=table ID=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            cat <<'HLP'
Usage: geodineum status [<id> | <component> | --config | --visitors] [--json] [--constellation <c>]

  (none)          every constellation this node serves: nodes (heartbeat age, load,
                  memory, what runs where), services (shape, env, health, placements,
                  heartbeat), components, visitor counters
  <id>            one service, site, node or component
  <component>     that component's own status (comms, gflow, gschedule, bak, geodeploy, ...)
  --config        this host's configuration health (the 7-layer validator; --fix, --verbose)
  --visitors      per-site visitor counters
  --json          machine-shaped; one object per constellation per line
HLP
            exit 0 ;;
        --json) MODE=json ;;
        -*) echo "status: unknown option $1 (see --help)" >&2; exit 2 ;;
        *) [[ -z "$ID" ]] || { echo "status: one id at a time" >&2; exit 2; }; ID="$1" ;;
    esac
    shift
done

# Every served constellation unless one was named; each rendered by this same script.
if [[ -z "$ID" && -z "${GNODE_TOPOLOGY_NAMESPACE:-}" ]]; then
    mapfile -t _cs < <(gnode_cli_constellations)
    if [[ ${#_cs[@]} -gt 1 ]]; then
        _rc=0 _first=1
        for _c in "${_cs[@]}"; do
            [[ "$MODE" == table && $_first -eq 0 ]] && echo ""
            _first=0
            _a=(); [[ "$MODE" == json ]] && _a=(--json)
            GNODE_STATUS_INNER=1 GNODE_TOPOLOGY_NAMESPACE="$_c" "$0" "${_a[@]}" || _rc=1
        done
        if [[ "$MODE" != json ]] && command -v geodineum >/dev/null 2>&1; then
            echo ""; geodineum status --visitors --compact 2>/dev/null || true
        fi
        exit $_rc
    fi
fi
NS="$(gnode_cli_constellation)"

vk() { VALKEY_USER=gnode_daemon "$VCLI" "$@" 2>/dev/null; }

T=$(mktemp -d); trap 'rm -rf "$T"' EXIT

# ── status <id>: one entity, in the constellation that holds it ──────────────
if [[ -n "$ID" ]]; then
    if [[ -n "${GNODE_TOPOLOGY_NAMESPACE:-}" ]]; then _cs=("$GNODE_TOPOLOGY_NAMESPACE"); else mapfile -t _cs < <(gnode_cli_constellations); fi
    : > "$T/intents"; : > "$T/hb"; HOLDER="" ALSO=""
    for c in "${_cs[@]}"; do
        if [[ "$(vk HEXISTS "{$c}:gnode:constellation:entities" "$ID")" == 1 ]]; then
            if [[ -z "$HOLDER" ]]; then
                HOLDER="$c"
                vk HGETALL "{$c}:gnode:constellation:entities" > "$T/ents"
            else
                ALSO+="{$c} "
            fi
        fi
        _i="$(vk HGET "{$c}:gnode:registrations" "$ID")"
        [[ -n "$_i" ]] && printf '%s\t%s\n' "$c" "$_i" >> "$T/intents"
        while IFS= read -r k; do
            [[ -n "$k" ]] && printf '%s\t%s\n' "$k" "$(vk GET "$k" | tr -d '\n')" >> "$T/hb"
        done < <(vk --scan --pattern "{$c}:gnode:heartbeat:*:${ID}:*"; vk --scan --pattern "{$c}:gnode:heartbeat:*:*:${ID}")
    done
    [[ -f "$T/ents" ]] || : > "$T/ents"
    CRED="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}/valkey_client_${ID}.password"
    LEDGER="${GEODINEUM_REGISTRATIONS_DIR:-/etc/geodineum/registrations}/${ID}.tsv"
    HOST_CRED=0; [[ -e "$CRED" ]] && HOST_CRED=1
    HOST_LEDGER=-1; [[ -r "$LEDGER" ]] && HOST_LEDGER="$(grep -c . "$LEDGER" || true)"
    rc=0
    MODE="$MODE" ID="$ID" HOLDER="$HOLDER" ALSO="${ALSO% }" SERVED="${_cs[*]}" HOST_CRED="$HOST_CRED" HOST_LEDGER="$HOST_LEDGER" CRED="$CRED" LEDGER="$LEDGER" \
        python3 - "$T" <<'PYEOF' || rc=$?
import json, os, sys, time
T = sys.argv[1]; now = int(time.time())
ID, holder = os.environ["ID"], os.environ["HOLDER"]
def pairs(path):
    l = [x.rstrip("\n") for x in open(path)]; l = [x for x in l if x != ""]
    return dict(zip(l[::2], l[1::2]))
def dec(raw):
    try: return json.loads(raw)
    except Exception: return {}
ents = {k: dec(v) for k, v in pairs(f"{T}/ents").items()}
SHAPES = {"1": "node", "2": "component", "3.1": "site", "3.2": "app", "3.3": "daemon", "4.1": "component@node", "4.2": "service@node"}
HEALTH = {0.0: "unknown", 0.25: "dead", 0.5: "degraded", 1.0: "healthy"}
def slot(e, i):
    pd = e.get("pd") or []; return pd[i] if len(pd) > i else None
def health(e):
    v = slot(e, 17); return None if v is None else HEALTH.get(round(float(v), 4), str(v))
e = ents.get(ID, {})
sh = str(e.get("sh", ""))
pl = [{"id": k, "shape": SHAPES.get(str(p.get("sh")), p.get("sh")), "of": p.get("of"), "on": p.get("on"), "health": health(p)}
      for k, p in sorted(ents.items()) if str(p.get("sh", "")).startswith("4") and ID in (p.get("of"), p.get("on"))]
intents = []
for line in open(f"{T}/intents"):
    c, raw = line.rstrip("\n").split("\t", 1); intents.append({"constellation": c, **dec(raw)})
hb = []
for line in open(f"{T}/hb"):
    if "\t" not in line: continue
    k, v = line.rstrip("\n").split("\t", 1); seg = k.split(":"); d = dec(v)
    if len(seg) < 6: continue
    hb.append({"key": k, "env": seg[3], "comp": seg[4], "node": ":".join(seg[5:]),
               "age_s": max(0, now - int(d["ts"])) if d.get("ts") else None})
also = os.environ.get("ALSO", "")
out = {"id": ID, "constellation": holder or None, "also_in": [a.strip("{}") for a in also.split()], "shape": SHAPES.get(sh, sh or None), "env": e.get("env"),
       "health": health(e) if e else None, "load": slot(e, 16) if e else None,
       "placements": pl, "intents": intents, "heartbeats": hb,
       "host": {"credential_file": os.environ["CRED"] if os.environ["HOST_CRED"] == "1" else None,
                "ledger_entries": None if os.environ["HOST_LEDGER"] == "-1" else int(os.environ["HOST_LEDGER"])}}
if not e and not intents and not hb:
    if os.environ["MODE"] == "json": print(json.dumps(out))
    else: print(f"no entity, intent or heartbeat for '{ID}' in {os.environ['SERVED']}")
    sys.exit(1)
if os.environ["MODE"] == "json":
    print(json.dumps(out)); sys.exit(0)
age = lambda a: "—" if a is None else (f"{a}s" if a < 120 else f"{a//60}m (stale)")
print(f"{ID}  {out['shape'] or 'not on the topology'}{' in {' + holder + '}' if holder else ''}{' (also ' + also + ')' if also else ''}"
      f"  env {out['env'] or '—'}  health {out['health'] or '—'}  load {out['load'] if out['load'] is not None else '—'}")
if pl:
    print("  placements  " + "  ".join(f"{p['id']}:{p['health'] or '?'}" for p in pl))
else:
    print("  placements  —" + ("  (a site is placed on the node that serves it; an app or daemon by its heartbeat)" if sh.startswith("3") else ""))
for i in intents:
    print(f"  intent      {{{i['constellation']}}} shape {i.get('shape') or '—'} profile {i.get('profile') or '—'}"
          f" env {i.get('environment') or '—'}{' on ' + i['on'] if i.get('on') else ''}  by {i.get('declared_by') or '—'}")
if not intents and sh.startswith("3"):
    print("  intent      — (none recorded: re-register to record it)")
for h in hb:
    print(f"  heartbeat   {h['comp']}@{h['node']} ({h['env']}) {age(h['age_s'])}")
host = out["host"]
print(f"  this host   credential {'present' if host['credential_file'] else '—'} · "
      f"ledger {'—' if host['ledger_entries'] is None else str(host['ledger_entries']) + ' entr' + ('y' if host['ledger_entries'] == 1 else 'ies')}")
PYEOF
    if [[ "$MODE" != json && $rc -eq 0 ]] && command -v geodineum >/dev/null 2>&1; then
        geodineum status --visitors "$ID" --compact 2>/dev/null || true
    fi
    exit "$rc"
fi

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
# The site registry is estate-wide; a registered id no served constellation holds is
# reported once, not rendered as an empty row in every constellation.
: > "$T/orphans"
if [[ -z "${GNODE_STATUS_INNER:-}" || "$NS" == "$(GNODE_TOPOLOGY_NAMESPACE= gnode_cli_constellations | head -1)" ]]; then
    mapfile -t _served < <(gnode_cli_constellations)
    while IFS= read -r sid; do
        [[ -n "$sid" ]] || continue
        _held=0
        for c in "${_served[@]}"; do
            [[ "$(vk HEXISTS "{$c}:gnode:constellation:entities" "$sid")" == 1 || "$(vk HEXISTS "{$c}:gnode:registrations" "$sid")" == 1 ]] && { _held=1; break; }
        done
        (( _held )) || echo "$sid" >> "$T/orphans"
    done < <(vk SMEMBERS "gnode:sites:registry")
fi

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
orphans = sorted({l.strip() for l in open(f"{T}/orphans") if l.strip()})
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
for sid in sorted(set(services) | set(intents)):
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
       "component_list": [{"component": cid, "health": health_of(e),
                           "on": sorted({p.get("on") for p in placements.values() if p.get("sh") == "4.1" and p.get("of") == cid} - {None})}
                          for cid, e in sorted(components.items())],
       "unplaced_registry": orphans,
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
if out["component_list"]:
    print(f"\n{'COMPONENTS':<20}{'HEALTH':<9}ON")
    for c in out["component_list"]:
        print(f"{c['component']:<20}{c['health'] or '—':<9}{' '.join(c['on']) or '—'}")
# Grouped by the topology's shapes (3.1 site, 3.2 app, 3.3 daemon); an id with only an
# intent is grouped by its declared profile, the way registration would shape it.
BY_PROFILE = {"web": "site", "headless": "app", "service": "daemon", "system": "daemon", "component": "daemon"}
GROUPS = [("site", "SITES"), ("app", "APPS"), ("daemon", "DAEMONS")]
group_of = lambda s: s["shape"] or BY_PROFILE.get(s["profile"] or "", "other")
for g, title in GROUPS + [("other", "NOT ON THE TOPOLOGY")]:
    rows_g = [s for s in out["services"] if group_of(s) == g]
    if not rows_g: continue
    print(f"\n{title:<20}{'ENV':<12}{'HEALTH':<9}{'ENTITY':<10}{'PLACED ON':<24}HB")
    for s in rows_g:
        where = f"{age(s['hb_age_s'])}@{s['hb_node']}" if s["hb_node"] else "—"
        pl = " ".join(f"{p['node']}:{p['health'] or '?'}" for p in s["placements"]) or "—"
        print(f"{s['service']:<20}{s['env'] or '—':<12}{s['health'] or '—':<9}"
              f"{'✓' if s['entity'] else '✗ intent':<10}{pl:<24}{where}")
if orphans:
    print(f"\nin the site registry, in no served constellation: {' '.join(orphans)}")
PYEOF

# Visitor counters (beacon-fed aggregates) — the fold lives in the installer
# CLI (`geodineum status --visitors`); ride along here best-effort, never fatally.
if [[ "$MODE" != "json" && -z "${GNODE_STATUS_INNER:-}" ]] && command -v geodineum >/dev/null 2>&1; then
    geodineum status --visitors --compact 2>/dev/null || true
fi
