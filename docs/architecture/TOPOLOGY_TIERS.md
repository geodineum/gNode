# The One Topology

A constellation has one topology: `{<c>}:gnode:constellation` (`<c>` defaults to `geodineum`). Every entity of the constellation is one record in it. The layer is a coordinate, not a separate store.

## Shapes

| Shape | Layer | Declared by |
|-------|-------|-------------|
| `1` | node | installer |
| `2` | component | installer (`register-components.sh`) |
| `3.1` | site | manifest `gnode:` block or `register_service` |
| `3.2` | app | same |
| `3.3` | daemon | same |
| `4.1` | placement of a component on a node | daemon, from heartbeats |
| `4.2` | placement of a service on a node | daemon, from `register_service` with `on` |

A shape names its own axes and their vocabularies (`daemon/config/topology_schema.yaml`, published per shape under `<tk>:schema:<shape>`). A layer and its sub-layers form one interval, so a query over layer `3` covers sites, apps and daemons.

## Declaration

A provider declares an entity by name: a shape, an environment, axis words (`protocol: resp3`, `latency_class: fast`) and metadata. The daemon encodes the words into coordinates and refuses a word it does not know; a number for an axis word is refused. Nothing outside the daemon computes a coordinate. The declaration lives in the component's manifest under a single `gnode:` block (`constellation`, `shape`, `heartbeat`, `holds`, `requires`, `fundamentality`, `capacity_weight`, `axes`) and reaches the daemon through `register_service`.

## Keys

| Key | Holds |
|-----|-------|
| `<tk>:entities` | id to record |
| `<tk>:meta` | counters and schema version |
| `<tk>:ix:<axis>` | ids scored by an axis (ranking and range queries) |
| `<tk>:c:<facet>:<value>` | ids holding a facet value |
| `<tk>:edges`, `<tk>:out:<id>`, `<tk>:in:<id>` | dependency edges |
| `<tk>:plof:<id>`, `<tk>:plon:<node>` | placements of an entity, placements on a node |
| `<tk>:pl:<service>` | the service's ring of active placements |
| `<tk>:holds:<id>`, `<tk>:req:<id>`, `<tk>:peers:<id>`, `<tk>:deny` | reach labels, declared peers, deny list |

Readers (gCore sites, gDash, the CLI) use plain reads; sites hold a read-only ACL. Every write goes through a daemon command.

## Derived state

The daemon holding the constellation's writer lease (`{<c>}:gnode:cluster:writer`) writes the derived axes: heartbeat becomes placement, placement becomes health, health and measured load decide ring membership. Edges are accepted only when the dependency potential strictly decreases from caller to target.

## Routing and permission

A relayed command resolves once over the target service's ring (exact weighted round-robin over healthy active placements) and is addressed to the chosen placement. Before it is forwarded, `GNODE_TOPO_CAN_REACH` decides: the target sits below the caller or is a declared peer, the environment matches or is global, the target's `requires` labels are a subset of the caller's `holds`, and no live deny exists. Anything else is refused; there is no default allow. Operators add and remove denies with `geodineum gnode deny` and `undeny`. See `INTER_SERVICE_ROUTING.md`.

## Named topologies (Pro)

`{<c>}:gnode:topo:<name>` with the catalog `{<c>}:gnode:topo:catalog`: independent topologies built on the same library (gNode-TOPO).

## DTAP environment detection

`dtap_schema.yaml` maps domain prefixes to environments:
- `staging.example.com` is staging (ViewKey-gated)
- `dev.example.com` is development (ViewKey-gated)
- `example.com` is production (public)

Schema-driven with i18n aliases (Dutch: `acceptatie` is acceptance).

## Files

| File | Purpose |
|------|---------|
| `daemon/config/topology_schema.yaml` | Shapes, axes, vocabularies |
| `daemon/config/dtap_schema.yaml` | DTAP prefix to environment mapping |
| `daemon/functions/gnode_topo.lua` | The topology library |
