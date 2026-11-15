# Changelog

All notable changes to this project are documented here. The format follows Keep a Changelog.

## [0.2.0] — unreleased

### Security: tenants

- The caller is the stream a command came from. A tenant may register and deregister only itself, reads only its own streams, and has no named topologies or channels. CMS state and templates are per tenant. A relayed reply needs the grant its command left. Templates have no `get_env`.
- A joined node's ValKey login may not write the Lua libraries (`FUNCTION LOAD`, `DELETE`, `FLUSH`, `RESTORE`); the master's keeps `LOAD`. A client login has no `SCAN` unless its manifest declares `keyspace_scan: true` or it is a site (`scripts/acl-tiers.sh`).

### Signals

- With Pro gNode-SIGNALS loaded, the writer-lease holder publishes scale-out (the sampler's suggestion) and sickness (a placement whose heartbeat went dead) to `{c}:signals:global:stream`: one entry when a condition starts, one when it clears. Without the library the daemon publishes nothing and says so once.

### Registration

- A registration or a deploy announced on `{c}:gnode:broadcast:global` (`site_registered` from `onboard-service.sh`, `component_deployed` from the installer's deploy sweep) makes the daemon re-read its site discovery and subscribe at once instead of at the next minute; the poll stays and a lost announcement costs at most one interval.
- `register-tools --tier tool` records each component's intent in `{c}:gnode:registrations:components`; the reconciler reports a component that goes missing or changes shape, as it does for sites and services. `deregister-service.sh` removes either intent.
- The reconciler takes the shape an intent names; a site or service onboarded with the `component` profile (a daemon) is no longer reported as drift.

### Faster

- One wait over all streams instead of a short block per stream: a command is picked up at once, however many sites there are.

### Breaking: the one topology

- The constellation is one topology: one schema for every shape (node, component, site, app, daemon, placement), declared by name, queried by layer, facet and axis word.
- Registration, relay, load sampling and health run on the constellation; the legacy numeric capability and tier-schema paths and the in-memory topology engine are removed.
- Custom-topology, unified-topology and geometric store/load/distance commands are removed; the command schema shrinks to match.
- Nodes declare themselves; GNODE_CONSTELLATIONS names the constellations a node serves.
- The manifest `gnode:` block is the declaration; the CLI registers a site on its host and deregister removes it from the topology.
- Extensions load against a topology-free handler signature.

