# Changelog

All notable changes to this project are documented here. The format follows Keep a Changelog.

## [0.2.0] — unreleased

### Breaking: the one topology

- The constellation is one topology: one schema for every shape (node, component, site, app, daemon, placement), declared by name, queried by layer, facet and axis word.
- Registration, relay, load sampling and health run on the constellation; the legacy numeric capability and tier-schema paths and the in-memory topology engine are removed.
- Custom-topology, unified-topology and geometric store/load/distance commands are removed; the command schema shrinks to match.
- Nodes declare themselves; GNODE_CONSTELLATIONS names the constellations a node serves.
- The manifest `gnode:` block is the declaration; the CLI registers a site on its host and deregister removes it from the topology.
- Extensions load against a topology-free handler signature.

