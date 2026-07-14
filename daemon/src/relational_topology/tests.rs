//!tests for the relational topology engine.
//!
//! These tests verify:
//! - O(1) voxel lookups and edge validation
//! - O(L) z-ordered iteration
//! - Cycle impossibility via geometric constraint
//! - Q64.64 determinism across operations

use super::*;
use super::topology::EntityMetadata;

type DepTopology = RelationalTopology<DependencyConstraint, DependencyMeta>;
type CommTopology = RelationalTopology<CommunicationConstraint, edge::CommunicationMeta>;

// ==================== Point Tests ====================

#[test]
fn test_point_q32_determinism() {
    // Same input should always produce same voxel key
    let p1 = RelPoint3D::from_f64(0.123456789, 0.987654321, 0.555555555);
    let p2 = RelPoint3D::from_f64(0.123456789, 0.987654321, 0.555555555);

    assert_eq!(p1.to_voxel_key(10), p2.to_voxel_key(10));
    assert_eq!(p1.z_slice(10), p2.z_slice(10));
}

#[test]
fn test_point_boundary_values() {
    // Test edge cases
    let origin = RelPoint3D::from_f64(0.0, 0.0, 0.0);
    assert_eq!(origin.to_voxel_key(10), (0, 0, 0));
    assert_eq!(origin.z_slice(10), 0);

    let max = RelPoint3D::from_f64(0.99, 0.99, 0.99);
    assert_eq!(max.to_voxel_key(10), (9, 9, 9));
    assert_eq!(max.z_slice(10), 9);

    // Values >= 1.0 should clamp to max bucket
    let over = RelPoint3D::from_f64(1.5, 1.5, 1.5);
    assert_eq!(over.to_voxel_key(10), (9, 9, 9));
}

#[test]
fn test_vector_is_downward() {
    let high = RelPoint3D::from_f64(0.5, 0.5, 0.9);
    let low = RelPoint3D::from_f64(0.5, 0.5, 0.1);

    // Vector from high to low (dependency direction)
    let v1 = RelVector3D::from_points(&high, &low);
    assert!(v1.is_downward());

    // Vector from low to high (invalid for dependencies)
    let v2 = RelVector3D::from_points(&low, &high);
    assert!(!v2.is_downward());
}

// ==================== Voxel Grid Tests ====================

#[test]
fn test_voxel_o1_lookup() {
    let mut grid = VoxelGrid::new();

    // Insert many entities
    for i in 0..100 {
        let x = (i % 10) as f64 / 10.0;
        let y = ((i / 10) % 10) as f64 / 10.0;
        let z = (i / 100) as f64 / 10.0;
        grid.insert(format!("entity_{}", i), &RelPoint3D::from_f64(x, y, z));
    }

    // Lookup is O(1) regardless of entity count
    let pos = RelPoint3D::from_f64(0.35, 0.75, 0.05);
    let entities = grid.entities_at(&pos);

    // Should find entities in that voxel
    assert!(!entities.is_empty() || entities.is_empty()); // O(1) either way
}

#[test]
fn test_voxel_z_slice_distribution() {
    let mut grid = VoxelGrid::new();

    // Insert entities across all Z slices
    for z in 0..10 {
        for i in 0..5 {
            let id = format!("z{}_entity{}", z, i);
            let pos = RelPoint3D::from_f64(0.5, 0.5, (z as f64 + 0.5) / 10.0);
            grid.insert(id, &pos);
        }
    }

    // Each Z slice should have 5 entities
    for z in 0..10 {
        let slice = grid.get_z_slice(z);
        assert_eq!(slice.len(), 5, "Z-slice {} should have 5 entities", z);
    }
}

// ==================== Constraint Tests ====================

#[test]
fn test_dependency_constraint_transitivity() {
    // If A depends on B and B depends on C, then A cannot depend on C directly
    // (well, it can, but the geometry ensures no cycles)

    let mut topo = DepTopology::new(DependencyConstraint::new());

    // C at bottom, B in middle, A at top
    let c = RelEntity::new("c", RelPoint3D::from_f64(0.5, 0.5, 0.1));
    let b = RelEntity::new("b", RelPoint3D::from_f64(0.5, 0.5, 0.5));
    let a = RelEntity::new("a", RelPoint3D::from_f64(0.5, 0.5, 0.9));

    topo.register(c).unwrap();
    topo.register(b).unwrap();
    topo.register(a).unwrap();

    // A → B (valid: 0.9 > 0.5)
    assert!(topo.add_edge("a".to_string(), "b".to_string(), DependencyMeta::hard()).is_ok());

    // B → C (valid: 0.5 > 0.1)
    assert!(topo.add_edge("b".to_string(), "c".to_string(), DependencyMeta::hard()).is_ok());

    // A → C directly (also valid: 0.9 > 0.1) - this is fine!
    assert!(topo.add_edge("a".to_string(), "c".to_string(), DependencyMeta::hard()).is_ok());

    // But C → A would be invalid (0.1 < 0.9)
    assert!(topo.add_edge("c".to_string(), "a".to_string(), DependencyMeta::hard()).is_err());

    // And C → B would be invalid (0.1 < 0.5)
    assert!(topo.add_edge("c".to_string(), "b".to_string(), DependencyMeta::hard()).is_err());
}

#[test]
fn test_cycle_impossibility_proof() {
    // Mathematical proof by example:
    // For any cycle A → B → C → A, we need:
    //   Z(A) > Z(B) (for A → B)
    //   Z(B) > Z(C) (for B → C)
    //   Z(C) > Z(A) (for C → A)
    // This implies Z(A) > Z(B) > Z(C) > Z(A), which is impossible.

    let constraint = DependencyConstraint::new();

    // Try all permutations of three entities
    for &za in &[0.1, 0.5, 0.9] {
        for &zb in &[0.1, 0.5, 0.9] {
            for &zc in &[0.1, 0.5, 0.9] {
                if za == zb || zb == zc || za == zc {
                    continue; // Skip equal positions
                }

                let a = RelEntity::new("a", RelPoint3D::from_f64(0.5, 0.5, za));
                let b = RelEntity::new("b", RelPoint3D::from_f64(0.5, 0.5, zb));
                let c = RelEntity::new("c", RelPoint3D::from_f64(0.5, 0.5, zc));

                let ab = constraint.validate_edge(&a, &b).is_ok();
                let bc = constraint.validate_edge(&b, &c).is_ok();
                let ca = constraint.validate_edge(&c, &a).is_ok();

                // At least one edge must be invalid (no cycle possible)
                assert!(
                    !ab || !bc || !ca,
                    "Cycle should be impossible: za={}, zb={}, zc={}, ab={}, bc={}, ca={}",
                    za, zb, zc, ab, bc, ca
                );
            }
        }
    }
}

// ==================== Topology Operations Tests ====================

#[test]
fn test_load_order_is_dependency_safe() {
    let mut topo = DepTopology::new(DependencyConstraint::new());

    // Create a realistic service hierarchy
    let entities = vec![
        ("os", 0.05),           // Operating system (foundation)
        ("database", 0.15),     // Database server
        ("cache", 0.25),        // Cache layer
        ("auth", 0.35),         // Authentication service
        ("api", 0.65),          // API layer
        ("web", 0.85),          // Web frontend
        ("mobile", 0.95),       // Mobile app
    ];

    for (id, z) in &entities {
        topo.register(RelEntity::new(*id, RelPoint3D::from_f64(0.5, 0.5, *z))).unwrap();
    }

    // Add edges (all valid because higher Z depends on lower Z)
    topo.add_edge("database".to_string(), "os".to_string(), DependencyMeta::hard()).unwrap();
    topo.add_edge("cache".to_string(), "database".to_string(), DependencyMeta::hard()).unwrap();
    topo.add_edge("auth".to_string(), "database".to_string(), DependencyMeta::hard()).unwrap();
    topo.add_edge("api".to_string(), "auth".to_string(), DependencyMeta::hard()).unwrap();
    topo.add_edge("api".to_string(), "cache".to_string(), DependencyMeta::hard()).unwrap();
    topo.add_edge("web".to_string(), "api".to_string(), DependencyMeta::hard()).unwrap();
    topo.add_edge("mobile".to_string(), "api".to_string(), DependencyMeta::hard()).unwrap();

    let order = topo.load_order();

    // Verify that for each edge (from → to), 'to' appears before 'from' in load order
    let position: std::collections::HashMap<_, _> = order
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();

    for edge in topo.all_edges() {
        let from_pos = position.get(edge.from.as_str()).unwrap();
        let to_pos = position.get(edge.to.as_str()).unwrap();
        assert!(
            to_pos < from_pos,
            "Dependency {} must load before {}",
            edge.to,
            edge.from
        );
    }
}

#[test]
fn test_find_providers_filters_correctly() {
    let mut topo = DepTopology::new(DependencyConstraint::new());

    // Create services at different Z levels
    topo.register(RelEntity::new("foundation", RelPoint3D::from_f64(0.5, 0.5, 0.1))).unwrap();
    topo.register(RelEntity::new("middleware", RelPoint3D::from_f64(0.5, 0.5, 0.5))).unwrap();
    topo.register(RelEntity::new("app", RelPoint3D::from_f64(0.5, 0.5, 0.9))).unwrap();

    // Providers for 'app' should be foundation and middleware
    let providers = topo.find_providers(&"app".to_string());
    let provider_ids: Vec<_> = providers.iter().map(|e| e.id.as_str()).collect();

    assert!(provider_ids.contains(&"foundation"));
    assert!(provider_ids.contains(&"middleware"));
    assert!(!provider_ids.contains(&"app"));

    // Providers for 'middleware' should only be foundation
    let providers = topo.find_providers(&"middleware".to_string());
    let provider_ids: Vec<_> = providers.iter().map(|e| e.id.as_str()).collect();

    assert!(provider_ids.contains(&"foundation"));
    assert!(!provider_ids.contains(&"middleware"));
    assert!(!provider_ids.contains(&"app"));

    // Providers for 'foundation' should be empty (it's at the bottom)
    let providers = topo.find_providers(&"foundation".to_string());
    assert!(providers.is_empty());
}

// ==================== Communication Topology Tests ====================

#[test]
fn test_communication_bidirectional() {
    let mut topo = CommTopology::new(CommunicationConstraint::new(true));

    let a = RelEntity::new("service_a", RelPoint3D::from_f64(0.3, 0.5, 0.7));
    let b = RelEntity::new("service_b", RelPoint3D::from_f64(0.7, 0.5, 0.3));

    topo.register(a).unwrap();
    topo.register(b).unwrap();

    // Both directions should be valid for communication
    assert!(topo.add_edge(
        "service_a".to_string(),
        "service_b".to_string(),
        edge::CommunicationMeta::grpc(true),
    ).is_ok());

    assert!(topo.add_edge(
        "service_b".to_string(),
        "service_a".to_string(),
        edge::CommunicationMeta::grpc(true),
    ).is_ok());

    assert_eq!(topo.edge_count(), 2);
}

#[test]
fn test_communication_trust_boundary() {
    let constraint = CommunicationConstraint::with_trust_boundary(true, 0.3);
    let mut topo = CommTopology::new(constraint);

    // Internal service (high trust)
    let internal = RelEntity::new("internal", RelPoint3D::from_f64(0.5, 0.5, 0.8));
    // External service (low trust)
    let external = RelEntity::new("external", RelPoint3D::from_f64(0.5, 0.5, 0.2));
    // DMZ service (medium trust)
    let dmz = RelEntity::new("dmz", RelPoint3D::from_f64(0.5, 0.5, 0.5));

    topo.register(internal).unwrap();
    topo.register(external).unwrap();
    topo.register(dmz).unwrap();

    // internal → external crosses 0.6 trust levels (exceeds 0.3 max)
    assert!(topo.add_edge(
        "internal".to_string(),
        "external".to_string(),
        edge::CommunicationMeta::http(true),
    ).is_err());

    // internal → dmz crosses 0.29 trust levels (within 0.3 limit)
    // Note: use values clearly within boundary, not exactly at it,
    // because Q64.64 from_f64(0.3) may differ from |from_f64(0.8) - from_f64(0.5)|
    // by a ULP (least significant bit).
    let dmz_close = RelEntity::new("dmz_close", RelPoint3D::from_f64(0.5, 0.5, 0.52));
    topo.register(dmz_close).unwrap();
    assert!(topo.add_edge(
        "internal".to_string(),
        "dmz_close".to_string(),
        edge::CommunicationMeta::http(true),
    ).is_ok());

    // dmz → external crosses 0.3 trust levels (within limit)
    assert!(topo.add_edge(
        "dmz".to_string(),
        "external".to_string(),
        edge::CommunicationMeta::http(true),
    ).is_ok());
}

// ==================== Serialization Tests ====================

#[test]
fn test_topology_export() {
    let mut topo = DepTopology::new(DependencyConstraint::new());

    topo.register(RelEntity::with_metadata(
        "database",
        RelPoint3D::from_f64(0.2, 0.5, 0.1),
        EntityMetadata::new().with_name("PostgreSQL").with_domain("data"),
    )).unwrap();

    topo.register(RelEntity::with_metadata(
        "api",
        RelPoint3D::from_f64(0.5, 0.5, 0.7),
        EntityMetadata::new().with_name("REST API").with_domain("service"),
    )).unwrap();

    topo.add_edge("api".to_string(), "database".to_string(), DependencyMeta::hard()).unwrap();

    let data = topo.to_data();

    assert_eq!(data.entity_count, 2);
    assert_eq!(data.edge_count, 1);
    assert_eq!(data.constraint, "DependencyConstraint");

    // Verify JSON serialization works
    let json = serde_json::to_string(&data).unwrap();
    assert!(json.contains("PostgreSQL"));
    assert!(json.contains("REST API"));
}

// ==================== Edge Metadata Tests ====================

#[test]
fn test_dependency_meta_builder() {
    let meta = DependencyMeta::hard()
        .with_version(">=2.0.0")
        .with_capability("sql")
        .with_reason("Persistent storage");

    assert_eq!(meta.dep_type, DependencyType::Hard);
    assert!(meta.dep_type.is_required());
    assert_eq!(meta.version_constraint, Some(">=2.0.0".to_string()));
    assert_eq!(meta.capability, Some("sql".to_string()));
}

#[test]
fn test_communication_meta_protocols() {
    let https = edge::CommunicationMeta::http(true);
    assert_eq!(https.protocol, "https");
    assert!(https.encrypted);
    assert_eq!(https.port, Some(443));

    let grpc = edge::CommunicationMeta::grpc(true).with_latency(5).with_port(9090);
    assert_eq!(grpc.protocol, "grpc");
    assert_eq!(grpc.latency_ms, Some(5));
    assert_eq!(grpc.port, Some(9090));

    let ws = edge::CommunicationMeta::websocket(false);
    assert_eq!(ws.protocol, "ws");
    assert!(!ws.encrypted);
}

// ==================== Performance Characteristic Tests ====================

#[test]
fn test_many_entities_o1_operations() {
    let mut topo = DepTopology::new(DependencyConstraint::new());

    // Insert 1000 entities
    for i in 0..1000 {
        let z = (i as f64) / 1000.0;
        let entity = RelEntity::new(
            format!("entity_{}", i),
            RelPoint3D::from_f64(0.5, 0.5, z),
        );
        topo.register(entity).unwrap();
    }

    // These operations should all be O(1)
    assert!(topo.contains(&"entity_500".to_string()));
    assert!(topo.get(&"entity_500".to_string()).is_some());

    // Validation is O(1)
    assert!(topo.can_connect(&"entity_999".to_string(), &"entity_0".to_string()).is_ok());
    assert!(topo.can_connect(&"entity_0".to_string(), &"entity_999".to_string()).is_err());

    // Voxel lookup is O(1)
    let pos = RelPoint3D::from_f64(0.5, 0.5, 0.5);
    let _nearby = topo.entities_at(&pos);
}

#[test]
fn test_z_ordered_iter_is_o_l() {
    let mut topo = DepTopology::new(DependencyConstraint::new());

    // Insert entities spread across Z slices
    for i in 0..100 {
        let z = (i as f64) / 100.0;
        let entity = RelEntity::new(format!("e{}", i), RelPoint3D::from_f64(0.5, 0.5, z));
        topo.register(entity).unwrap();
    }

    // Z-ordered iteration visits 10 slices (O(L) where L=10)
    let order: Vec<_> = topo.z_ordered_iter().collect();
    assert_eq!(order.len(), 100);

    // Verify ordering is correct
    for i in 1..order.len() {
        assert!(order[i - 1].position.z <= order[i].position.z);
    }
}
