//! Custom Topology Module
//!
//! Provides Rust-native precision calculations for user-defined topologies.
//! Uses Q64.64 fixed-point arithmetic for cluster-safe, deterministic results.
//!
//! Architecture:
//! - Lua: Atomic persistence (ValKey storage)
//! - Rust: Precision calculations (distance, similarity, discovery ranking)

use crate::geometric_precision::{FixedPoint, FixedVector};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Query type for dimension matching
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum QueryType {
    #[default]
    Equality,
    Minimum,
    Maximum,
    Range,
    Informational,
}


/// Dimension configuration in a custom topology
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionConfig {
    pub index: usize,
    #[serde(default)]
    pub query_type: QueryType,
    #[serde(default)]
    pub values: HashMap<String, f64>,
}

/// Entity stored in a custom topology
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomEntity {
    pub id: String,
    pub point: Vec<f64>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub registered_at: Option<i64>,
}

/// Custom topology structure loaded from ValKey
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomTopology {
    pub dimensions: usize,
    #[serde(default)]
    pub capability_dimensions: HashMap<String, usize>,
    #[serde(default)]
    pub query_types: HashMap<String, String>,
    #[serde(default)]
    pub values: HashMap<String, HashMap<String, f64>>,
    #[serde(default)]
    pub services: HashMap<String, CustomEntity>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub schema_version: Option<String>,
}

/// Discovery requirement
#[derive(Debug, Clone)]
pub struct Requirement {
    pub dimension_index: usize,
    pub query_type: QueryType,
    pub value: Option<FixedPoint>,
    pub min: Option<FixedPoint>,
    pub max: Option<FixedPoint>,
}

/// Discovery result with Q64.64 precision score
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryResult {
    pub id: String,
    pub score: f64,
    pub point: Vec<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    pub distance: f64,
}

impl CustomTopology {
    /// Load from JSON string (from ValKey)
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Convert entity point to FixedVector for precision calculations
    fn point_to_fixed(&self, point: &[f64]) -> FixedVector {
        let f32_coords: Vec<f32> = point
            .iter()
            .map(|&v| v as f32)
            .collect();
        FixedVector::from_f32_slice(&f32_coords)
    }

    /// Calculate Euclidean distance using Q64.64 fixed-point
    /// This is cluster-safe - produces identical results on any node
    pub fn distance_precise(&self, point1: &[f64], point2: &[f64]) -> f64 {
        if point1.len() != point2.len() {
            debug!(
                "Dimension mismatch in distance_precise: point1 has {} dimensions, point2 has {}",
                point1.len(), point2.len()
            );
            return f64::MAX;
        }

        let v1 = self.point_to_fixed(point1);
        let v2 = self.point_to_fixed(point2);

        v1.distance_to(&v2).to_f64()
    }

    /// Calculate Manhattan distance using Q64.64 fixed-point
    pub fn manhattan_distance_precise(&self, point1: &[f64], point2: &[f64]) -> f64 {
        if point1.len() != point2.len() {
            debug!(
                "Dimension mismatch in manhattan_distance_precise: point1 has {} dimensions, point2 has {}",
                point1.len(), point2.len()
            );
            return f64::MAX;
        }

        let v1 = self.point_to_fixed(point1);
        let v2 = self.point_to_fixed(point2);

        {
            let len = v1.len().min(v2.len());
            let mut sum = FixedPoint::from_int(0);
            for i in 0..len {
                sum = sum + (v1[i] - v2[i]).abs();
            }
            sum.to_f64()
        }
    }

    /// Calculate similarity score (inverse distance) using Q64.64
    pub fn similarity_precise(&self, point1: &[f64], point2: &[f64]) -> f64 {
        let distance = self.distance_precise(point1, point2);
        if distance < 0.0001 {
            1.0 // Perfect match
        } else {
            1.0 / (1.0 + distance)
        }
    }

    /// Parse query type from string
    fn parse_query_type(&self, dim_name: &str) -> QueryType {
        self.query_types
            .get(dim_name)
            .map(|s| match s.as_str() {
                "minimum" => QueryType::Minimum,
                "maximum" => QueryType::Maximum,
                "range" => QueryType::Range,
                "informational" => QueryType::Informational,
                _ => QueryType::Equality,
            })
            .unwrap_or(QueryType::Equality)
    }

    /// Translate human-readable value to numeric using Q64.64
    fn translate_value(&self, dim_name: &str, value: &serde_json::Value) -> Option<FixedPoint> {
        match value {
            serde_json::Value::Number(n) => {
                Some(FixedPoint::from_f64(n.as_f64().unwrap_or(0.0)))
            }
            serde_json::Value::String(s) => {
                // Look up in values map
                self.values
                    .get(dim_name)
                    .and_then(|dim_values| dim_values.get(s))
                    .map(|&v| FixedPoint::from_f64(v))
                    .or_else(|| s.parse::<f64>().ok().map(FixedPoint::from_f64))
            }
            _ => None,
        }
    }

    /// Discover entities using Q64.64 precision for all calculations
    /// Returns results sorted by score (cluster-safe ordering)
    pub fn discover_precise(
        &self,
        requirements: &serde_json::Value,
        max_results: usize,
        include_metadata: bool,
    ) -> Vec<DiscoveryResult> {
        // Parse requirements into Requirement structs with FixedPoint values
        let mut parsed_reqs: Vec<Requirement> = Vec::new();

        if let serde_json::Value::Object(req_map) = requirements {
            for (dim_name, req_value) in req_map {
                if let Some(&dim_index) = self.capability_dimensions.get(dim_name) {
                    let query_type = self.parse_query_type(dim_name);

                    let mut req = Requirement {
                        dimension_index: dim_index,
                        query_type,
                        value: None,
                        min: None,
                        max: None,
                    };

                    // Handle complex constraints {min: X, max: Y}
                    if let serde_json::Value::Object(constraint) = req_value {
                        if let Some(min_val) = constraint.get("min") {
                            req.min = self.translate_value(dim_name, min_val);
                            req.query_type = QueryType::Minimum;
                        }
                        if let Some(max_val) = constraint.get("max") {
                            req.max = self.translate_value(dim_name, max_val);
                            req.query_type = QueryType::Maximum;
                        }
                    } else {
                        req.value = self.translate_value(dim_name, req_value);
                    }

                    parsed_reqs.push(req);
                }
            }
        }

        // Tolerance for equality checks (in Q64.64)
        let tolerance = FixedPoint::from_f64(0.001);

        // Find matching entities
        let mut matches: Vec<DiscoveryResult> = Vec::new();

        for (entity_id, entity) in &self.services {
            let point = &entity.point;
            let fixed_point = self.point_to_fixed(point);

            let mut is_match = true;
            let mut score = FixedPoint::from_f64(0.0);

            for req in &parsed_reqs {
                if req.dimension_index >= point.len() {
                    continue;
                }

                let entity_value = fixed_point[req.dimension_index];

                match req.query_type {
                    QueryType::Equality => {
                        if let Some(ref target) = req.value {
                            let diff = (entity_value - *target).abs();
                            if diff > tolerance {
                                is_match = false;
                                break;
                            }
                        }
                    }
                    QueryType::Minimum => {
                        let threshold = req.min.or(req.value).unwrap_or(FixedPoint::from_f64(0.0));
                        if entity_value < threshold - tolerance {
                            is_match = false;
                            break;
                        }
                        score = score + entity_value;
                    }
                    QueryType::Maximum => {
                        let threshold = req.max.or(req.value).unwrap_or(FixedPoint::from_f64(1.0));
                        if entity_value > threshold + tolerance {
                            is_match = false;
                            break;
                        }
                        // For maximum constraints, lower is better
                        score = score + (FixedPoint::from_f64(1.0) - entity_value);
                    }
                    QueryType::Range => {
                        let min_val = req.min.unwrap_or(FixedPoint::from_f64(0.0));
                        let max_val = req.max.unwrap_or(FixedPoint::from_f64(1.0));
                        if entity_value < min_val - tolerance || entity_value > max_val + tolerance {
                            is_match = false;
                            break;
                        }
                    }
                    QueryType::Informational => {
                        // No filtering, just informational
                    }
                }
            }

            if is_match {
                // Calculate distance from origin for tie-breaking
                let origin: Vec<f64> = vec![0.0; point.len()];
                let distance = self.distance_precise(point, &origin);

                matches.push(DiscoveryResult {
                    id: entity_id.clone(),
                    score: score.to_f64(),
                    point: point.clone(),
                    metadata: if include_metadata {
                        Some(entity.metadata.clone())
                    } else {
                        None
                    },
                    distance,
                });
            }
        }

        // Sort by score (descending) using FixedPoint-derived values
        // This ensures cluster-safe ordering
        matches.sort_by(|a, b| {
            // Primary: score (descending)
            let score_cmp = b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal);
            if score_cmp != std::cmp::Ordering::Equal {
                return score_cmp;
            }
            // Secondary: distance (ascending)
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Limit results
        matches.truncate(max_results);
        matches
    }

    /// Find k-nearest neighbors using Q64.64 distance
    pub fn knn_precise(&self, query_point: &[f64], k: usize) -> Vec<DiscoveryResult> {
        let mut results: Vec<DiscoveryResult> = self.services
            .iter()
            .map(|(id, entity)| {
                let distance = self.distance_precise(&entity.point, query_point);
                DiscoveryResult {
                    id: id.clone(),
                    score: 1.0 / (1.0 + distance), // Similarity score
                    point: entity.point.clone(),
                    metadata: Some(entity.metadata.clone()),
                    distance,
                }
            })
            .collect();

        // Sort by distance (Q64.64 precision)
        results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });

        results.truncate(k);
        results
    }

    /// Calculate centroid of a set of entities using Q64.64
    pub fn centroid_precise(&self, entity_ids: &[String]) -> Option<Vec<f64>> {
        if entity_ids.is_empty() {
            return None;
        }

        let dim_count = self.dimensions;
        let mut sum: Vec<FixedPoint> = vec![FixedPoint::from_f64(0.0); dim_count];
        let mut count = 0;

        for id in entity_ids {
            if let Some(entity) = self.services.get(id) {
                for (i, &val) in entity.point.iter().enumerate() {
                    if i < dim_count {
                        sum[i] = sum[i] + FixedPoint::from_f64(val);
                    }
                }
                count += 1;
            }
        }

        if count == 0 {
            return None;
        }

        let count_fp = FixedPoint::from_f64(count as f64);
        let centroid: Vec<f64> = sum
            .iter()
            .map(|&s| (s / count_fp).to_f64())
            .collect();

        Some(centroid)
    }

    /// Calculate variance across entities for a dimension using Q64.64
    pub fn dimension_variance(&self, dimension_index: usize) -> f64 {
        if self.services.is_empty() || dimension_index >= self.dimensions {
            return 0.0;
        }

        // Calculate mean
        let mut sum = FixedPoint::from_f64(0.0);
        let mut count = 0;

        for entity in self.services.values() {
            if dimension_index < entity.point.len() {
                sum = sum + FixedPoint::from_f64(entity.point[dimension_index]);
                count += 1;
            }
        }

        if count == 0 {
            return 0.0;
        }

        let mean = sum / FixedPoint::from_f64(count as f64);

        // Calculate variance
        let mut variance_sum = FixedPoint::from_f64(0.0);
        for entity in self.services.values() {
            if dimension_index < entity.point.len() {
                let val = FixedPoint::from_f64(entity.point[dimension_index]);
                let diff = val - mean;
                variance_sum = variance_sum + diff * diff;
            }
        }

        (variance_sum / FixedPoint::from_f64(count as f64)).to_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_precise() {
        let topology = CustomTopology {
            dimensions: 3,
            capability_dimensions: HashMap::new(),
            query_types: HashMap::new(),
            values: HashMap::new(),
            services: HashMap::new(),
            metadata: serde_json::Value::Null,
            schema_version: Some("test".to_string()),
        };

        let p1 = vec![0.0, 0.0, 0.0];
        let p2 = vec![1.0, 0.0, 0.0];

        let dist = topology.distance_precise(&p1, &p2);
        assert!((dist - 1.0).abs() < 0.0001);

        let p3 = vec![1.0, 1.0, 1.0];
        let dist2 = topology.distance_precise(&p1, &p3);
        let expected = (3.0_f64).sqrt();
        assert!((dist2 - expected).abs() < 0.001);
    }

    #[test]
    fn test_cluster_safe_determinism() {
        let topology = CustomTopology {
            dimensions: 5,
            capability_dimensions: HashMap::new(),
            query_types: HashMap::new(),
            values: HashMap::new(),
            services: HashMap::new(),
            metadata: serde_json::Value::Null,
            schema_version: Some("test".to_string()),
        };

        // Run same calculation 1000 times - must be identical
        let p1 = vec![0.123456, 0.789012, 0.345678, 0.901234, 0.567890];
        let p2 = vec![0.987654, 0.321098, 0.765432, 0.109876, 0.543210];

        let first_result = topology.distance_precise(&p1, &p2);

        for _ in 0..1000 {
            let result = topology.distance_precise(&p1, &p2);
            assert_eq!(result, first_result, "Results must be identical for cluster safety");
        }
    }
}
