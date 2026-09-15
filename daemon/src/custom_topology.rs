//! Custom Topology Module
//!
//! Q64.64 fixed-point calculations for user-defined topologies, identical on every node.
//!
//! A custom topology is one JSON document its owner stores with SET at `topology_key`;
//! the format is in COMMAND_SCHEMA.md. This module reads that document, never writes it.

use crate::geometric_precision::{FixedPoint, FixedVector};
use log::debug;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Query type for dimension matching
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
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

/// Custom topology document as stored at its key
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

/// A point in Q64.64, converted from f64 directly (an f32 step would drop precision).
fn to_fixed(point: &[f64]) -> FixedVector {
    FixedVector::from_slice(&point.iter().map(|&v| FixedPoint::from_f64(v)).collect::<Vec<_>>())
}

/// Euclidean distance in Q64.64; `f64::MAX` when the dimension counts differ.
pub fn fixed_distance(point1: &[f64], point2: &[f64]) -> f64 {
    if point1.len() != point2.len() {
        debug!(
            "Dimension mismatch in fixed_distance: point1 has {} dimensions, point2 has {}",
            point1.len(), point2.len()
        );
        return f64::MAX;
    }
    to_fixed(point1).distance_to(&to_fixed(point2)).to_f64()
}

impl CustomTopology {
    /// Load from JSON string (from ValKey)
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Calculate Euclidean distance using Q64.64 fixed-point
    pub fn distance_precise(&self, point1: &[f64], point2: &[f64]) -> f64 {
        fixed_distance(point1, point2)
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

        let v1 = to_fixed(point1);
        let v2 = to_fixed(point2);
        let mut sum = FixedPoint::from_int(0);
        for i in 0..v1.len() {
            sum = sum + (v1[i] - v2[i]).abs();
        }
        sum.to_f64()
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
                self.values
                    .get(dim_name)
                    .and_then(|dim_values| dim_values.get(s))
                    .map(|&v| FixedPoint::from_f64(v))
                    .or_else(|| s.parse::<f64>().ok().map(FixedPoint::from_f64))
            }
            _ => None,
        }
    }

    /// Filter and rank entities. `{min, max}` is a closed range, `{min}` and `{max}` are
    /// one-sided, a bare value is exact (also on a range axis). Unknown dimensions and
    /// untranslatable values are errors. Order: score, then distance from the origin,
    /// then id, so equal entities come back in the same order on every node.
    pub fn discover_precise(
        &self,
        requirements: &serde_json::Value,
        max_results: usize,
        include_metadata: bool,
    ) -> Result<Vec<DiscoveryResult>, String> {
        let req_map = requirements
            .as_object()
            .ok_or_else(|| "requirements must be an object of dimension name → constraint".to_string())?;

        let mut parsed_reqs: Vec<Requirement> = Vec::with_capacity(req_map.len());
        for (dim_name, req_value) in req_map {
            let dimension_index = *self
                .capability_dimensions
                .get(dim_name)
                .ok_or_else(|| format!("unknown dimension '{}'", dim_name))?;
            let translate = |v: &serde_json::Value| {
                self.translate_value(dim_name, v)
                    .ok_or_else(|| format!("dimension '{}' has no value {}", dim_name, v))
            };

            let mut req = Requirement {
                dimension_index,
                query_type: self.parse_query_type(dim_name),
                value: None,
                min: None,
                max: None,
            };

            if let serde_json::Value::Object(bounds) = req_value {
                req.min = bounds.get("min").map(&translate).transpose()?;
                req.max = bounds.get("max").map(&translate).transpose()?;
                req.query_type = match (req.min.is_some(), req.max.is_some()) {
                    (true, true) => QueryType::Range,
                    (true, false) => QueryType::Minimum,
                    (false, true) => QueryType::Maximum,
                    (false, false) => {
                        return Err(format!("bounds for '{}' need min, max or both", dim_name))
                    }
                };
            } else {
                req.value = Some(translate(req_value)?);
            }

            parsed_reqs.push(req);
        }

        let tolerance = FixedPoint::from_f64(0.001);
        let zero = FixedPoint::from_f64(0.0);
        let one = FixedPoint::from_f64(1.0);
        let mut matches: Vec<DiscoveryResult> = Vec::new();

        for (entity_id, entity) in &self.services {
            let fixed_point = to_fixed(&entity.point);
            let mut is_match = true;
            let mut score = zero;

            for req in &parsed_reqs {
                if req.dimension_index >= entity.point.len() {
                    is_match = false;
                    break;
                }

                let v = fixed_point[req.dimension_index];
                let within = |target: FixedPoint| (v - target).abs() <= tolerance;

                match req.query_type {
                    QueryType::Equality => {
                        if let Some(target) = req.value {
                            if !within(target) {
                                is_match = false;
                                break;
                            }
                        }
                    }
                    QueryType::Minimum => {
                        let threshold = req.min.or(req.value).unwrap_or(zero);
                        if v < threshold - tolerance {
                            is_match = false;
                            break;
                        }
                        score = score + v;
                    }
                    QueryType::Maximum => {
                        let threshold = req.max.or(req.value).unwrap_or(one);
                        if v > threshold + tolerance {
                            is_match = false;
                            break;
                        }
                        // For maximum constraints, lower is better
                        score = score + (one - v);
                    }
                    QueryType::Range => {
                        let inside = match (req.min, req.max, req.value) {
                            (None, None, Some(target)) => within(target),
                            (min, max, _) => {
                                let lo = min.unwrap_or(zero);
                                let hi = max.unwrap_or(one);
                                v >= lo - tolerance && v <= hi + tolerance
                            }
                        };
                        if !inside {
                            is_match = false;
                            break;
                        }
                    }
                    QueryType::Informational => {}
                }
            }

            if is_match {
                let origin = vec![0.0; entity.point.len()];
                matches.push(DiscoveryResult {
                    id: entity_id.clone(),
                    score: score.to_f64(),
                    point: entity.point.clone(),
                    metadata: include_metadata.then(|| entity.metadata.clone()),
                    distance: fixed_distance(&entity.point, &origin),
                });
            }
        }

        matches.sort_by(|a, b| {
            b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal)
                .then_with(|| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal))
                .then_with(|| a.id.cmp(&b.id))
        });
        matches.truncate(max_results);
        Ok(matches)
    }

    /// k nearest entities to `query_point` by Q64.64 distance; equal distances by id.
    pub fn knn_precise(&self, query_point: &[f64], k: usize) -> Result<Vec<DiscoveryResult>, String> {
        if query_point.len() != self.dimensions {
            return Err(format!(
                "query_point has {} dimensions; the topology has {}",
                query_point.len(), self.dimensions
            ));
        }

        let mut results: Vec<DiscoveryResult> = self.services
            .iter()
            .map(|(id, entity)| {
                let distance = fixed_distance(&entity.point, query_point);
                DiscoveryResult {
                    id: id.clone(),
                    score: 1.0 / (1.0 + distance),
                    point: entity.point.clone(),
                    metadata: Some(entity.metadata.clone()),
                    distance,
                }
            })
            .collect();

        results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        results.truncate(k);
        Ok(results)
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
    use serde_json::json;

    fn empty_topology(dimensions: usize) -> CustomTopology {
        CustomTopology {
            dimensions,
            capability_dimensions: HashMap::new(),
            query_types: HashMap::new(),
            values: HashMap::new(),
            services: HashMap::new(),
            metadata: serde_json::Value::Null,
            schema_version: Some("test".to_string()),
        }
    }

    /// Two axes: `level` (minimum) and `span` (range, with named values).
    fn topology(points: &[(&str, [f64; 2])]) -> CustomTopology {
        CustomTopology {
            capability_dimensions: HashMap::from([("level".to_string(), 0), ("span".to_string(), 1)]),
            query_types: HashMap::from([
                ("level".to_string(), "minimum".to_string()),
                ("span".to_string(), "range".to_string()),
            ]),
            values: HashMap::from([(
                "span".to_string(),
                HashMap::from([("short".to_string(), 0.25), ("long".to_string(), 0.75)]),
            )]),
            services: points
                .iter()
                .map(|(id, p)| {
                    (id.to_string(), CustomEntity {
                        id: id.to_string(),
                        point: p.to_vec(),
                        metadata: serde_json::Value::Null,
                        registered_at: None,
                    })
                })
                .collect(),
            ..empty_topology(2)
        }
    }

    fn ids(results: &[DiscoveryResult]) -> Vec<&str> {
        results.iter().map(|r| r.id.as_str()).collect()
    }

    #[test]
    fn test_distance_precise() {
        let topology = empty_topology(3);

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
        let topology = empty_topology(5);

        // Run same calculation 1000 times - must be identical
        let p1 = vec![0.123456, 0.789012, 0.345678, 0.901234, 0.567890];
        let p2 = vec![0.987654, 0.321098, 0.765432, 0.109876, 0.543210];

        let first_result = topology.distance_precise(&p1, &p2);

        for _ in 0..1000 {
            let result = topology.distance_precise(&p1, &p2);
            assert_eq!(result, first_result, "Results must be identical for cluster safety");
        }
    }

    #[test]
    fn distance_keeps_differences_an_f32_would_erase() {
        assert!(fixed_distance(&[0.5], &[0.5 + 1e-9]) > 0.0);
    }

    #[test]
    fn range_with_both_bounds_excludes_values_outside_either_bound() {
        let t = topology(&[("below", [0.5, 0.1]), ("inside", [0.5, 0.5]), ("above", [0.5, 0.9])]);
        let r = t.discover_precise(&json!({"span": {"min": "short", "max": "long"}}), 10, false).unwrap();
        assert_eq!(ids(&r), vec!["inside"]);
    }

    #[test]
    fn a_single_bound_keeps_its_one_sided_meaning() {
        let t = topology(&[("low", [0.4, 0.2]), ("mid", [0.6, 0.5]), ("high", [0.9, 0.9])]);
        let at_least = t.discover_precise(&json!({"level": {"min": 0.5}}), 10, false).unwrap();
        assert_eq!(ids(&at_least), vec!["high", "mid"]);
        let at_most = t.discover_precise(&json!({"span": {"max": "short"}}), 10, false).unwrap();
        assert_eq!(ids(&at_most), vec!["low"]);
    }

    #[test]
    fn a_bare_value_on_a_range_axis_means_that_value() {
        let t = topology(&[("short", [0.5, 0.25]), ("long", [0.5, 0.75])]);
        let r = t.discover_precise(&json!({"span": "long"}), 10, false).unwrap();
        assert_eq!(ids(&r), vec!["long"]);
    }

    #[test]
    fn unknown_dimensions_and_values_are_errors_not_silent_matches() {
        let t = topology(&[("any", [0.5, 0.5])]);
        assert!(t.discover_precise(&json!({"colour": 1}), 10, false).is_err());
        assert!(t.discover_precise(&json!({"span": "medium"}), 10, false).is_err());
        assert!(t.discover_precise(&json!({"span": {}}), 10, false).is_err());
    }

    #[test]
    fn equal_entities_come_back_in_id_order() {
        let t = topology(&[("b", [0.5, 0.5]), ("a", [0.5, 0.5]), ("c", [0.5, 0.5])]);
        let discovered = t.discover_precise(&json!({"level": {"min": 0.1}}), 10, false).unwrap();
        assert_eq!(ids(&discovered), vec!["a", "b", "c"]);
        let nearest = t.knn_precise(&[0.5, 0.5], 3).unwrap();
        assert_eq!(ids(&nearest), vec!["a", "b", "c"]);
    }

    #[test]
    fn knn_refuses_a_query_of_the_wrong_width() {
        assert!(topology(&[]).knn_precise(&[0.5], 1).is_err());
    }
}
