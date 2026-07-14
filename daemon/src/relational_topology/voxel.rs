//! Voxel Grid for O(1) spatial lookups.
//!
//! A 10×10×10 grid that partitions 3D relational space into 1000 buckets.
//! Each bucket contains references to entities within that spatial region.

use super::point::RelPoint3D;
use super::topology::EntityId;
use std::collections::HashMap;

/// Voxel key as (x, y, z) bucket indices.
pub type VoxelKey = (u8, u8, u8);

/// Default grid size (10×10×10 = 1000 buckets).
pub const DEFAULT_GRID_SIZE: i32 = 10;

/// Number of Z-slices for ordered iteration.
#[allow(dead_code)]
pub const Z_SLICE_COUNT: usize = 10;

/// Voxel grid for O(1) spatial queries.
///
/// The grid divides the unit cube [0,1]³ into `grid_size³` buckets.
/// Each bucket stores a list of entity IDs located within that region.
///
/// # Performance
///
/// - Point query: O(1) - direct hash lookup
/// - Range query: O(k) - k neighboring buckets
/// - Insert: O(1) amortized
/// - Remove: O(n) where n = entities in bucket
#[derive(Debug, Clone)]
pub struct VoxelGrid {
    /// Grid size per dimension (default: 10).
    grid_size: i32,

    /// Bucket storage: voxel_key -> list of entity IDs.
    buckets: HashMap<VoxelKey, Vec<EntityId>>,

    /// Z-slice index for O(L) ordered iteration.
    /// z_slices[i] contains all entities where floor(z * grid_size) == i
    z_slices: Vec<Vec<EntityId>>,

    /// Total entity count.
    entity_count: usize,
}

impl VoxelGrid {
    /// Create a new voxel grid with default size (10×10×10).
    pub fn new() -> Self {
        Self::with_grid_size(DEFAULT_GRID_SIZE)
    }

    /// Create a voxel grid with custom grid size.
    pub fn with_grid_size(grid_size: i32) -> Self {
        Self {
            grid_size,
            buckets: HashMap::new(),
            z_slices: vec![Vec::new(); grid_size as usize],
            entity_count: 0,
        }
    }

    /// Get the grid size.
    pub fn grid_size(&self) -> i32 {
        self.grid_size
    }

    /// Insert an entity at a given position.
    ///
    /// Returns the voxel key and z-slice where the entity was inserted.
    pub fn insert(&mut self, id: EntityId, position: &RelPoint3D) -> (VoxelKey, usize) {
        let voxel_key = position.to_voxel_key(self.grid_size);
        let z_slice = position.z_slice(self.grid_size);

        // Insert into voxel bucket
        self.buckets
            .entry(voxel_key)
            .or_default()
            .push(id.clone());

        // Insert into z-slice for ordered iteration
        if z_slice < self.z_slices.len() {
            self.z_slices[z_slice].push(id);
        }

        self.entity_count += 1;
        (voxel_key, z_slice)
    }

    /// Remove an entity from the grid.
    ///
    /// Returns true if the entity was found and removed.
    pub fn remove(&mut self, id: &EntityId, position: &RelPoint3D) -> bool {
        let voxel_key = position.to_voxel_key(self.grid_size);
        let z_slice = position.z_slice(self.grid_size);

        let mut removed = false;

        // Remove from voxel bucket
        if let Some(bucket) = self.buckets.get_mut(&voxel_key) {
            if let Some(pos) = bucket.iter().position(|x| x == id) {
                bucket.swap_remove(pos);
                removed = true;
            }
        }

        // Remove from z-slice
        if z_slice < self.z_slices.len() {
            if let Some(pos) = self.z_slices[z_slice].iter().position(|x| x == id) {
                self.z_slices[z_slice].swap_remove(pos);
            }
        }

        if removed {
            self.entity_count -= 1;
        }
        removed
    }

    /// Get all entities in a specific voxel bucket - O(1).
    pub fn get_bucket(&self, key: VoxelKey) -> &[EntityId] {
        self.buckets.get(&key).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get all entities at a specific voxel position - O(1).
    pub fn entities_at(&self, position: &RelPoint3D) -> &[EntityId] {
        let key = position.to_voxel_key(self.grid_size);
        self.get_bucket(key)
    }

    /// Get all entities in a Z-slice - O(1).
    ///
    /// Z-slices are indexed 0 to grid_size-1, where slice 0 contains
    /// entities with the lowest Z values (foundation/providers in dependency topology).
    pub fn get_z_slice(&self, z_bucket: usize) -> &[EntityId] {
        if z_bucket < self.z_slices.len() {
            &self.z_slices[z_bucket]
        } else {
            &[]
        }
    }

    /// Get all entities below a Z threshold (providers) - O(Z).
    ///
    /// Returns entities in Z-slices [0, z_bucket).
    pub fn entities_below_z(&self, z_bucket: usize) -> impl Iterator<Item = &EntityId> {
        self.z_slices[..z_bucket.min(self.z_slices.len())]
            .iter()
            .flat_map(|slice| slice.iter())
    }

    /// Get all entities above a Z threshold (dependents) - O(Z).
    ///
    /// Returns entities in Z-slices (z_bucket, grid_size].
    pub fn entities_above_z(&self, z_bucket: usize) -> impl Iterator<Item = &EntityId> {
        let start = (z_bucket + 1).min(self.z_slices.len());
        self.z_slices[start..]
            .iter()
            .flat_map(|slice| slice.iter())
    }

    /// Iterate all entities in Z-order (ascending) - O(L) where L = grid_size.
    ///
    /// This gives load order for dependency topologies: providers first, dependents last.
    pub fn z_ordered_iter(&self) -> impl Iterator<Item = &EntityId> {
        self.z_slices.iter().flat_map(|slice| slice.iter())
    }

    /// Iterate all entities in reverse Z-order (descending) - O(L).
    ///
    /// This gives shutdown order: dependents first, providers last.
    pub fn z_ordered_iter_rev(&self) -> impl Iterator<Item = &EntityId> {
        self.z_slices.iter().rev().flat_map(|slice| slice.iter())
    }

    /// Get neighboring voxels (26-connectivity + self).
    ///
    /// Returns up to 27 voxel keys in the 3×3×3 neighborhood.
    pub fn get_neighbors(&self, key: VoxelKey) -> Vec<VoxelKey> {
        let (x, y, z) = key;
        let mut neighbors = Vec::with_capacity(27);

        for dx in -1i8..=1 {
            for dy in -1i8..=1 {
                for dz in -1i8..=1 {
                    let nx = x as i8 + dx;
                    let ny = y as i8 + dy;
                    let nz = z as i8 + dz;

                    if nx >= 0
                        && nx < self.grid_size as i8
                        && ny >= 0
                        && ny < self.grid_size as i8
                        && nz >= 0
                        && nz < self.grid_size as i8
                    {
                        neighbors.push((nx as u8, ny as u8, nz as u8));
                    }
                }
            }
        }

        neighbors
    }

    /// Get all entities in neighboring voxels - O(27 * avg_bucket_size).
    pub fn entities_near(&self, position: &RelPoint3D) -> impl Iterator<Item = &EntityId> {
        let key = position.to_voxel_key(self.grid_size);
        let neighbors = self.get_neighbors(key);

        neighbors
            .into_iter()
            .flat_map(move |k| self.get_bucket(k).iter())
    }

    /// Get statistics about the grid.
    pub fn stats(&self) -> VoxelGridStats {
        let non_empty_buckets = self.buckets.values().filter(|b| !b.is_empty()).count();
        let max_bucket_size = self.buckets.values().map(|b| b.len()).max().unwrap_or(0);
        let avg_bucket_size = if non_empty_buckets > 0 {
            self.entity_count as f64 / non_empty_buckets as f64
        } else {
            0.0
        };

        VoxelGridStats {
            grid_size: self.grid_size,
            total_entities: self.entity_count,
            non_empty_buckets,
            max_bucket_size,
            avg_bucket_size,
        }
    }

    /// Total number of entities in the grid.
    pub fn len(&self) -> usize {
        self.entity_count
    }

    /// Check if grid is empty.
    pub fn is_empty(&self) -> bool {
        self.entity_count == 0
    }

    /// Clear all entities from the grid.
    pub fn clear(&mut self) {
        self.buckets.clear();
        for slice in &mut self.z_slices {
            slice.clear();
        }
        self.entity_count = 0;
    }
}

impl Default for VoxelGrid {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about voxel grid utilization.
#[derive(Debug, Clone)]
pub struct VoxelGridStats {
    pub grid_size: i32,
    pub total_entities: usize,
    pub non_empty_buckets: usize,
    pub max_bucket_size: usize,
    pub avg_bucket_size: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_lookup() {
        let mut grid = VoxelGrid::new();
        let id = "service_a".to_string();
        let pos = RelPoint3D::from_f64(0.55, 0.32, 0.87);

        let (voxel_key, z_slice) = grid.insert(id.clone(), &pos);

        assert_eq!(voxel_key, (5, 3, 8));
        assert_eq!(z_slice, 8);

        let bucket = grid.get_bucket(voxel_key);
        assert_eq!(bucket.len(), 1);
        assert_eq!(bucket[0], id);
    }

    #[test]
    fn test_z_ordered_iteration() {
        let mut grid = VoxelGrid::new();

        // Insert entities at different Z levels
        grid.insert("provider".to_string(), &RelPoint3D::from_f64(0.5, 0.5, 0.1));
        grid.insert("middle".to_string(), &RelPoint3D::from_f64(0.5, 0.5, 0.5));
        grid.insert("dependent".to_string(), &RelPoint3D::from_f64(0.5, 0.5, 0.9));

        let order: Vec<_> = grid.z_ordered_iter().collect();
        assert_eq!(order.len(), 3);
        assert_eq!(order[0], "provider");
        assert_eq!(order[1], "middle");
        assert_eq!(order[2], "dependent");
    }

    #[test]
    fn test_entities_below_z() {
        let mut grid = VoxelGrid::new();

        grid.insert("low".to_string(), &RelPoint3D::from_f64(0.5, 0.5, 0.15));
        grid.insert("mid".to_string(), &RelPoint3D::from_f64(0.5, 0.5, 0.45));
        grid.insert("high".to_string(), &RelPoint3D::from_f64(0.5, 0.5, 0.85));

        // Get entities below z=0.5 (z_bucket = 5)
        let below: Vec<_> = grid.entities_below_z(5).collect();
        assert_eq!(below.len(), 2); // low (z_slice=1) and mid (z_slice=4)
    }

    #[test]
    fn test_remove() {
        let mut grid = VoxelGrid::new();
        let id = "test".to_string();
        let pos = RelPoint3D::from_f64(0.5, 0.5, 0.5);

        grid.insert(id.clone(), &pos);
        assert_eq!(grid.len(), 1);

        let removed = grid.remove(&id, &pos);
        assert!(removed);
        assert_eq!(grid.len(), 0);
    }

    #[test]
    fn test_neighbors() {
        let grid = VoxelGrid::new();
        let neighbors = grid.get_neighbors((5, 5, 5));
        assert_eq!(neighbors.len(), 27); // 3×3×3 including self

        // Corner has fewer neighbors
        let corner_neighbors = grid.get_neighbors((0, 0, 0));
        assert_eq!(corner_neighbors.len(), 8); // 2×2×2
    }
}
