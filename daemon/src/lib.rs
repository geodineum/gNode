use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};
use thiserror::Error;

// Re-export daemon module
pub mod daemon;

// Error handling
#[derive(Error, Debug)]
pub enum GeometricError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("YAML error: {0}")]
    Yaml(String),
    
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    
    #[error("Dimension mismatch: expected {expected} but got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
    
    #[error("Service not found: {0}")]
    ServiceNotFound(String),
    
    #[error("Invalid state: {0}")]
    InvalidState(String),
    
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
}

pub type Result<T> = std::result::Result<T, GeometricError>;

// Basic types
pub type ServiceId = String;
pub type CapabilityPoint = Vec<f64>;
pub type RequirementPoint = Vec<f64>;

// Service configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Capability {
    pub name: String,
    pub value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Requirement {
    pub name: String,
    pub min_value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequirementSet {
    pub requirements: Vec<Requirement>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub id: ServiceId,
    pub capabilities: Vec<Capability>,
    pub metadata: HashMap<String, String>,
}

// Geometric Topology
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeometricTopology {
    pub dimensions: usize,
    pub services: HashMap<ServiceId, ServicePointData>,
    pub capability_dimensions: HashMap<String, usize>,
    pub dependencies: HashMap<ServiceId, Vec<ServiceId>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServicePointData {
    pub id: ServiceId,
    pub point: CapabilityPoint,
    pub metadata: HashMap<String, String>,
}

impl GeometricTopology {
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            services: HashMap::new(),
            capability_dimensions: HashMap::new(),
            dependencies: HashMap::new(),
        }
    }
    
    pub fn register_service(&mut self, service: &ServiceConfig) -> Result<()> {
        // Convert capabilities to point
        let mut point = vec![0.0; self.dimensions];
        
        for cap in &service.capabilities {
            if let Some(dim) = self.capability_dimensions.get(&cap.name) {
                if *dim < self.dimensions {
                    point[*dim] = cap.value;
                }
            }
        }
        
        // Create service data
        let service_data = ServicePointData {
            id: service.id.clone(),
            point: point.clone(),
            metadata: service.metadata.clone(),
        };
        
        // Register service
        self.services.insert(service.id.clone(), service_data);
        
        // Extract dependencies from metadata if available
        if let Some(deps_str) = service.metadata.get("dependencies") {
            if let Ok(deps) = serde_json::from_str::<Vec<ServiceId>>(deps_str) {
                self.dependencies.insert(service.id.clone(), deps);
            }
        }
        
        Ok(())
    }
    
    pub fn find_services(&self, requirements: &RequirementSet) -> Result<Vec<ServiceId>> {
        // Convert requirements to point
        let mut req_point = vec![0.0; self.dimensions];
        
        for req in &requirements.requirements {
            if let Some(dim) = self.capability_dimensions.get(&req.name) {
                if *dim < self.dimensions {
                    req_point[*dim] = req.min_value;
                }
            }
        }
        
        // Filter services to find matches
        let matches = self.services.iter()
            .filter(|(_, service)| {
                // Check if service meets requirements
                for req in &requirements.requirements {
                    if let Some(dim) = self.capability_dimensions.get(&req.name) {
                        if *dim < self.dimensions && *dim < service.point.len() {
                            if service.point[*dim] < req.min_value {
                                return false;
                            }
                        }
                    }
                }
                true
            })
            .map(|(id, _)| id.clone())
            .collect();
        
        Ok(matches)
    }
    
    pub fn get_load_sequence(&self) -> Result<Vec<ServiceId>> {
        // If there are no dependencies, just return all services
        if self.dependencies.is_empty() {
            return Ok(self.services.keys().cloned().collect());
        }
        
        // Use topological sort to determine load order
        let mut visited = HashMap::new();
        let mut temp = HashMap::new();
        let mut order = Vec::new();
        
        // Process each service
        for id in self.services.keys() {
            if !visited.contains_key(id) {
                self.visit_node(id, &mut visited, &mut temp, &mut order)?;
            }
        }
        
        // Reverse to get correct load order
        order.reverse();
        
        Ok(order)
    }
    
    fn visit_node(
        &self,
        node: &ServiceId,
        visited: &mut HashMap<ServiceId, bool>,
        temp: &mut HashMap<ServiceId, bool>,
        order: &mut Vec<ServiceId>
    ) -> Result<()> {
        // Check for circular dependencies
        if temp.contains_key(node) {
            return Err(GeometricError::InvalidState(
                format!("Circular dependency detected involving {}", node)
            ));
        }
        
        // Skip if already visited
        if visited.contains_key(node) {
            return Ok(());
        }
        
        // Mark as temporarily visited
        temp.insert(node.clone(), true);
        
        // Visit dependencies
        if let Some(deps) = self.dependencies.get(node) {
            for dep in deps {
                if self.services.contains_key(dep) {
                    self.visit_node(dep, visited, temp, order)?;
                }
            }
        }
        
        // Mark as visited and add to order
        temp.remove(node);
        visited.insert(node.clone(), true);
        order.push(node.clone());
        
        Ok(())
    }
    
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(GeometricError::Json)
    }
    
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json)
            .map_err(GeometricError::Json)
    }
    
    pub fn to_bincode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(GeometricError::Bincode)
    }
    
    pub fn from_bincode(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(GeometricError::Bincode)
    }
    
    pub fn get_load_distribution(&self) -> HashMap<ServiceId, f64> {
        let mut distribution = HashMap::new();
        let total_services = self.services.len() as f64;
        
        if total_services > 0.0 {
            for id in self.services.keys() {
                distribution.insert(id.clone(), 1.0 / total_services);
            }
        }
        
        distribution
    }
}

// Redis-based persistence for GeometricTopology
pub struct GeometricTopologyStorage {
    client: redis::Client,
    site_id: String,
    prefix: String,
}

impl GeometricTopologyStorage {
    pub fn new(redis_url: &str, site_id: &str, prefix: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        
        Ok(Self {
            client,
            site_id: site_id.to_string(),
            prefix: prefix.to_string(),
        })
    }
    
    fn get_storage_key(&self) -> String {
        format!("{{{0}}}:{1}:topology", self.site_id, self.prefix)
    }
    
    pub fn save(&self, topology: &GeometricTopology) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let key = self.get_storage_key();
        
        // Serialize topology
        let data = topology.to_bincode()?;
        
        // Save to Redis
        redis::cmd("SET")
            .arg(&key)
            .arg(data)
            .execute(&mut conn);
        
        Ok(())
    }
    
    pub fn load(&self) -> Result<Option<GeometricTopology>> {
        let mut conn = self.client.get_connection()?;
        let key = self.get_storage_key();
        
        // Load from Redis
        let data: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&key)
            .query(&mut conn)
            .map_err(GeometricError::Redis)?;
        
        match data {
            Some(bytes) => Ok(Some(GeometricTopology::from_bincode(&bytes)?)),
            None => Ok(None),
        }
    }
}

// Shared topology manager
pub struct SharedTopology {
    topology: Arc<Mutex<GeometricTopology>>,
    storage: Option<GeometricTopologyStorage>,
    auto_save: bool,
}

impl SharedTopology {
    pub fn new(dimensions: usize) -> Self {
        Self {
            topology: Arc::new(Mutex::new(GeometricTopology::new(dimensions))),
            storage: None,
            auto_save: false,
        }
    }
    
    pub fn with_storage(dimensions: usize, redis_url: &str, site_id: &str, prefix: &str) -> Result<Self> {
        let storage = GeometricTopologyStorage::new(redis_url, site_id, prefix)?;
        
        // Try to load existing topology
        let topology = match storage.load()? {
            Some(t) => t,
            None => GeometricTopology::new(dimensions),
        };
        
        Ok(Self {
            topology: Arc::new(Mutex::new(topology)),
            storage: Some(storage),
            auto_save: true,
        })
    }
    
    pub fn get_topology_ref(&self) -> Arc<Mutex<GeometricTopology>> {
        Arc::clone(&self.topology)
    }
    
    pub fn register_capability_dimension(&self, name: &str, dimension: usize) -> Result<()> {
        let mut topology = self.topology.lock().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to lock topology: {}", e))
        })?;
        
        topology.capability_dimensions.insert(name.to_string(), dimension);
        
        // Auto-save if enabled
        if self.auto_save {
            if let Some(storage) = &self.storage {
                storage.save(&topology)?;
            }
        }
        
        Ok(())
    }
    
    pub fn register_service(&self, service: &ServiceConfig) -> Result<()> {
        let mut topology = self.topology.lock().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to lock topology: {}", e))
        })?;
        
        topology.register_service(service)?;
        
        // Auto-save if enabled
        if self.auto_save {
            if let Some(storage) = &self.storage {
                storage.save(&topology)?;
            }
        }
        
        Ok(())
    }
    
    pub fn find_services(&self, requirements: &RequirementSet) -> Result<Vec<ServiceId>> {
        let topology = self.topology.lock().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to lock topology: {}", e))
        })?;
        
        topology.find_services(requirements)
    }
    
    pub fn get_load_sequence(&self) -> Result<Vec<ServiceId>> {
        let topology = self.topology.lock().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to lock topology: {}", e))
        })?;
        
        topology.get_load_sequence()
    }
    
    pub fn get_capability_dimensions(&self) -> Result<HashMap<String, usize>> {
        let topology = self.topology.lock().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to lock topology: {}", e))
        })?;
        
        Ok(topology.capability_dimensions.clone())
    }
    
    pub fn save(&self) -> Result<()> {
        if let Some(storage) = &self.storage {
            let topology = self.topology.lock().map_err(|e| {
                GeometricError::InvalidState(format!("Failed to lock topology: {}", e))
            })?;
            
            storage.save(&topology)?;
        }
        
        Ok(())
    }
}

// FFI implementation for PHP compatibility
// (Only needed for backward compatibility, daemon mode is preferred)
#[no_mangle]
pub extern "C" fn geometric_topology_create(dimensions: usize) -> *mut GeometricTopology {
    Box::into_raw(Box::new(GeometricTopology::new(dimensions)))
}

#[no_mangle]
pub extern "C" fn geometric_topology_free(topology: *mut GeometricTopology) {
    if !topology.is_null() {
        unsafe {
            let _ = Box::from_raw(topology);
        }
    }
}

// Global error handling
static mut LAST_ERROR: Option<String> = None;

fn set_error(error: String) {
    unsafe {
        LAST_ERROR = Some(error);
    }
}

#[no_mangle]
pub extern "C" fn geometric_last_error() -> *const c_char {
    unsafe {
        if let Some(error) = &LAST_ERROR {
            CString::new(error.clone()).unwrap_or_default().into_raw()
        } else {
            CString::new("No error").unwrap_or_default().into_raw()
        }
    }
}

#[no_mangle]
pub extern "C" fn geometric_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            let _ = CString::from_raw(s);
        }
    }
}

#[no_mangle]
pub extern "C" fn hello_world() -> *mut c_char {
    let message = "Hello from GSD daemon!";
    CString::new(message).unwrap().into_raw()
}

#[no_mangle]
pub extern "C" fn geometric_topology_register_capability_dimension(
    topology: *mut GeometricTopology,
    name: *const c_char,
    dimension: usize
) -> bool {
    if topology.is_null() || name.is_null() {
        set_error("Null parameters".to_string());
        return false;
    }
    
    let topology = unsafe { &mut *topology };
    
    // Convert name from C string
    let name_str = match unsafe { CStr::from_ptr(name).to_str() } {
        Ok(s) => s.to_string(),
        Err(_) => {
            set_error("Invalid name string".to_string());
            return false;
        }
    };
    
    // Register the capability dimension
    topology.capability_dimensions.insert(name_str, dimension);
    
    true
}

#[no_mangle]
pub extern "C" fn geometric_topology_register_service(
    topology: *mut GeometricTopology,
    service_json: *const c_char
) -> bool {
    if topology.is_null() || service_json.is_null() {
        set_error("Null parameters".to_string());
        return false;
    }
    
    let topology = unsafe { &mut *topology };
    
    // Convert service JSON
    let service_str = match unsafe { CStr::from_ptr(service_json).to_str() } {
        Ok(s) => s,
        Err(_) => {
            set_error("Invalid service JSON string".to_string());
            return false;
        }
    };
    
    // Parse service
    let service: ServiceConfig = match serde_json::from_str(service_str) {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Failed to parse service JSON: {}", e));
            return false;
        }
    };
    
    // Register service
    match topology.register_service(&service) {
        Ok(_) => true,
        Err(e) => {
            set_error(format!("Failed to register service: {:?}", e));
            false
        }
    }
}

#[no_mangle]
pub extern "C" fn geometric_topology_find_services(
    topology: *mut GeometricTopology,
    requirements_json: *const c_char
) -> *mut c_char {
    if topology.is_null() || requirements_json.is_null() {
        set_error("Null parameters".to_string());
        return ptr::null_mut();
    }
    
    let topology = unsafe { &*topology };
    
    // Convert requirements JSON
    let req_str = match unsafe { CStr::from_ptr(requirements_json).to_str() } {
        Ok(s) => s,
        Err(_) => {
            set_error("Invalid requirements JSON string".to_string());
            return ptr::null_mut();
        }
    };
    
    // Parse requirements
    let requirements: RequirementSet = match serde_json::from_str(req_str) {
        Ok(r) => r,
        Err(e) => {
            set_error(format!("Failed to parse requirements JSON: {}", e));
            return ptr::null_mut();
        }
    };
    
    // Find services
    match topology.find_services(&requirements) {
        Ok(services) => {
            // Convert to JSON
            match serde_json::to_string(&services) {
                Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
                Err(e) => {
                    set_error(format!("Failed to serialize results: {}", e));
                    ptr::null_mut()
                }
            }
        },
        Err(e) => {
            set_error(format!("Failed to find services: {:?}", e));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn geometric_topology_get_load_sequence(
    topology: *mut GeometricTopology
) -> *mut c_char {
    if topology.is_null() {
        set_error("Null topology pointer".to_string());
        return ptr::null_mut();
    }
    
    let topology = unsafe { &*topology };
    
    match topology.get_load_sequence() {
        Ok(sequence) => {
            // Convert to JSON
            match serde_json::to_string(&sequence) {
                Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
                Err(e) => {
                    set_error(format!("Failed to serialize load sequence: {}", e));
                    ptr::null_mut()
                }
            }
        },
        Err(e) => {
            set_error(format!("Failed to get load sequence: {:?}", e));
            ptr::null_mut()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_register_service() {
        let mut topology = GeometricTopology::new(2);
        
        // Register capability dimensions
        topology.capability_dimensions.insert("storage".to_string(), 0);
        topology.capability_dimensions.insert("compute".to_string(), 1);
        
        // Create service
        let service = ServiceConfig {
            id: "test-service".to_string(),
            capabilities: vec![
                Capability {
                    name: "storage".to_string(),
                    value: 0.7,
                },
                Capability {
                    name: "compute".to_string(),
                    value: 0.3,
                },
            ],
            metadata: HashMap::new(),
        };
        
        // Register service
        topology.register_service(&service).unwrap();
        
        // Verify registration
        assert!(topology.services.contains_key(&"test-service".to_string()));
        
        let service_data = topology.services.get(&"test-service".to_string()).unwrap();
        assert_eq!(service_data.point[0], 0.7);
        assert_eq!(service_data.point[1], 0.3);
    }
    
    #[test]
    fn test_find_services() {
        let mut topology = GeometricTopology::new(2);
        
        // Register capability dimensions
        topology.capability_dimensions.insert("storage".to_string(), 0);
        topology.capability_dimensions.insert("compute".to_string(), 1);
        
        // Create services
        let service1 = ServiceConfig {
            id: "service1".to_string(),
            capabilities: vec![
                Capability {
                    name: "storage".to_string(),
                    value: 0.9,
                },
                Capability {
                    name: "compute".to_string(),
                    value: 0.1,
                },
            ],
            metadata: HashMap::new(),
        };
        
        let service2 = ServiceConfig {
            id: "service2".to_string(),
            capabilities: vec![
                Capability {
                    name: "storage".to_string(),
                    value: 0.5,
                },
                Capability {
                    name: "compute".to_string(),
                    value: 0.5,
                },
            ],
            metadata: HashMap::new(),
        };
        
        // Register services
        topology.register_service(&service1).unwrap();
        topology.register_service(&service2).unwrap();
        
        // Find services with storage > 0.7
        let requirements = RequirementSet {
            requirements: vec![
                Requirement {
                    name: "storage".to_string(),
                    min_value: 0.7,
                },
            ],
        };
        
        let matches = topology.find_services(&requirements).unwrap();
        
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], "service1");
    }
    
    #[test]
    fn test_load_sequence() {
        let mut topology = GeometricTopology::new(2);
        
        // Register services
        let service1 = ServiceConfig {
            id: "service1".to_string(),
            capabilities: vec![],
            metadata: HashMap::new(),
        };
        
        let service2 = ServiceConfig {
            id: "service2".to_string(),
            capabilities: vec![],
            metadata: {
                let mut map = HashMap::new();
                map.insert("dependencies".to_string(), r#"["service1"]"#.to_string());
                map
            },
        };
        
        let service3 = ServiceConfig {
            id: "service3".to_string(),
            capabilities: vec![],
            metadata: {
                let mut map = HashMap::new();
                map.insert("dependencies".to_string(), r#"["service2"]"#.to_string());
                map
            },
        };
        
        // Register services
        topology.register_service(&service1).unwrap();
        topology.register_service(&service2).unwrap();
        topology.register_service(&service3).unwrap();
        
        // Get load sequence
        let sequence = topology.get_load_sequence().unwrap();
        
        // Check that dependencies come before dependents
        let pos1 = sequence.iter().position(|id| id == "service1").unwrap();
        let pos2 = sequence.iter().position(|id| id == "service2").unwrap();
        let pos3 = sequence.iter().position(|id| id == "service3").unwrap();
        
        assert!(pos1 < pos2);
        assert!(pos2 < pos3);
    }
}