<?php
namespace nCore\Modules\Core\Client;

/**
 * GSDFallback - Local fallback for when GSD is unavailable
 * 
 * Provides basic service discovery functionality through local memory
 * when GSD daemon is not available. This is a simplified implementation
 * that doesn't have all the geometric capabilities of the full daemon
 * but provides essential functionality for service discovery.
 */
class GSDFallback {
    /** @var array Capability dimensions */
    private $capabilityDimensions = [];
    
    /** @var array Services registry */
    private $services = [];
    
    /** @var array Dependency graph */
    private $dependencies = [];
    
    /** @var bool Debug mode */
    private $debug = false;
    
    /**
     * Constructor
     * 
     * @param array $config Configuration options
     */
    public function __construct(array $config = []) {
        $this->debug = $config['debug'] ?? false;
        
        $this->debug("Initializing GSD fallback");
    }
    
    /**
     * Log debug messages
     * 
     * @param string $message Debug message
     */
    private function debug(string $message): void {
        if ($this->debug) {
            error_log("[GSDFallback] {$message}");
        }
    }
    
    /**
     * Register a capability dimension
     * 
     * @param string $name Name of the capability
     * @param int $dimension Dimension index
     * @return bool Success
     */
    public function registerCapabilityDimension(string $name, int $dimension): bool {
        $this->debug("Registering capability dimension: {$name} => {$dimension}");
        $this->capabilityDimensions[$name] = $dimension;
        return true;
    }
    
    /**
     * Register a service with the topology
     * 
     * @param string $id Service ID
     * @param array $capabilities Array of capabilities [name => value]
     * @param array $metadata Optional metadata
     * @return bool Success
     */
    public function registerService(string $id, array $capabilities, array $metadata = []): bool {
        $this->debug("Registering service: {$id}");
        
        // Convert capabilities to a normalized format
        $normalizedCapabilities = [];
        foreach ($capabilities as $name => $value) {
            $normalizedCapabilities[$name] = (float)$value;
        }
        
        $this->services[$id] = [
            'id' => $id,
            'capabilities' => $normalizedCapabilities,
            'metadata' => $metadata
        ];
        
        // Extract dependencies from metadata if available
        if (isset($metadata['dependencies']) && is_array($metadata['dependencies'])) {
            $this->dependencies[$id] = $metadata['dependencies'];
        }
        
        return true;
    }
    
    /**
     * Find services matching requirements
     * 
     * @param array $requirements Array of requirements [name => min_value]
     * @return array Array of service IDs
     */
    public function findServices(array $requirements): array {
        $this->debug("Finding services with requirements: " . json_encode($requirements));
        
        $matches = [];
        
        foreach ($this->services as $id => $service) {
            $match = true;
            
            foreach ($requirements as $name => $minValue) {
                if (!isset($service['capabilities'][$name]) || 
                    $service['capabilities'][$name] < $minValue) {
                    $match = false;
                    break;
                }
            }
            
            if ($match) {
                $matches[] = $id;
            }
        }
        
        $this->debug("Found " . count($matches) . " matching services");
        return $matches;
    }
    
    /**
     * Get the load sequence
     * 
     * This implementation uses a simple topological sort of the dependency graph
     * to determine a valid load sequence.
     * 
     * @return array Array of service IDs in load order
     */
    public function getLoadSequence(): array {
        $this->debug("Generating load sequence");
        
        // If there are no dependencies, just return all service IDs
        if (empty($this->dependencies)) {
            return array_keys($this->services);
        }
        
        // Build dependency graph
        $graph = [];
        foreach ($this->services as $id => $service) {
            $graph[$id] = $this->dependencies[$id] ?? [];
        }
        
        // Perform topological sort
        $visited = [];
        $temp = [];
        $order = [];
        
        // Visit function for DFS
        $visit = function($node) use (&$visited, &$temp, &$order, &$graph, &$visit) {
            // If node is already in result, skip
            if (isset($visited[$node])) {
                return;
            }
            
            // Check for circular dependencies
            if (isset($temp[$node])) {
                // Circular dependency detected, but we'll continue anyway
                // by breaking the cycle
                $this->debug("Warning: Circular dependency detected with service {$node}");
                return;
            }
            
            // Mark node as temporarily visited
            $temp[$node] = true;
            
            // Visit dependencies
            if (isset($graph[$node])) {
                foreach ($graph[$node] as $dependency) {
                    if (isset($graph[$dependency])) {
                        $visit($dependency);
                    }
                }
            }
            
            // Mark node as visited and add to result
            unset($temp[$node]);
            $visited[$node] = true;
            $order[] = $node;
        };
        
        // Visit all nodes
        foreach (array_keys($graph) as $node) {
            if (!isset($visited[$node])) {
                $visit($node);
            }
        }
        
        // Reverse the order to get correct load sequence
        $loadSequence = array_reverse($order);
        
        // Add any services not in the dependency graph
        foreach (array_keys($this->services) as $id) {
            if (!in_array($id, $loadSequence)) {
                $loadSequence[] = $id;
            }
        }
        
        $this->debug("Generated load sequence with " . count($loadSequence) . " services");
        return $loadSequence;
    }
    
    /**
     * Get registered capability dimensions
     * 
     * @return array Map of capability names to dimensions
     */
    public function getCapabilityDimensions(): array {
        return $this->capabilityDimensions;
    }
    
    /**
     * Get services registry
     * 
     * @return array All registered services
     */
    public function getServices(): array {
        return $this->services;
    }
    
    /**
     * Get dependency graph
     * 
     * @return array Service dependencies
     */
    public function getDependencies(): array {
        return $this->dependencies;
    }
    
    /**
     * Clear all stored data
     * 
     * @return bool Success
     */
    public function clear(): bool {
        $this->debug("Clearing all data");
        
        $this->capabilityDimensions = [];
        $this->services = [];
        $this->dependencies = [];
        
        return true;
    }
    
    /**
     * Check if the fallback is working
     * 
     * @return string Hello message
     */
    public function hello(): string {
        return "Hello from GSD fallback implementation!";
    }
}