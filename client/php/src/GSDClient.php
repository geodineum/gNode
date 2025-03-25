<?php
namespace nCore\Modules\Core\Client;

use nCore\Modules\Core\Adapters\Shared\ValKeyStorage;
use nCore\Modules\Core\Interfaces\Shared\StorageInterface;
use nCore\Modules\Core\Exceptions\StorageException;
use nCore\Modules\Core\Exceptions\InitializationException;

/**
 * GSDClient - Client implementation for the GSD service
 * 
 * Provides API for communication with the Geometric Service Daemon (GSD).
 * This is the production-ready fixed version that properly handles
 * ValKey stream communication with the daemon.
 * 
 * @version 1.0.0
 * @date 2025-03-25
 */
class GSDClient {
    /** @var ValKeyStorage Storage for communication */
    private $storage;
    
    /** @var string Site identifier */
    private $siteId;
    
    /** @var string Node identifier */
    private $nodeId;
    
    /** @var array Configuration */
    private $config;
    
    /** @var bool Connection state */
    private $connected = false;
    
    /** @var string Communication stream name */
    private $streamName;
    
    /** @var bool Auto-start daemon if not running */
    private $autoStartDaemon = false;
    
    /** @var array Cached capability dimensions */
    private $capabilityDimensions = [];
    
    /** @var GSDFallback Fallback implementation */
    private $fallback;
    
    /** @var bool Using fallback mode */
    private $usingFallback = false;
    
    /** @var array Command response cache */
    private $responseCache = [];
    
    /** @var int Request timeout in milliseconds */
    private $timeout = 5000;
    
    /** @var int Cache expiration in seconds */
    private $cacheExpiration = 300;
    
    /**
     * Constructor
     * 
     * @param ValKeyStorage $storage ValKey storage for communication
     * @param string $siteId Site identifier
     * @param string $nodeId Node identifier
     * @param array $config Configuration options
     */
    public function __construct(
        ValKeyStorage $storage,
        string $siteId = 'default',
        string $nodeId = 'default',
        array $config = []
    ) {
        $this->storage = $storage;
        $this->siteId = $siteId;
        $this->nodeId = $nodeId;
        $this->config = array_merge([
            'stream_prefix' => 'gsd',
            'auto_start' => false,
            'daemon_path' => null,
            'timeout' => 5.0,
            'retry_attempts' => 3,
            'retry_delay' => 0.5,
            'use_fallback' => true,
            'cache_expiration' => 300,
            'debug' => false
        ], $config);
        
        $this->streamName = sprintf('{%s}:%s:stream:%s',
            $this->siteId,
            $this->config['stream_prefix'],
            $this->nodeId
        );
        
        $this->autoStartDaemon = $this->config['auto_start'] && $this->config['daemon_path'];
        $this->timeout = intval($this->config['timeout'] * 1000); // Convert to milliseconds
        $this->cacheExpiration = intval($this->config['cache_expiration']);
        
        // Initialize fallback if enabled
        if ($this->config['use_fallback']) {
            $this->fallback = new GSDFallback();
        }
        
        // Initialize streams and connection
        $this->setupStreams();
        $this->connect();
    }
    
    /**
     * Set up streams and consumer groups
     */
    private function setupStreams(): void {
        if (!$this->storage->isConnected()) {
            throw new StorageException('Not connected to ValKey server');
        }
        
        $commandStream = $this->streamName . ':commands';
        $responseStream = $this->streamName . ':responses';
        
        $this->debug("Setting up streams: {$commandStream}, {$responseStream}");
        
        try {
            // Create command consumer group
            $this->storage->xGroupCreate($commandStream, 'gsd-daemon', '$', true);
            
            // Create response consumer group
            $this->storage->xGroupCreate($responseStream, 'gsd-client', '$', true);
            
            $this->debug("Streams and consumer groups created successfully");
        } catch (\Exception $e) {
            // Group may already exist, ignore
            $this->debug("Stream setup error (may already exist): {$e->getMessage()}");
        }
    }
    
    /**
     * Connect to the daemon via ValKey
     * 
     * @return bool Success status
     */
    private function connect(): bool {
        if (!$this->storage->isConnected()) {
            throw new StorageException('Not connected to ValKey server');
        }
        
        $this->debug("Connecting to GSD daemon via stream: {$this->streamName}");
        
        // Check if daemon is running by pinging it
        $response = $this->sendCommand('ping');
        
        if ($response && isset($response['status']) && $response['status'] === 'ok') {
            $this->connected = true;
            $this->usingFallback = false;
            $this->debug("Successfully connected to GSD daemon");
            return true;
        }
        
        // If auto-start is enabled, try to start the daemon
        if ($this->autoStartDaemon) {
            $this->debug("Auto-starting GSD daemon");
            $this->startDaemon();
            
            // Wait for daemon to start
            $attempts = 0;
            $maxAttempts = $this->config['retry_attempts'];
            $delay = $this->config['retry_delay'];
            
            while ($attempts < $maxAttempts) {
                sleep($delay);
                $attempts++;
                
                $this->debug("Pinging daemon (attempt {$attempts}/{$maxAttempts})");
                $response = $this->sendCommand('ping');
                if ($response && isset($response['status']) && $response['status'] === 'ok') {
                    $this->connected = true;
                    $this->usingFallback = false;
                    $this->debug("Successfully connected to auto-started GSD daemon");
                    return true;
                }
            }
            
            $this->debug("Failed to connect to auto-started GSD daemon after {$maxAttempts} attempts");
        }
        
        // Fall back to local implementation if available
        if ($this->fallback) {
            $this->debug("Using fallback implementation");
            $this->usingFallback = true;
            return true;
        }
        
        $this->debug("Failed to connect to GSD daemon and no fallback available");
        return false;
    }
    
    /**
     * Start the daemon process
     * 
     * @return bool Success status
     */
    private function startDaemon(): bool {
        $daemonPath = $this->config['daemon_path'];
        if (!$daemonPath || !file_exists($daemonPath)) {
            $this->debug("Invalid daemon path: {$daemonPath}");
            return false;
        }
        
        try {
            // Build environment variables
            $env = [
                'REDIS_HOST' => $this->config['redis_host'] ?? '127.0.0.1',
                'REDIS_PORT' => $this->config['redis_port'] ?? '6379',
                'REDIS_AUTH' => $this->config['redis_auth'] ?? '',
                'SITE_ID' => $this->siteId,
                'NODE_ID' => $this->nodeId,
                'STREAM_PREFIX' => $this->config['stream_prefix'],
                'DIMENSIONS' => $this->config['dimensions'] ?? '8',
                'DEBUG' => $this->config['debug'] ? '1' : '0'
            ];
            
            // Build environment string
            $envStr = '';
            foreach ($env as $key => $value) {
                $envStr .= "{$key}='{$value}' ";
            }
            
            // Start daemon in background
            $command = "{$envStr} {$daemonPath} > /dev/null 2>&1 & echo $!";
            $pid = exec($command);
            
            $this->debug("Started GSD daemon with PID: {$pid}");
            
            // Store PID for future reference
            $pidKey = sprintf('{%s}:%s:daemon:pid:%s',
                $this->siteId,
                $this->config['stream_prefix'],
                $this->nodeId
            );
            $this->storage->set($pidKey, $pid);
            
            return true;
        } catch (\Exception $e) {
            $this->debug("Failed to start daemon: {$e->getMessage()}");
            return false;
        }
    }
    
    /**
     * Stop the daemon process
     * 
     * @return bool Success status
     */
    public function stopDaemon(): bool {
        // Get daemon PID
        $pidKey = sprintf('{%s}:%s:daemon:pid:%s',
            $this->siteId,
            $this->config['stream_prefix'],
            $this->nodeId
        );
        $pid = $this->storage->get($pidKey);
        
        if (!$pid) {
            $this->debug("No PID found for daemon");
            return false;
        }
        
        try {
            // Send SIGTERM to daemon
            $command = "kill {$pid} 2>/dev/null || true";
            exec($command);
            
            // Wait for daemon to stop
            $attempts = 0;
            $maxAttempts = $this->config['retry_attempts'];
            $delay = $this->config['retry_delay'];
            
            while ($attempts < $maxAttempts) {
                sleep($delay);
                $attempts++;
                
                // Check if process is still running
                $command = "ps -p {$pid} > /dev/null 2>&1 || echo 'not-running'";
                $result = exec($command);
                
                if ($result === 'not-running') {
                    $this->debug("GSD daemon (PID: {$pid}) stopped successfully");
                    $this->storage->delete($pidKey);
                    $this->connected = false;
                    return true;
                }
            }
            
            // Force kill if still running
            $command = "kill -9 {$pid} 2>/dev/null || true";
            exec($command);
            
            $this->debug("GSD daemon (PID: {$pid}) forcefully terminated");
            $this->storage->delete($pidKey);
            $this->connected = false;
            return true;
        } catch (\Exception $e) {
            $this->debug("Failed to stop daemon: {$e->getMessage()}");
            return false;
        }
    }
    
    /**
     * Get daemon status
     * 
     * @return array Status information
     */
    public function getDaemonStatus(): array {
        // Get daemon PID
        $pidKey = sprintf('{%s}:%s:daemon:pid:%s',
            $this->siteId,
            $this->config['stream_prefix'],
            $this->nodeId
        );
        $pid = $this->storage->get($pidKey);
        
        if (!$pid) {
            return [
                'running' => false,
                'pid' => null,
                'connected' => $this->connected,
                'uptime' => 0
            ];
        }
        
        // Check if process is running
        $command = "ps -p {$pid} -o etime= 2>/dev/null || echo ''";
        $uptime = trim(exec($command));
        
        $running = !empty($uptime);
        
        return [
            'running' => $running,
            'pid' => $pid,
            'connected' => $this->connected,
            'uptime' => $uptime
        ];
    }
    
    /**
     * Send a command to the daemon via ValKey stream
     * 
     * @param string $command Command name
     * @param array $parameters Command parameters
     * @return array|null Response or null on failure
     */
    private function sendCommand(string $command, array $parameters = []): ?array {
        if (!$this->storage->isConnected()) {
            throw new StorageException('Not connected to ValKey server');
        }
        
        // Check if using fallback
        if ($this->usingFallback && $this->fallback) {
            return $this->executeFallbackCommand($command, $parameters);
        }
        
        // Check cache for read-only commands
        $cacheKey = null;
        $useCache = in_array($command, ['findServices', 'getLoadSequence', 'getCapabilityDimensions']);
        
        if ($useCache) {
            $cacheKey = md5($command . json_encode($parameters));
            if (isset($this->responseCache[$cacheKey]) && 
                (time() - $this->responseCache[$cacheKey]['time'] < $this->cacheExpiration)) {
                return $this->responseCache[$cacheKey]['data'];
            }
        }
        
        // Generate a unique request ID
        $requestId = uniqid('req_', true);
        
        // Build the request message with proper JSON encoding of parameters
        $request = [
            'id' => $requestId,
            'command' => $command,
            'parameters' => json_encode($parameters), // Ensure parameters are JSON encoded
            'site_id' => $this->siteId,
            'node_id' => $this->nodeId,
            'timestamp' => (string)microtime(true) // Ensure timestamp is a string
        ];
        
        $this->debug("Sending command: {$command} with ID: {$requestId}");
        
        // Get stream names
        $commandStream = $this->streamName . ':commands';
        $responseStream = $this->streamName . ':responses';
        
        // Add the request to the command stream
        $messageId = $this->storage->xAdd($commandStream, '*', $request);
        $this->debug("Added message with ID: {$messageId}");
        
        // Wait for response on the response stream
        $start = microtime(true);
        $timeoutMs = $this->timeout;
        
        // First check if we already have a response in the stream
        $messages = $this->storage->redis->xRange($responseStream, '-', '+');
        $lastId = "0";
        
        // Look for our response in existing messages
        foreach ($messages as $id => $data) {
            if (isset($data['id']) && $data['id'] === $requestId) {
                $this->debug("Found response in stream: {$id}");
                
                // Delete the message
                $this->storage->xDel($responseStream, [$id]);
                
                // Parse the response data
                if (isset($data['response'])) {
                    $response = json_decode($data['response'], true);
                    
                    // Cache the response for read-only commands
                    if ($useCache && $cacheKey && isset($response['status']) && $response['status'] === 'ok') {
                        $this->responseCache[$cacheKey] = [
                            'time' => time(),
                            'data' => $response
                        ];
                    }
                    
                    return $response;
                }
                
                // If response field is missing, create basic response
                $response = [
                    'id' => $requestId,
                    'status' => 'ok',
                    'result' => [],
                    'timestamp' => microtime(true)
                ];
                
                // Cache the response for read-only commands
                if ($useCache && $cacheKey) {
                    $this->responseCache[$cacheKey] = [
                        'time' => time(),
                        'data' => $response
                    ];
                }
                
                return $response;
            }
            $lastId = $id;
        }
        
        // Wait for new messages
        while ((microtime(true) - $start) * 1000 < $timeoutMs) {
            try {
                // Read new messages, starting from the last ID we saw
                $responses = $this->storage->xRead(
                    [$responseStream => $lastId],
                    10, // Read up to 10 messages
                    100 // 100ms block time
                );
                
                if (!empty($responses) && isset($responses[$responseStream])) {
                    foreach ($responses[$responseStream] as $id => $data) {
                        $lastId = $id; // Update last seen ID
                        
                        if (isset($data['id']) && $data['id'] === $requestId) {
                            $this->debug("Found response with ID: {$id}");
                            
                            // Delete the message
                            $this->storage->xDel($responseStream, [$id]);
                            
                            // Parse the response data
                            if (isset($data['response'])) {
                                $response = json_decode($data['response'], true);
                                
                                // Cache the response for read-only commands
                                if ($useCache && $cacheKey && isset($response['status']) && $response['status'] === 'ok') {
                                    $this->responseCache[$cacheKey] = [
                                        'time' => time(),
                                        'data' => $response
                                    ];
                                }
                                
                                return $response;
                            }
                            
                            // If response field is missing, create basic response
                            $response = [
                                'id' => $requestId,
                                'status' => 'ok',
                                'result' => [],
                                'timestamp' => microtime(true)
                            ];
                            
                            // Cache the response for read-only commands
                            if ($useCache && $cacheKey) {
                                $this->responseCache[$cacheKey] = [
                                    'time' => time(),
                                    'data' => $response
                                ];
                            }
                            
                            return $response;
                        }
                    }
                }
            } catch (\Exception $e) {
                $this->debug("Error reading response: {$e->getMessage()}");
                // Continue trying until timeout
            }
            
            // Short sleep to prevent CPU spinning
            usleep(10000); // 10ms
        }
        
        $this->debug("Command timed out after {$this->config['timeout']} seconds");
        
        // Timeout occurred, fall back to local implementation if available
        if ($this->fallback && $this->config['use_fallback']) {
            $this->debug("Falling back to local implementation after timeout");
            $this->usingFallback = true;
            return $this->executeFallbackCommand($command, $parameters);
        }
        
        return null;
    }
    
    /**
     * Execute a command using the fallback implementation
     * 
     * @param string $command Command name
     * @param array $parameters Command parameters
     * @return array|null Result or null on failure
     */
    private function executeFallbackCommand(string $command, array $parameters = []): ?array {
        if (!$this->fallback) {
            return null;
        }
        
        $this->debug("Executing fallback command: {$command}");
        
        $result = null;
        $status = 'error';
        
        try {
            switch ($command) {
                case 'ping':
                    $result = true;
                    $status = 'ok';
                    break;
                    
                case 'registerCapabilityDimension':
                    $name = $parameters['name'] ?? '';
                    $dimension = $parameters['dimension'] ?? 0;
                    
                    if (empty($name)) {
                        return [
                            'status' => 'error',
                            'error' => 'Invalid name parameter',
                            'timestamp' => microtime(true)
                        ];
                    }
                    
                    $result = $this->fallback->registerCapabilityDimension($name, $dimension);
                    $status = $result ? 'ok' : 'error';
                    break;
                    
                case 'registerService':
                    $id = $parameters['id'] ?? '';
                    $capabilities = $parameters['capabilities'] ?? [];
                    $metadata = $parameters['metadata'] ?? [];
                    
                    if (empty($id)) {
                        return [
                            'status' => 'error',
                            'error' => 'Invalid id parameter',
                            'timestamp' => microtime(true)
                        ];
                    }
                    
                    $result = $this->fallback->registerService($id, $capabilities, $metadata);
                    $status = $result ? 'ok' : 'error';
                    break;
                    
                case 'findServices':
                    $requirements = $parameters['requirements'] ?? [];
                    
                    if (empty($requirements)) {
                        return [
                            'status' => 'error',
                            'error' => 'Invalid requirements parameter',
                            'timestamp' => microtime(true)
                        ];
                    }
                    
                    $result = $this->fallback->findServices($requirements);
                    $status = 'ok';
                    break;
                    
                case 'getLoadSequence':
                    $result = $this->fallback->getLoadSequence();
                    $status = 'ok';
                    break;
                    
                default:
                    return [
                        'status' => 'error',
                        'error' => "Unknown command: {$command}",
                        'timestamp' => microtime(true)
                    ];
            }
            
            return [
                'status' => $status,
                'result' => $result,
                'timestamp' => microtime(true)
            ];
        } catch (\Exception $e) {
            $this->debug("Fallback command error: {$e->getMessage()}");
            
            return [
                'status' => 'error',
                'error' => $e->getMessage(),
                'timestamp' => microtime(true)
            ];
        }
    }
    
    /**
     * Log debug messages
     * 
     * @param string $message Debug message
     */
    private function debug(string $message): void {
        if ($this->config['debug']) {
            error_log("[GSDClient] {$message}");
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
        $response = $this->sendCommand('registerCapabilityDimension', [
            'name' => $name,
            'dimension' => $dimension
        ]);
        
        // Update local cache of capability dimensions
        if ($response && isset($response['status']) && $response['status'] === 'ok') {
            $this->capabilityDimensions[$name] = $dimension;
        }
        
        return $response && isset($response['status']) && $response['status'] === 'ok';
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
        $response = $this->sendCommand('registerService', [
            'id' => $id,
            'capabilities' => $capabilities,
            'metadata' => $metadata
        ]);
        
        return $response && isset($response['status']) && $response['status'] === 'ok';
    }
    
    /**
     * Find services matching requirements
     * 
     * @param array $requirements Array of requirements [name => min_value]
     * @return array Array of service IDs
     */
    public function findServices(array $requirements): array {
        $response = $this->sendCommand('findServices', [
            'requirements' => $requirements
        ]);
        
        if ($response && isset($response['status']) && $response['status'] === 'ok') {
            return $response['result'] ?? [];
        }
        
        return [];
    }
    
    /**
     * Get the load sequence
     * 
     * @return array Array of service IDs in load order
     */
    public function getLoadSequence(): array {
        $response = $this->sendCommand('getLoadSequence', []);
        
        if ($response && isset($response['status']) && $response['status'] === 'ok') {
            return $response['result'] ?? [];
        }
        
        return [];
    }
    
    /**
     * Get registered capability dimensions
     * 
     * @return array Map of capability names to dimensions
     */
    public function getCapabilityDimensions(): array {
        $response = $this->sendCommand('getCapabilityDimensions', []);
        
        if ($response && isset($response['status']) && $response['status'] === 'ok') {
            $this->capabilityDimensions = $response['result'] ?? [];
            return $this->capabilityDimensions;
        }
        
        return $this->capabilityDimensions;
    }
    
    /**
     * Check if client is connected to daemon
     * 
     * @return bool Connection status
     */
    public function isConnected(): bool {
        return $this->connected || $this->usingFallback;
    }
    
    /**
     * Get client status
     * 
     * @return array Status information
     */
    public function getStatus(): array {
        return [
            'connected' => $this->connected,
            'using_fallback' => $this->usingFallback,
            'site_id' => $this->siteId,
            'node_id' => $this->nodeId,
            'stream_name' => $this->streamName,
            'auto_start' => $this->autoStartDaemon,
            'daemon' => $this->getDaemonStatus()
        ];
    }
    
    /**
     * Check if the service is working
     * 
     * @return string Hello message
     */
    public function hello(): string {
        $response = $this->sendCommand('hello', []);
        
        if ($response && isset($response['status']) && $response['status'] === 'ok') {
            return $response['result'] ?? "Hello from GSD Client!";
        }
        
        return "Hello from GSD Client (fallback mode)!";
    }
}