<?php
/**
 * gNode Daemon Health Check Script
 * 
 * This script checks the health of the gNode daemon and associated consumer groups,
 * providing diagnostic information and fixing issues when possible.
 * 
 * Usage: php check-daemon-health.php [--start-if-down] [--site-id=default] [--node-id=default] [--fix-consumer] [--debug]
 * 
 * Options:
 *   --start-if-down    Start the daemon if it's not running
 *   --site-id=<id>     Site ID to check (default: default)
 *   --node-id=<id>     Node ID to check (default: default)
 *   --fix-consumer     Fix consumer group issues if detected
 *   --debug            Show detailed debug information
 */

// Check for Composer autoloader
$autoloadPaths = [
    __DIR__ . '/../vendor/autoload.php', // Standard location
    __DIR__ . '/../../../autoload.php',  // Global installation
];

$loaded = false;
foreach ($autoloadPaths as $path) {
    if (file_exists($path)) {
        require_once $path;
        $loaded = true;
        break;
    }
}

if (!$loaded) {
    echo "ERROR: Composer autoloader not found. Please run 'composer install' in the project root.\n";
    exit(1);
}

// This script may need to be updated to work with the new gNode-Client repository
// For now, we'll just use basic Redis commands instead of the client classes

// Simulate the client code for the health check
class BasicRedis {
    private $redis;
    
    public function __construct($config) {
        $this->redis = new \Redis();
        $this->redis->connect($config['host'], $config['port']);
    }
    
    public function ping() {
        return $this->redis->ping();
    }
    
    public function exists($key) {
        return $this->redis->exists($key);
    }
    
    public function getScriptSHAs($siteId) {
        $scripts_key = "{{$siteId}}:gcore:gnode:scripts";
        return $this->redis->hGetAll($scripts_key) ?: [];
    }
}

// Parse command line arguments
$options = getopt('', ['start-if-down', 'site-id::', 'node-id::', 'fix-consumer', 'debug']);

$startIfDown = isset($options['start-if-down']);
$siteId = $options['site-id'] ?? 'default';
$nodeId = $options['node-id'] ?? 'default';
$fixConsumer = isset($options['fix-consumer']);
$debug = isset($options['debug']);

echo "gNode Daemon Health Check\n";
echo "======================\n";
echo "Site ID:    $siteId\n";
echo "Node ID:    $nodeId\n";
echo "Start if down: " . ($startIfDown ? "Yes" : "No") . "\n";
echo "Fix consumer:  " . ($fixConsumer ? "Yes" : "No") . "\n";
echo "Debug mode:    " . ($debug ? "Yes" : "No") . "\n\n";

// Step 1: Create basic Redis client
$redis = new BasicRedis([
    'host' => '127.0.0.1',
    'port' => 47445
]);

// Step 2: Check Redis connection
try {
    $ping = $redis->ping();
    echo "ValKey/Redis connection: " . ($ping ? "OK" : "Failed") . "\n";
    
    if (!$ping) {
        echo "ERROR: Cannot connect to ValKey/Redis. Please check if it's running.\n";
        exit(1);
    }
} catch (\Exception $e) {
    echo "ERROR: ValKey/Redis connection failed: " . $e->getMessage() . "\n";
    exit(1);
}

// Step 3: Check for script SHAs in ValKey/Redis
$scriptShas = $redis->getScriptSHAs($siteId);
echo "Script SHAs in ValKey/Redis: " . count($scriptShas) . "\n";

if (empty($scriptShas)) {
    echo "WARNING: No script SHAs found. This is normal if using direct mode.\n";
    
    // Check for direct mode flag
    if ($redis->exists("{{$siteId}}:gnode:direct_mode")) {
        echo "NOTICE: Direct mode is enabled for ValKey compatibility.\n";
        echo "This means the daemon is using direct Redis commands instead of Lua scripts.\n";
    } else {
        echo "WARNING: No script SHAs found and direct mode is not enabled.\n";
        echo "The daemon might not be running properly.\n";
    }
}

// Step 4: Check for command and response streams
$streamPrefix = getenv('STREAM_PREFIX') ?: 'gnode';
$commandStream = sprintf('{%s}:%s:stream:%s:commands', $siteId, $streamPrefix, $nodeId);
$responseStream = sprintf('{%s}:%s:stream:%s:responses', $siteId, $streamPrefix, $nodeId);

$commandStreamExists = $redis->exists($commandStream);
$responseStreamExists = $redis->exists($responseStream);

echo "Stream Status\n";
echo "============\n";
echo "Command Stream:  $commandStream\n";
echo "Response Stream: $responseStream\n";
echo "Command Stream Exists:  " . ($commandStreamExists ? "Yes" : "No") . "\n";
echo "Response Stream Exists: " . ($responseStreamExists ? "Yes" : "No") . "\n\n";

// Step 5: Check if daemon process is running
$daemonRunning = false;
exec("pgrep -f gnode-daemon", $output, $return);
if ($return === 0 && !empty($output)) {
    echo "gNode Daemon process: Running (PID: " . implode(', ', $output) . ")\n";
    $daemonRunning = true;
    
    // For daemon responsiveness, we can check if the direct mode flag exists
    // This isn't a perfect test, but at least indicates that daemon has communicated with Redis
    if ($redis->exists("{{$siteId}}:gnode:direct_mode")) {
        echo "gNode Daemon responsive: Yes (direct mode enabled)\n";
    } else {
        // If no direct mode flag, it might still be working with scripts
        echo "gNode Daemon potentially responsive (can't verify without client library)\n";
    }
    
    // Check consumer groups and active consumers if daemon is running
    if ($commandStreamExists && $daemonRunning) {
        // Use the Redis instance directly
        $nativeRedis = new \Redis();
        $nativeRedis->connect('127.0.0.1', 47445);
        
        // Check consumer groups
        $commandGroupExists = false;
        $responseGroupExists = false;
        
        try {
            $groups = $nativeRedis->xInfo('GROUPS', $commandStream);
            foreach ($groups as $group) {
                if ($group['name'] === 'gnode-daemon') {
                    $commandGroupExists = true;
                    break;
                }
            }
        } catch (\Exception $e) {
            // Group info failed, likely because the group doesn't exist
        }
        
        try {
            $groups = $nativeRedis->xInfo('GROUPS', $responseStream);
            foreach ($groups as $group) {
                if ($group['name'] === 'gnode-client') {
                    $responseGroupExists = true;
                    break;
                }
            }
        } catch (\Exception $e) {
            // Group info failed, likely because the group doesn't exist
        }
        
        echo "\nConsumer Group Status\n";
        echo "====================\n";
        echo "Command Group ('gnode-daemon') Exists:  " . ($commandGroupExists ? "Yes" : "No") . "\n";
        echo "Response Group ('gnode-client') Exists: " . ($responseGroupExists ? "Yes" : "No") . "\n\n";
        
        // Check if consumer exists and is active
        $consumers = [];
        $activeConsumers = [];
        $daemonConsumerActive = false;
        $processorConsumerActive = false;
        
        try {
            if ($commandGroupExists) {
                $consumers = $nativeRedis->xInfo('CONSUMERS', $commandStream, 'gnode-daemon');
                
                echo "Consumers for Command Stream\n";
                echo "===========================\n";
                if (empty($consumers)) {
                    echo "No consumers found for command stream\n";
                } else {
                    foreach ($consumers as $consumer) {
                        $name = $consumer['name'] ?? 'unknown';
                        $pending = $consumer['pending'] ?? 0;
                        $idle = $consumer['idle'] ?? 0;
                        $active = $idle < 30000; // Active if seen in last 30 seconds
                        
                        if ($active) {
                            $activeConsumers[] = $name;
                        }
                        
                        // Check for both consumer naming patterns
                        if ($name === "daemon-$nodeId") {
                            $daemonConsumerActive = $active;
                        } elseif ($name === "processor-$nodeId") {
                            $processorConsumerActive = $active;
                        }
                        
                        echo "Consumer: $name\n";
                        echo "  Pending: $pending messages\n";
                        echo "  Idle:    " . ($idle / 1000) . " seconds\n";
                        echo "  Active:  " . ($active ? "Yes" : "No") . "\n";
                    }
                }
                echo "\n";
            }
        } catch (\Exception $e) {
            echo "Failed to get consumer info: " . $e->getMessage() . "\n\n";
        }
        
        // Check stream status
        try {
            echo "Stream Details\n";
            echo "=============\n";
            
            // Command stream details
            $streamInfo = $nativeRedis->xInfo('STREAM', $commandStream);
            $length = $streamInfo['length'] ?? 0;
            $lastId = $streamInfo['last-generated-id'] ?? '0-0';
            
            echo "Command Stream Length: $length messages\n";
            echo "Last Message ID:       $lastId\n";
            
            // Calculate consumer lag if groups exist
            if ($commandGroupExists) {
                $groups = $nativeRedis->xInfo('GROUPS', $commandStream);
                foreach ($groups as $group) {
                    if ($group['name'] === 'gnode-daemon') {
                        $lastDeliveredId = $group['last-delivered-id'] ?? '0-0';
                        $pendingMessages = $group['pending'] ?? 0;
                        $consumerCount = $group['consumers'] ?? 0;
                        
                        // Calculate lag
                        $lag = 0;
                        if ($lastDeliveredId !== '0-0' && $lastId !== '0-0') {
                            $lastIdParts = explode('-', $lastId);
                            $deliveredIdParts = explode('-', $lastDeliveredId);
                            
                            if (isset($lastIdParts[0]) && isset($deliveredIdParts[0])) {
                                $lag = (int)$lastIdParts[0] - (int)$deliveredIdParts[0];
                            }
                        }
                        
                        echo "Group 'gnode-daemon':\n";
                        echo "  Last Delivered ID: $lastDeliveredId\n";
                        echo "  Pending Messages:  $pendingMessages\n";
                        echo "  Consumer Count:    $consumerCount\n";
                        echo "  Lag:               $lag messages\n";
                    }
                }
            }
            
            echo "\n";
        } catch (\Exception $e) {
            echo "Failed to get stream info: " . $e->getMessage() . "\n\n";
        }
    }
} else {
    echo "gNode Daemon process: Not running\n";
    $daemonRunning = false;
}

// Step 6: Start the daemon if requested and not running
if (!$daemonRunning && $startIfDown) {
    echo "\nAttempting to start gNode Daemon...\n";
    
    $daemonPath = __DIR__ . '/../daemon/target/release/gnode-daemon';
    if (!file_exists($daemonPath)) {
        echo "ERROR: Daemon binary not found at $daemonPath\n";
        echo "Please build the daemon first with: ./scripts/build.sh\n";
        exit(1);
    }
    
    // Make sure logs directory exists
    $logsDir = __DIR__ . '/../logs';
    if (!is_dir($logsDir)) {
        mkdir($logsDir, 0755, true);
    }
    
    $logFile = $logsDir . '/gnode-daemon.log';
    $startCommand = sprintf(
        'RUST_LOG=info nohup %s --site-id %s --node-id %s --debug > %s 2>&1 & echo $!',
        $daemonPath,
        escapeshellarg($siteId),
        escapeshellarg($nodeId),
        $logFile
    );
    
    echo "Executing: $startCommand\n";
    exec($startCommand, $output, $return);
    
    if ($return === 0 && !empty($output)) {
        $pid = trim($output[0]);
        echo "gNode Daemon started with PID: $pid\n";
        
        // Wait for daemon to initialize
        echo "Waiting for daemon to initialize...\n";
        sleep(2);
        
        // Verify daemon is actually running
        exec("ps -p $pid", $psOutput, $psReturn);
        if ($psReturn === 0) {
            echo "Daemon process verified running.\n";
            
            // Check if we can connect now
            try {
                $tempClient = new \gCore\gNode\gNodeClient(
                    $storage,
                    $siteId,
                    'health-check-' . uniqid(),
                    [
                        'timeout' => 5.0,
                        'use_fallback' => false
                    ]
                );
                
                $connected = $tempClient->isConnected();
                echo "gNode Daemon connection: " . ($connected ? "Successful" : "Failed") . "\n";
                
                if ($connected) {
                    // Check scripts again in case the daemon loaded them
                    $scriptShas = ScriptManagerFactory::getScriptShas($storage, $siteId, true);
                    echo "Script SHAs after daemon start: " . count($scriptShas) . "\n";
                } else {
                    echo "WARNING: Started the daemon process but cannot connect to it.\n";
                }
            } catch (\Exception $e) {
                echo "ERROR: Failed to connect to started daemon: " . $e->getMessage() . "\n";
            }
        } else {
            echo "ERROR: Started daemon process but it's no longer running.\n";
            echo "Check the log file at: $logFile\n";
        }
    } else {
        echo "ERROR: Failed to start daemon.\n";
    }
}

// Step 7: Summary
echo "\nSummary\n";
echo "=======\n";
echo "ValKey/Redis: " . ($ping ? "OK" : "FAIL") . "\n";

// Check for direct mode and adjust script check accordingly
$directMode = $redis->exists("{{$siteId}}:gnode:direct_mode");
if ($directMode) {
    echo "Scripts: N/A (Direct mode enabled)\n";
} else {
    echo "Scripts: " . (count($scriptShas) > 0 ? "OK (" . count($scriptShas) . " scripts)" : "FAIL (No scripts)") . "\n";
}

echo "Command Stream: " . ($commandStreamExists ? "OK" : "MISSING") . "\n";
echo "Response Stream: " . ($responseStreamExists ? "OK" : "MISSING") . "\n";
echo "Daemon Process: " . ($daemonRunning ? "OK" : "NOT RUNNING") . "\n";

// Check if consumer groups exist and active if daemon is running
if ($daemonRunning && isset($commandGroupExists) && isset($responseGroupExists)) {
    echo "Command Group: " . ($commandGroupExists ? "OK" : "MISSING") . "\n";
    echo "Response Group: " . ($responseGroupExists ? "OK" : "MISSING") . "\n";
    
    if (isset($activeConsumers)) {
        echo "Active Consumers: " . (empty($activeConsumers) ? "NONE" : "OK (" . count($activeConsumers) . ")") . "\n";
    }
}

// Detect issues
$issues = [];

if (!$daemonRunning) {
    $issues[] = "Daemon is not running";
}

if (!$commandStreamExists || !$responseStreamExists) {
    $issues[] = "One or both streams don't exist";
}

if ($daemonRunning && isset($commandGroupExists) && isset($responseGroupExists)) {
    if (!$commandGroupExists || !$responseGroupExists) {
        $issues[] = "One or both consumer groups don't exist";
    }
    
    if (isset($activeConsumers) && empty($activeConsumers)) {
        $issues[] = "No active consumers for command stream";
    } elseif (isset($daemonConsumerActive) && isset($processorConsumerActive) && 
              !$daemonConsumerActive && !$processorConsumerActive) {
        $issues[] = "No active consumers with expected names (daemon-$nodeId or processor-$nodeId)";
    }
}

// If issues detected and fix-consumer specified, fix them
if (!empty($issues) && $fixConsumer) {
    echo "\nIssues Found\n";
    echo "============\n";
    foreach ($issues as $issue) {
        echo "- $issue\n";
    }
    echo "\n";
    
    echo "Fixing Issues\n";
    echo "============\n";
    
    // Fix issues based on what was detected
    if (!$daemonRunning && $startIfDown) {
        echo "Daemon will be started automatically.\n";
    }
    
    // Create Redis instance for fixes if it doesn't exist
    if (!isset($nativeRedis)) {
        $nativeRedis = new \Redis();
        $nativeRedis->connect('127.0.0.1', 47445);
    }
    
    // Create streams if needed
    if (!$commandStreamExists) {
        try {
            $nativeRedis->xAdd($commandStream, '*', ['init' => 1]);
            echo "Created command stream\n";
            $commandStreamExists = true;
        } catch (\Exception $e) {
            echo "Failed to create command stream: " . $e->getMessage() . "\n";
        }
    }
    
    if (!$responseStreamExists) {
        try {
            $nativeRedis->xAdd($responseStream, '*', ['init' => 1]);
            echo "Created response stream\n";
            $responseStreamExists = true;
        } catch (\Exception $e) {
            echo "Failed to create response stream: " . $e->getMessage() . "\n";
        }
    }
    
    // Create/reset consumer groups
    if ($commandStreamExists) {
        try {
            $nativeRedis->xGroup('DESTROY', $commandStream, 'gnode-daemon');
        } catch (\Exception $e) {
            // Group might not exist, ignore
        }
        
        try {
            $nativeRedis->xGroup('CREATE', $commandStream, 'gnode-daemon', '$', true);
            echo "Reset command stream consumer group 'gnode-daemon'\n";
        } catch (\Exception $e) {
            echo "Failed to create command consumer group: " . $e->getMessage() . "\n";
        }
    }
    
    if ($responseStreamExists) {
        try {
            $nativeRedis->xGroup('DESTROY', $responseStream, 'gnode-client');
        } catch (\Exception $e) {
            // Group might not exist, ignore
        }
        
        try {
            $nativeRedis->xGroup('CREATE', $responseStream, 'gnode-client', '$', true);
            echo "Reset response stream consumer group 'gnode-client'\n";
        } catch (\Exception $e) {
            echo "Failed to create response consumer group: " . $e->getMessage() . "\n";
        }
    }
    
    if ($daemonRunning) {
        echo "\nRestarting daemon to apply fixes...\n";
        $stopScript = __DIR__ . '/stop-daemon.sh';
        exec($stopScript);
        sleep(1);
        
        // Remove debug log file if it exists
        $debugLogFile = dirname(__DIR__) . '/daemon/daemon-debug.log';
        if (file_exists($debugLogFile)) {
            echo "Removing old debug log file...\n";
            unlink($debugLogFile);
        }
        
        $startScript = __DIR__ . '/start-daemon.sh';
        exec($startScript, $output, $return);
        
        if ($return === 0) {
            echo "Daemon restarted successfully.\n";
        } else {
            echo "Failed to restart daemon: " . implode("\n", $output) . "\n";
        }
    }
    
    echo "\nPlease run this script again to verify fixes.\n";
    exit(0);
} elseif (!empty($issues)) {
    echo "\nIssues Found\n";
    echo "============\n";
    foreach ($issues as $issue) {
        echo "- $issue\n";
    }
    
    echo "\nUse --fix-consumer to automatically fix these issues.\n";
    exit(1);
}

// All checks passed
// Add diagnostic functions if in debug mode
if ($debug && $daemonRunning && $commandStreamExists) {
    // Create Redis instance for diagnostics if it doesn't exist
    if (!isset($nativeRedis)) {
        $nativeRedis = new \Redis();
        $nativeRedis->connect('127.0.0.1', 47445);
    }
    echo "\nRunning Diagnostics\n";
    echo "==================\n";
    
    // Test sending a message to command stream
    echo "Sending test message to command stream...\n";
    try {
        $msgId = $nativeRedis->xAdd($commandStream, '*', [
            'id' => uniqid(),
            'command' => 'diagnostic_test',
            'parameters' => json_encode(['timestamp' => time()]),
            'site_id' => $siteId,
            'node_id' => $nodeId,
            'timestamp' => time()
        ]);
        
        echo "Message sent with ID: $msgId\n";
        
        // Wait briefly for processing
        sleep(2);
        
        // Check if message was processed
        $pending = $nativeRedis->xPending($commandStream, 'gnode-daemon');
        echo "Pending messages: " . $pending[0] . "\n";
        
        // If we have pending messages, check details
        if ($pending[0] > 0) {
            $pendingDetails = $nativeRedis->xPending($commandStream, 'gnode-daemon', '-', '+', 10);
            echo "Pending message details:\n";
            foreach ($pendingDetails as $detail) {
                echo "  ID: " . $detail[0] . "\n";
                echo "  Consumer: " . $detail[1] . "\n";
                echo "  Idle time: " . ($detail[2] / 1000) . " seconds\n";
                echo "  Deliveries: " . $detail[3] . "\n";
            }
        }
    } catch (\Exception $e) {
        echo "Failed to run diagnostic test: " . $e->getMessage() . "\n";
    }
}

// Set exit code based on health status
$isHealthy = $ping && ($directMode || count($scriptShas) > 0) && $commandStreamExists && $daemonRunning;

// Display connection information for the client
if ($isHealthy) {
    echo "\nConnection Information for gNode:\n";
    echo "=============================\n";
    echo "Site ID: $siteId\n";
    echo "Node ID: $nodeId\n";
    echo "Command Stream: $commandStream\n";
    
    if ($directMode) {
        echo "Mode: Direct (ValKey compatibility)\n";
    } else {
        echo "Mode: Script-based\n";
        echo "Script SHAs: " . count($scriptShas) . " scripts available\n";
    }
    
    echo "\ngNode Health: HEALTHY\n";
    echo "No issues found.\n";
    exit(0);
} else {
    exit(1);
}