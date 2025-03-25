<?php
/**
 * Basic usage example for GSD client
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use GSD\GSDClient;
use GSD\Storage\RedisStorage;

// Create Redis storage
$storage = new RedisStorage([
    'host' => '127.0.0.1',
    'port' => 6379
]);

// Create GSD client
$client = new GSDClient(
    $storage,
    'example',  // site ID
    'example1', // node ID
    [
        'stream_prefix' => 'gsd',
        'debug' => true,
        'use_fallback' => true
    ]
);

// Check connection
if (!$client->isConnected()) {
    die("Not connected to GSD daemon\n");
}

// Register a capability dimension
$client->registerCapabilityDimension('performance', 0);
$client->registerCapabilityDimension('reliability', 1);

// Register a service
$result = $client->registerService('example-service', [
    'performance' => 0.9,
    'reliability' => 0.8
], [
    'description' => 'Example service',
    'version' => '1.0'
]);

echo "Service registration: " . ($result ? "success" : "failed") . "\n";

// Find services
$services = $client->findServices([
    'performance' => 0.7
]);

echo "Found services: " . implode(', ', $services) . "\n";

// Get load sequence
$sequence = $client->getLoadSequence();

echo "Load sequence: " . implode(' -> ', $sequence) . "\n";
