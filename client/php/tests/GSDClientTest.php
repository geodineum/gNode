<?php
namespace GSD\Tests;

use GSD\GSDClient;
use GSD\Storage\RedisStorage;
use PHPUnit\Framework\TestCase;

class GSDClientTest extends TestCase {
    private $client;
    
    protected function setUp(): void {
        $storage = new RedisStorage([
            'host' => '127.0.0.1',
            'port' => 6379
        ]);
        
        $this->client = new GSDClient(
            $storage,
            'test',
            'phpunit',
            [
                'debug' => true,
                'use_fallback' => true
            ]
        );
    }
    
    public function testPing() {
        $this->assertTrue($this->client->ping());
    }
    
    // Add more tests here
}
