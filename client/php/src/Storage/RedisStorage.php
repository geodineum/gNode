<?php
namespace GSD\Storage;

/**
 * Redis Storage Implementation
 */
class RedisStorage implements StorageInterface {
    private $redis;
    private $isConnected = false;
    
    public function __construct(array $config = []) {
        $this->config = array_merge([
            'host' => '127.0.0.1',
            'port' => 6379,
            'auth' => '',
            'database' => 0,
            'timeout' => 2.0,
            'prefix' => '',
        ], $config);
        
        $this->connect();
    }
    
    private function connect() {
        try {
            $this->redis = new \Redis();
            $connected = $this->redis->connect(
                $this->config['host'],
                $this->config['port'],
                $this->config['timeout']
            );
            
            if (!$connected) {
                throw new \Exception("Failed to connect to Redis server at {$this->config['host']}:{$this->config['port']}");
            }
            
            if (!empty($this->config['auth'])) {
                $this->redis->auth($this->config['auth']);
            }
            
            if ($this->config['database'] > 0) {
                $this->redis->select($this->config['database']);
            }
            
            if (!empty($this->config['prefix'])) {
                $this->redis->setOption(\Redis::OPT_PREFIX, $this->config['prefix']);
            }
            
            $this->isConnected = true;
        } catch (\Exception $e) {
            error_log("Redis connection error: " . $e->getMessage());
            $this->isConnected = false;
        }
    }
    
    public function isConnected(): bool {
        if (!$this->isConnected || !$this->redis) {
            return false;
        }
        
        try {
            return $this->redis->ping() === true || $this->redis->ping() === '+PONG';
        } catch (\Exception $e) {
            return false;
        }
    }
    
    public function set(string $key, $value, int $ttl = 0): bool {
        try {
            if ($ttl > 0) {
                return $this->redis->setex($key, $ttl, $value);
            }
            return $this->redis->set($key, $value);
        } catch (\Exception $e) {
            error_log("Redis set error: " . $e->getMessage());
            return false;
        }
    }
    
    public function get(string $key) {
        try {
            return $this->redis->get($key);
        } catch (\Exception $e) {
            error_log("Redis get error: " . $e->getMessage());
            return null;
        }
    }
    
    public function delete(string $key): bool {
        try {
            return $this->redis->del($key) > 0;
        } catch (\Exception $e) {
            error_log("Redis delete error: " . $e->getMessage());
            return false;
        }
    }
    
    public function exists(string $key): bool {
        try {
            return $this->redis->exists($key) > 0;
        } catch (\Exception $e) {
            error_log("Redis exists error: " . $e->getMessage());
            return false;
        }
    }
    
    public function keys(string $pattern): array {
        try {
            return $this->redis->keys($pattern);
        } catch (\Exception $e) {
            error_log("Redis keys error: " . $e->getMessage());
            return [];
        }
    }
    
    public function xAdd(string $key, string $id, array $fields): string {
        try {
            return $this->redis->xAdd($key, $id, $fields);
        } catch (\Exception $e) {
            error_log("Redis xAdd error: " . $e->getMessage());
            throw $e;
        }
    }
    
    public function xGroupCreate(string $key, string $group, string $id, bool $mkstream = false): bool {
        try {
            if ($mkstream && !$this->redis->exists($key)) {
                $this->redis->xAdd($key, '*', ['init' => 'stream']);
            }
            
            $this->redis->xGroup('CREATE', $key, $group, $id, $mkstream);
            return true;
        } catch (\Exception $e) {
            if (strpos($e->getMessage(), 'BUSYGROUP') !== false) {
                return true;
            }
            error_log("Redis xGroupCreate error: " . $e->getMessage());
            return false;
        }
    }
    
    public function xRead(array $streams, int $count = null, int $block = null): array {
        try {
            if ($count !== null && $block !== null) {
                return $this->redis->xRead($streams, $count, $block);
            } elseif ($count !== null) {
                return $this->redis->xRead($streams, $count);
            } else {
                return $this->redis->xRead($streams);
            }
        } catch (\Exception $e) {
            error_log("Redis xRead error: " . $e->getMessage());
            return [];
        }
    }
    
    public function xDel(string $key, array $ids): int {
        try {
            return $this->redis->xDel($key, $ids);
        } catch (\Exception $e) {
            error_log("Redis xDel error: " . $e->getMessage());
            return 0;
        }
    }
}
