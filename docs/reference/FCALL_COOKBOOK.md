# FCALL Cookbook: Polyglot Integration Reference

```yaml
@PRIME: gNode-FCALL|ValKey-functions|XADD-XREADGROUP|multi-language|
        service-topology|geometric-discover|staleness|health-stream|
        circuit-breaker|distributed-tracing|pub-sub|cache-ops
@AUDIENCE: developers-integrating-with-gNode-from-any-language
@PREREQUISITE: CLAUDE.md(§2-§8)|running-gNode-daemon|registered-site
```

---

## §0 SYSTEM IDENTITY

This cookbook provides **copy-pasteable** examples for the 10 most common gNode operations in 6 languages: CLI (valkey-cli), Python (redis-py), Node.js (ioredis), PHP (gNode-Client + raw), Rust (redis-rs), and Go (go-redis).

All examples assume:
- ValKey running on `127.0.0.1:47445`
- ACL user `gnode_client_{service_id}` for application access
- Site ID: `staging_my_app` (replace with your site)
- Environment: `production`

---

## §1 CONNECTION

### Authentication

gNode uses ACL-based auth. **Never** use the `--pass` CLI flag with long hex passwords — use the `REDISCLI_AUTH` environment variable.

**CLI**:
```bash
# ALWAYS use the secure wrapper script
./scripts/valkey-cli-secure.sh PING

# Or manually (for one-off commands):
export REDISCLI_AUTH="$(cat .gnode/valkey_client_staging_my_app.password)"
valkey-cli -p 47445 --user gnode_client_staging_my_app PING
```

**Python (redis-py)**:
```python
import redis

r = redis.Redis(
    host='127.0.0.1',
    port=47445,
    username='gnode_client_staging_my_app',
    password=open('.gnode/valkey_client_staging_my_app.password').read().strip(),
    decode_responses=True
)
assert r.ping()
```

**Node.js (ioredis)**:
```javascript
const Redis = require('ioredis');
const fs = require('fs');

const client = new Redis({
  host: '127.0.0.1',
  port: 47445,
  username: 'gnode_client_staging_my_app',
  password: fs.readFileSync('.gnode/valkey_client_staging_my_app.password', 'utf8').trim(),
});
```

**PHP (gNode-Client)**:
```php
// gNode-Client handles connection automatically via site.env
$gnode = new \gNode\Client\GNodeClient();
// All FCALL/XADD operations go through $gnode->execute($command, $params)
```

**Rust (redis-rs)**:
```rust
use redis::Client;

let password = std::fs::read_to_string(".gnode/valkey_client_staging_my_app.password")
    .expect("read password")
    .trim()
    .to_string();
let client = Client::open(format!(
    "redis://gnode_client_staging_my_app:{}@127.0.0.1:47445",
    password
))?;
let mut conn = client.get_connection()?;
```

**Go (go-redis)**:
```go
import "github.com/redis/go-redis/v9"

password, _ := os.ReadFile(".gnode/valkey_client_staging_my_app.password")
rdb := redis.NewClient(&redis.Options{
    Addr:     "127.0.0.1:47445",
    Username: "gnode_client_staging_my_app",
    Password: strings.TrimSpace(string(password)),
})
```

---

## §2 Register Entity

Register a service into the service-tier topology (30D = 25 discovery + 5 storage). The daemon pre-computes Q64.64 bucket keys and z_scores, but you can also register directly via FCALL if you pre-compute those values.

**Preferred**: Send `register_service` command via stream (daemon computes Q64.64).

**CLI**:
```bash
# Via stream command (recommended — daemon computes bucket_key + z_score)
./scripts/valkey-cli-secure.sh XADD staging_my_app:gnode:unified:production '*' \
  id "reg-$(date +%s)" \
  cmd "register_service" \
  params '{"id":"MyService","capabilities":{"domain_primary":0.7,"service_tier":0.30,"protocol":0.5},"metadata":{"type":"worker","version":"1.0"}}' \
  _cr "1"
```

**Direct FCALL** (advanced — requires pre-computed bucket_key and z_score):
```bash
./scripts/valkey-cli-secure.sh FCALL GNODE_REGISTER_CAPABILITY_VECTOR 1 \
  "{staging_my_app}:gnode:services" \
  "MyService" \
  '{"pr":[2147483648,0,0,0,0,0,0,0,3006477107,0,0,0,0,0,0,0,0,1288490189,0,0,0,0,0],"pd":[0.5,0,0,0,0,0,0,0,0.7,0,0,0,0,0,0,0,0,0.3,0,0,0,0,0],"c":{"domain_primary":0.7,"service_tier":0.30,"protocol":0.5},"m":{"type":"worker"}}' \
  "0005000000000000000700000000000000000000000000000000000000000000000000000003" \
  1288490189
```

**Python**:
```python
import json, time

# Via stream (recommended)
r.xadd('staging_my_app:gnode:unified:production', {
    'id': f'reg-{int(time.time())}',
    'cmd': 'register_service',
    'params': json.dumps({
        'id': 'MyService',
        'capabilities': {'domain_primary': 0.7, 'service_tier': 0.30},
        'metadata': {'type': 'worker'}
    }),
    '_cr': '1'
})
```

**Node.js**:
```javascript
await client.xadd('staging_my_app:gnode:unified:production', '*',
  'id', `reg-${Date.now()}`,
  'cmd', 'register_service',
  'params', JSON.stringify({
    id: 'MyService',
    capabilities: { domain_primary: 0.7, service_tier: 0.30 },
    metadata: { type: 'worker' }
  }),
  '_cr', '1'
);
```

**PHP (gNode-Client)**:
```php
$gnode->execute('register_service', [
    'id' => 'MyService',
    'capabilities' => ['domain_primary' => 0.7, 'service_tier' => 0.30],
    'metadata' => ['type' => 'worker'],
]);
```

**Rust**:
```rust
use redis::Commands;

redis::cmd("XADD")
    .arg("staging_my_app:gnode:unified:production")
    .arg("*")
    .arg("id").arg(format!("reg-{}", chrono::Utc::now().timestamp()))
    .arg("cmd").arg("register_service")
    .arg("params").arg(serde_json::json!({
        "id": "MyService",
        "capabilities": {"domain_primary": 0.7, "service_tier": 0.30},
        "metadata": {"type": "worker"}
    }).to_string())
    .arg("_cr").arg("1")
    .query::<String>(&mut conn)?;
```

**Go**:
```go
params, _ := json.Marshal(map[string]interface{}{
    "id":           "MyService",
    "capabilities": map[string]float64{"domain_primary": 0.7, "service_tier": 0.30},
    "metadata":     map[string]string{"type": "worker"},
})
rdb.XAdd(ctx, &redis.XAddArgs{
    Stream: "staging_my_app:gnode:unified:production",
    Values: map[string]interface{}{
        "id":     fmt.Sprintf("reg-%d", time.Now().Unix()),
        "cmd":    "register_service",
        "params": string(params),
        "_cr":    "1",
    },
}).Result()
```

---

## §3 Discover by Capability

Find services matching capability requirements using O(1) spatial-hash discovery.

**CLI**:
```bash
./scripts/valkey-cli-secure.sh XADD staging_my_app:gnode:unified:production '*' \
  id "disc-$(date +%s)" \
  cmd "geometric_discover" \
  params '{"capabilities":{"domain_primary":0.7,"throughput_tier":0.5},"limit":5}' \
  _cr "1"
```

**Python**:
```python
r.xadd('staging_my_app:gnode:unified:production', {
    'id': f'disc-{int(time.time())}',
    'cmd': 'geometric_discover',
    'params': json.dumps({
        'capabilities': {'domain_primary': 0.7, 'throughput_tier': 0.5},
        'limit': 5
    }),
    '_cr': '1'
})
```

**Node.js**:
```javascript
await client.xadd('staging_my_app:gnode:unified:production', '*',
  'id', `disc-${Date.now()}`,
  'cmd', 'geometric_discover',
  'params', JSON.stringify({
    capabilities: { domain_primary: 0.7, throughput_tier: 0.5 },
    limit: 5
  }),
  '_cr', '1'
);
```

**PHP (gNode-Client)**:
```php
$results = $gnode->execute('geometric_discover', [
    'capabilities' => ['domain_primary' => 0.7, 'throughput_tier' => 0.5],
    'limit' => 5,
]);
```

**Rust**:
```rust
use redis::Commands;

redis::cmd("XADD")
    .arg("staging_my_app:gnode:unified:production")
    .arg("*")
    .arg("id").arg(format!("disc-{}", chrono::Utc::now().timestamp()))
    .arg("cmd").arg("geometric_discover")
    .arg("params").arg(serde_json::json!({
        "capabilities": {"domain_primary": 0.7, "throughput_tier": 0.5},
        "limit": 5
    }).to_string())
    .arg("_cr").arg("1")
    .query::<String>(&mut conn)?;
```

**Go**:
```go
params, _ := json.Marshal(map[string]interface{}{
    "capabilities": map[string]float64{"domain_primary": 0.7, "throughput_tier": 0.5},
    "limit":        5,
})
rdb.XAdd(ctx, &redis.XAddArgs{
    Stream: "staging_my_app:gnode:unified:production",
    Values: map[string]interface{}{
        "id":     fmt.Sprintf("disc-%d", time.Now().Unix()),
        "cmd":    "geometric_discover",
        "params": string(params),
        "_cr":    "1",
    },
}).Result()
```

---

## §4 Describe Entity

Get detailed description of a registered service entity: tier, capabilities, edges, health.

**CLI (via stream)**:
```bash
./scripts/valkey-cli-secure.sh XADD staging_my_app:gnode:unified:production '*' \
  id "desc-$(date +%s)" \
  cmd "service_describe" \
  params '{"entity_id":"MyService"}' \
  _cr "1"
```

**CLI (direct FCALL — raw entity without enrichment)**:
```bash
./scripts/valkey-cli-secure.sh FCALL GNODE_TOPO_GET_ENTITY 1 \
  "{staging_my_app}:gnode:services" "MyService"
```

**Python**:
```python
# Via stream (recommended — includes tier classification + health)
r.xadd('staging_my_app:gnode:unified:production', {
    'id': f'desc-{int(time.time())}',
    'cmd': 'service_describe',
    'params': json.dumps({'entity_id': 'MyService'}),
    '_cr': '1'
})

# Direct FCALL (raw entity data only)
raw = r.fcall('GNODE_TOPO_GET_ENTITY', 1,
    '{staging_my_app}:gnode:services', 'MyService')
entity = json.loads(raw)
```

**Node.js**:
```javascript
// Via stream
await client.xadd('staging_my_app:gnode:unified:production', '*',
  'id', `desc-${Date.now()}`,
  'cmd', 'service_describe',
  'params', JSON.stringify({ entity_id: 'MyService' }),
  '_cr', '1'
);

// Direct FCALL
const raw = await client.call('FCALL', 'GNODE_TOPO_GET_ENTITY', 1,
  '{staging_my_app}:gnode:services', 'MyService');
```

**PHP (gNode-Client)**:
```php
// Via gNode-Client
$info = $gnode->execute('service_describe', ['entity_id' => 'MyService']);

// Direct FCALL
$raw = $gnode->fcall('GNODE_TOPO_GET_ENTITY', ['{staging_my_app}:gnode:services'], ['MyService']);
```

**Rust**:
```rust
use redis::Commands;

// Via stream (recommended — includes tier classification + health)
redis::cmd("XADD")
    .arg("staging_my_app:gnode:unified:production")
    .arg("*")
    .arg("id").arg(format!("desc-{}", chrono::Utc::now().timestamp()))
    .arg("cmd").arg("service_describe")
    .arg("params").arg(serde_json::json!({"entity_id": "MyService"}).to_string())
    .arg("_cr").arg("1")
    .query::<String>(&mut conn)?;

// Direct FCALL (raw entity data only)
let raw: String = redis::cmd("FCALL")
    .arg("GNODE_TOPO_GET_ENTITY")
    .arg(1)
    .arg("{staging_my_app}:gnode:services")
    .arg("MyService")
    .query(&mut conn)?;
let entity: serde_json::Value = serde_json::from_str(&raw)?;
```

**Go**:
```go
// Via stream (recommended — includes tier classification + health)
params, _ := json.Marshal(map[string]string{"entity_id": "MyService"})
rdb.XAdd(ctx, &redis.XAddArgs{
    Stream: "staging_my_app:gnode:unified:production",
    Values: map[string]interface{}{
        "id":     fmt.Sprintf("desc-%d", time.Now().Unix()),
        "cmd":    "service_describe",
        "params": string(params),
        "_cr":    "1",
    },
}).Result()

// Direct FCALL (raw entity data only)
raw, err := rdb.FCall(ctx, "GNODE_TOPO_GET_ENTITY",
    []string{"{staging_my_app}:gnode:services"},
    "MyService",
).Result()
```

---

## §5 Send Command via Stream

The universal pattern for sending any command to gNode via the unified stream.

**Stream key format**: `{service_id}:gnode:unified:{environment}`

**Fields**:
| Field | Required | Description |
|-------|----------|-------------|
| `id`  | Yes | Unique command ID (client-generated) |
| `cmd` | Yes | Command name (case-insensitive) |
| `params` | Yes | JSON-encoded parameters |
| `_gh` | No | Routing hint (e.g., `"inference"`) |
| `_cr` | No | Set to `"1"` to include response in stream |

**CLI**:
```bash
./scripts/valkey-cli-secure.sh XADD staging_my_app:gnode:unified:production '*' \
  id "cmd-$(date +%s)" \
  cmd "ping" \
  params '{}' \
  _cr "1"
```

**Python**:
```python
cmd_id = f'cmd-{int(time.time() * 1000)}'
r.xadd('staging_my_app:gnode:unified:production', {
    'id': cmd_id,
    'cmd': 'status',
    'params': '{}',
    '_cr': '1'
})
```

**Node.js**:
```javascript
const cmdId = `cmd-${Date.now()}`;
await client.xadd('staging_my_app:gnode:unified:production', '*',
  'id', cmdId,
  'cmd', 'status',
  'params', '{}',
  '_cr', '1'
);
```

**Rust**:
```rust
use redis::Commands;

let cmd_id = format!("cmd-{}", chrono::Utc::now().timestamp_millis());
redis::cmd("XADD")
    .arg("staging_my_app:gnode:unified:production")
    .arg("*")
    .arg("id").arg(&cmd_id)
    .arg("cmd").arg("status")
    .arg("params").arg("{}")
    .arg("_cr").arg("1")
    .query::<String>(&mut conn)?;
```

**Go**:
```go
cmdId := fmt.Sprintf("cmd-%d", time.Now().UnixMilli())
rdb.XAdd(ctx, &redis.XAddArgs{
    Stream: "staging_my_app:gnode:unified:production",
    Values: map[string]interface{}{
        "id":     cmdId,
        "cmd":    "status",
        "params": "{}",
        "_cr":    "1",
    },
}).Result()
```

---

## §6 Read Response

Read responses from the unified stream using consumer groups. Responses have an `id` field matching the command ID and a `status` of `"ok"` or `"error"`.

**CLI** (one-shot read, newest entries):
```bash
./scripts/valkey-cli-secure.sh XREVRANGE staging_my_app:gnode:unified:production + - COUNT 5
```

**Python**:
```python
# Create consumer group (once)
try:
    r.xgroup_create('staging_my_app:gnode:unified:production', 'my-app', id='0', mkstream=True)
except redis.ResponseError:
    pass  # Group already exists

# Read responses
responses = r.xreadgroup(
    'my-app', 'consumer-1',
    {'staging_my_app:gnode:unified:production': '>'},
    count=10,
    block=5000  # 5s timeout
)

for stream, messages in responses:
    for msg_id, fields in messages:
        if 'status' in fields:
            # This is a response
            print(f"Response {fields['id']}: {fields['status']}")
            if 'result' in fields:
                result = json.loads(fields['result'])
        r.xack('staging_my_app:gnode:unified:production', 'my-app', msg_id)
```

**Node.js**:
```javascript
// Create consumer group (once)
try {
  await client.xgroup('CREATE', 'staging_my_app:gnode:unified:production',
    'my-app', '0', 'MKSTREAM');
} catch (e) { /* already exists */ }

// Read responses
const results = await client.xreadgroup(
  'GROUP', 'my-app', 'consumer-1',
  'COUNT', 10, 'BLOCK', 5000,
  'STREAMS', 'staging_my_app:gnode:unified:production', '>'
);
```

**PHP (gNode-Client)**:
```php
// gNode-Client handles XREADGROUP internally
$response = $gnode->execute('ping', []);
// Returns parsed response directly
```

**Rust**:
```rust
use redis::Commands;

// Create consumer group (once)
let _: Result<(), _> = redis::cmd("XGROUP")
    .arg("CREATE")
    .arg("staging_my_app:gnode:unified:production")
    .arg("my-app")
    .arg("0")
    .arg("MKSTREAM")
    .query(&mut conn);

// Read responses
let results: redis::Value = redis::cmd("XREADGROUP")
    .arg("GROUP").arg("my-app").arg("consumer-1")
    .arg("COUNT").arg(10)
    .arg("BLOCK").arg(5000)
    .arg("STREAMS").arg("staging_my_app:gnode:unified:production")
    .arg(">")
    .query(&mut conn)?;

// ACK after processing
redis::cmd("XACK")
    .arg("staging_my_app:gnode:unified:production")
    .arg("my-app")
    .arg(msg_id)
    .query::<i64>(&mut conn)?;
```

**Go**:
```go
// Create consumer group (once)
rdb.XGroupCreateMkStream(ctx,
    "staging_my_app:gnode:unified:production",
    "my-app", "0").Err()

// Read responses
streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
    Group:    "my-app",
    Consumer: "consumer-1",
    Streams:  []string{"staging_my_app:gnode:unified:production", ">"},
    Count:    10,
    Block:    5 * time.Second,
}).Result()

for _, stream := range streams {
    for _, msg := range stream.Messages {
        if status, ok := msg.Values["status"]; ok {
            fmt.Printf("Response %s: %s\n", msg.Values["id"], status)
        }
        // ACK after processing
        rdb.XAck(ctx, "staging_my_app:gnode:unified:production",
            "my-app", msg.ID).Result()
    }
}
```

---

## §7 Publish Broadcast

Publish a message to a pub/sub channel with history and rate limiting.

**CLI**:
```bash
./scripts/valkey-cli-secure.sh FCALL GNODE_PUBSUB_PUBLISH 1 \
  "staging_my_app:pubsub" \
  "topology_updates" \
  '{"event":"service_registered","service_id":"MyService","timestamp":1708234567}'
```

**Python**:
```python
result = r.fcall('GNODE_PUBSUB_PUBLISH', 1,
    'staging_my_app:pubsub',
    'topology_updates',
    json.dumps({
        'event': 'service_registered',
        'service_id': 'MyService',
        'timestamp': int(time.time())
    })
)
```

**Node.js**:
```javascript
const result = await client.call('FCALL', 'GNODE_PUBSUB_PUBLISH', 1,
  'staging_my_app:pubsub',
  'topology_updates',
  JSON.stringify({
    event: 'service_registered',
    service_id: 'MyService',
    timestamp: Date.now()
  })
);
```

**PHP (gNode-Client)**:
```php
$gnode->fcall('GNODE_PUBSUB_PUBLISH', ['staging_my_app:pubsub'], [
    'topology_updates',
    json_encode(['event' => 'service_registered', 'service_id' => 'MyService'])
]);
```

**Rust**:
```rust
use redis::Commands;

let message = serde_json::json!({
    "event": "service_registered",
    "service_id": "MyService",
    "timestamp": std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
});
let result: String = redis::cmd("FCALL")
    .arg("GNODE_PUBSUB_PUBLISH")
    .arg(1)
    .arg("staging_my_app:pubsub")
    .arg("topology_updates")
    .arg(message.to_string())
    .query(&mut conn)?;
```

**Go**:
```go
message, _ := json.Marshal(map[string]interface{}{
    "event":      "service_registered",
    "service_id": "MyService",
    "timestamp":  time.Now().Unix(),
})
result, err := rdb.FCall(ctx, "GNODE_PUBSUB_PUBLISH",
    []string{"staging_my_app:pubsub"},
    "topology_updates",
    string(message),
).Result()
```

---

## §8 Cache Get/Set

Atomic cache operations with optional TTL.

**CLI**:
```bash
# Set with 300s TTL
./scripts/valkey-cli-secure.sh FCALL GNODE_CACHE_SET 1 \
  "staging_my_app:cache" "user:123:profile" '{"name":"Alice"}' 300

# Get
./scripts/valkey-cli-secure.sh FCALL GNODE_CACHE_GET 1 \
  "staging_my_app:cache" "user:123:profile"
```

**Python**:
```python
# Set
r.fcall('GNODE_CACHE_SET', 1,
    'staging_my_app:cache',
    'user:123:profile',
    json.dumps({'name': 'Alice'}),
    300  # TTL seconds
)

# Get
cached = r.fcall('GNODE_CACHE_GET', 1,
    'staging_my_app:cache',
    'user:123:profile'
)
if cached:
    data = json.loads(cached)
```

**Node.js**:
```javascript
// Set
await client.call('FCALL', 'GNODE_CACHE_SET', 1,
  'staging_my_app:cache', 'user:123:profile',
  JSON.stringify({ name: 'Alice' }), 300);

// Get
const cached = await client.call('FCALL', 'GNODE_CACHE_GET', 1,
  'staging_my_app:cache', 'user:123:profile');
```

**PHP (gNode-Client)**:
```php
// Set
$gnode->fcall('GNODE_CACHE_SET', ['staging_my_app:cache'], [
    'user:123:profile', json_encode(['name' => 'Alice']), 300
]);

// Get
$cached = $gnode->fcall('GNODE_CACHE_GET', ['staging_my_app:cache'], [
    'user:123:profile'
]);
```

**Rust**:
```rust
use redis::Commands;

// Set with 300s TTL
redis::cmd("FCALL")
    .arg("GNODE_CACHE_SET")
    .arg(1)
    .arg("staging_my_app:cache")
    .arg("user:123:profile")
    .arg(serde_json::json!({"name": "Alice"}).to_string())
    .arg(300)
    .query::<String>(&mut conn)?;

// Get
let cached: Option<String> = redis::cmd("FCALL")
    .arg("GNODE_CACHE_GET")
    .arg(1)
    .arg("staging_my_app:cache")
    .arg("user:123:profile")
    .query(&mut conn)?;
if let Some(val) = cached {
    let data: serde_json::Value = serde_json::from_str(&val)?;
}
```

**Go**:
```go
// Set with 300s TTL
rdb.FCall(ctx, "GNODE_CACHE_SET",
    []string{"staging_my_app:cache"},
    "user:123:profile",
    `{"name":"Alice"}`,
    300,
).Result()

// Get
cached, err := rdb.FCall(ctx, "GNODE_CACHE_GET",
    []string{"staging_my_app:cache"},
    "user:123:profile",
).Result()
if err == nil {
    var data map[string]interface{}
    json.Unmarshal([]byte(fmt.Sprint(cached)), &data)
}
```

---

## §9 Circuit Breaker

Three-state circuit breaker (CLOSED → OPEN → HALF_OPEN) for resilient service communication.

**CLI**:
```bash
# Check if circuit is open (returns JSON with state)
./scripts/valkey-cli-secure.sh FCALL GNODE_RESILIENCE_CIRCUIT_CHECK 1 \
  "staging_my_app:resilience" "payment-gateway"

# Record success (closes circuit if half-open)
./scripts/valkey-cli-secure.sh FCALL GNODE_RESILIENCE_CIRCUIT_SUCCESS 1 \
  "staging_my_app:resilience" "payment-gateway"

# Record failure (may trip circuit open)
./scripts/valkey-cli-secure.sh FCALL GNODE_RESILIENCE_CIRCUIT_FAILURE 1 \
  "staging_my_app:resilience" "payment-gateway"

```

**Python**:
```python
# Check before calling external service
state = json.loads(r.fcall('GNODE_RESILIENCE_CIRCUIT_CHECK', 1,
    'staging_my_app:resilience', 'payment-gateway'))

if state.get('allowed'):
    try:
        result = call_payment_gateway()
        r.fcall('GNODE_RESILIENCE_CIRCUIT_SUCCESS', 1,
            'staging_my_app:resilience', 'payment-gateway')
    except Exception:
        r.fcall('GNODE_RESILIENCE_CIRCUIT_FAILURE', 1,
            'staging_my_app:resilience', 'payment-gateway')
else:
    # Circuit is OPEN — use fallback
    result = use_cached_response()
```

**Node.js**:
```javascript
const state = JSON.parse(await client.call('FCALL', 'GNODE_RESILIENCE_CIRCUIT_CHECK', 1,
  'staging_my_app:resilience', 'payment-gateway'));

if (state.allowed) {
  try {
    const result = await callPaymentGateway();
    await client.call('FCALL', 'GNODE_RESILIENCE_CIRCUIT_SUCCESS', 1,
      'staging_my_app:resilience', 'payment-gateway');
  } catch (e) {
    await client.call('FCALL', 'GNODE_RESILIENCE_CIRCUIT_FAILURE', 1,
      'staging_my_app:resilience', 'payment-gateway');
  }
}
```

**PHP (gNode-Client)**:
```php
$state = json_decode($gnode->fcall('GNODE_RESILIENCE_CIRCUIT_CHECK',
    ['staging_my_app:resilience'], ['payment-gateway']), true);

if ($state['allowed']) {
    try {
        $result = callPaymentGateway();
        $gnode->fcall('GNODE_RESILIENCE_CIRCUIT_SUCCESS',
            ['staging_my_app:resilience'], ['payment-gateway']);
    } catch (\Exception $e) {
        $gnode->fcall('GNODE_RESILIENCE_CIRCUIT_FAILURE',
            ['staging_my_app:resilience'], ['payment-gateway']);
    }
}
```

**Rust**:
```rust
use redis::Commands;

// Check before calling external service
let state_json: String = redis::cmd("FCALL")
    .arg("GNODE_RESILIENCE_CIRCUIT_CHECK")
    .arg(1)
    .arg("staging_my_app:resilience")
    .arg("payment-gateway")
    .query(&mut conn)?;
let state: serde_json::Value = serde_json::from_str(&state_json)?;

if state["allowed"].as_bool().unwrap_or(false) {
    match call_payment_gateway() {
        Ok(result) => {
            redis::cmd("FCALL")
                .arg("GNODE_RESILIENCE_CIRCUIT_SUCCESS")
                .arg(1)
                .arg("staging_my_app:resilience")
                .arg("payment-gateway")
                .query::<String>(&mut conn)?;
        }
        Err(_) => {
            redis::cmd("FCALL")
                .arg("GNODE_RESILIENCE_CIRCUIT_FAILURE")
                .arg(1)
                .arg("staging_my_app:resilience")
                .arg("payment-gateway")
                .query::<String>(&mut conn)?;
        }
    }
}
```

**Go**:
```go
// Check before calling external service
stateJSON, err := rdb.FCall(ctx, "GNODE_RESILIENCE_CIRCUIT_CHECK",
    []string{"staging_my_app:resilience"},
    "payment-gateway",
).Result()

var state map[string]interface{}
json.Unmarshal([]byte(fmt.Sprint(stateJSON)), &state)

if allowed, ok := state["allowed"].(bool); ok && allowed {
    if err := callPaymentGateway(); err == nil {
        rdb.FCall(ctx, "GNODE_RESILIENCE_CIRCUIT_SUCCESS",
            []string{"staging_my_app:resilience"},
            "payment-gateway",
        ).Result()
    } else {
        rdb.FCall(ctx, "GNODE_RESILIENCE_CIRCUIT_FAILURE",
            []string{"staging_my_app:resilience"},
            "payment-gateway",
        ).Result()
    }
}
```


## §10 Health Update

High-frequency health metrics sent to the health stream. Uses compressed field names for bandwidth.

**Stream key**: `{service_id}:gnode:health` (ONE per site, no environment suffix)

**Fields**:
| Field | Description |
|-------|-------------|
| `t`   | Type: `"lu"` (load_update) |
| `si`  | Service ID |
| `l`   | Load factor (0.0-1.0) |
| `cpu` | CPU usage (0.0-1.0) |
| `mem` | Memory usage (0.0-1.0) |
| `rq`  | Active request count |
| `lat` | Avg latency (ms) |
| `err` | Error rate (0.0-1.0) |
| `ts`  | Unix timestamp (seconds) |

**CLI**:
```bash
./scripts/valkey-cli-secure.sh XADD staging_my_app:gnode:health '*' \
  t "lu" \
  si "MyService" \
  l "0.35" \
  cpu "0.42" \
  mem "0.28" \
  rq "12" \
  lat "8" \
  err "0.001" \
  ts "$(date +%s)"
```

**Python**:
```python
r.xadd('staging_my_app:gnode:health', {
    't': 'lu',
    'si': 'MyService',
    'l': '0.35',
    'cpu': '0.42',
    'mem': '0.28',
    'rq': '12',
    'lat': '8',
    'err': '0.001',
    'ts': str(int(time.time()))
})
```

**Node.js**:
```javascript
await client.xadd('staging_my_app:gnode:health', '*',
  't', 'lu',
  'si', 'MyService',
  'l', '0.35',
  'cpu', '0.42',
  'mem', '0.28',
  'rq', '12',
  'lat', '8',
  'err', '0.001',
  'ts', Math.floor(Date.now() / 1000).toString()
);
```

**PHP (gNode-Client)**:
```php
// gNode-Client provides a high-level helper
$gnode->sendHealthUpdate([
    'service_id' => 'MyService',
    'load' => 0.35,
    'cpu' => 0.42,
    'memory' => 0.28,
    'active_requests' => 12,
    'avg_latency_ms' => 8,
    'error_rate' => 0.001,
]);
```

**Rust**:
```rust
redis::cmd("XADD")
    .arg("staging_my_app:gnode:health")
    .arg("*")
    .arg("t").arg("lu")
    .arg("si").arg("MyService")
    .arg("l").arg("0.35")
    .arg("cpu").arg("0.42")
    .arg("mem").arg("0.28")
    .arg("rq").arg("12")
    .arg("lat").arg("8")
    .arg("err").arg("0.001")
    .arg("ts").arg(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string())
    .query::<String>(&mut conn)?;
```

**Go**:
```go
rdb.XAdd(ctx, &redis.XAddArgs{
    Stream: "staging_my_app:gnode:health",
    Values: map[string]interface{}{
        "t":   "lu",
        "si":  "MyService",
        "l":   "0.35",
        "cpu": "0.42",
        "mem": "0.28",
        "rq":  "12",
        "lat": "8",
        "err": "0.001",
        "ts":  fmt.Sprintf("%d", time.Now().Unix()),
    },
}).Result()
```

---

## Quick Reference Table

| Operation | Method | Key/Stream |
|-----------|--------|------------|
| Register entity | `XADD` cmd=`register_service` | `{service_id}:gnode:unified:{env}` |
| Discover | `XADD` cmd=`geometric_discover` | `{service_id}:gnode:unified:{env}` |
| Describe entity | `XADD` cmd=`service_describe` | `{service_id}:gnode:unified:{env}` |
| Raw entity get | `FCALL GNODE_TOPO_GET_ENTITY` | `{service_id}:gnode:services` |
| Send command | `XADD` | `{service_id}:gnode:unified:{env}` |
| Read response | `XREADGROUP` | `{service_id}:gnode:unified:{env}` |
| Pub/sub publish | `FCALL GNODE_PUBSUB_PUBLISH` | `{service_id}:pubsub` |
| Cache get/set | `FCALL GNODE_CACHE_GET/SET` | `{service_id}:cache` |
| Circuit breaker | `FCALL GNODE_CIRCUIT_*` | `{service_id}:resilience` |
| Trace span | `FCALL GNODE_SPAN_*` | `{service_id}:tracing` |
| Health update | `XADD` | `{service_id}:gnode:health` |
