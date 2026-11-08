<?php
// One tenant against another through its own stream. Exit 1 if anything crossed. See attack.sh.
[$_, $port] = $argv;
$r = new Redis(); $r->connect('127.0.0.1', (int)$port);
$tk = '{geodineum}:gnode:constellation'; $fail = 0;
$check = function (string $what, bool $ok) use (&$fail) { printf("  %s %s\n", $ok ? 'ok  ' : 'FAIL', $what); if (!$ok) { $fail++; } };
$send = function (string $from, string $c, array $p, ?string $ss = null) use ($r) {
    $ss = $ss ?? $from; $id = uniqid($from . ':', true);
    $r->xAdd("{{$from}}:gnode:unified:production", '*', ['t' => 'c', 'id' => $id, 'c' => $c, 'ss' => $ss, 'sn' => 'client', 'ts' => (string)(microtime(true) * 1000), 'p' => json_encode($p + ['_request_id' => $id])]);
    $end = microtime(true) + 4;
    while (microtime(true) < $end) {
        foreach (array_unique([$from, $ss]) as $s) {
            $g = $r->get("{{$s}}:res:$id");
            if ($g !== false) { return ['at' => $s, 'reply' => json_decode($g, true)]; }
        }
        usleep(3000);
    }
    return ['at' => null, 'reply' => null];
};
$refused = fn (array $a) => ($a['reply']['status'] ?? '') === 'error' && strpos((string)$a['reply']['error'], 'may ') !== false;
$check('a site registers itself', ($send('site_1', 'register_service', ['id' => 'site_1', 'shape' => 'site', 'environment' => 'production', 'metadata' => ['note' => 'mine']])['reply']['status'] ?? '') === 'ok');
$mine = $r->hGet("$tk:entities", 'site_1');
$check('and it is there', is_string($mine));
foreach ([
    ['another site overwrites it', 'register_service', ['id' => 'site_1', 'shape' => 'site', 'metadata' => ['note' => 'hijack']]],
    ['another site deregisters it', 'deregister_service', ['service_id' => 'site_1']],
    ['another site removes the node', 'deregister_service', ['service_id' => 'bench']],
    ['a site registers a fake service', 'register_service', ['id' => 'evil', 'shape' => 'daemon']],
    ['a site registers itself as a node', 'register_service', ['id' => 'site_2', 'shape' => 'node']],
    ['a site claims a capability', 'register_service', ['id' => 'site_2', 'shape' => 'daemon', 'holds' => ['comms']]],
    ['a site writes another constellation', 'register_service', ['id' => 'site_2', 'shape' => 'site', 'constellation' => 'other']],
    ['a site adds an edge for another', 'topology_edge_add', ['from' => 'site_1', 'to' => 'bench']],
    ['a site opens a channel', 'channel_open', ['target_site' => 'site_1']],
] as [$what, $c, $p]) {
    $check("refused: $what", $refused($send('site_2', $c, $p)));
}
$a = $send('site_2', 'deregister_service', ['service_id' => 'site_1'], 'site_1');
$check('writing the victim as ss changes nothing: still refused', $refused($a));
$check('and the answer lands in the caller\'s namespace', $a['at'] === 'site_2');
$check("the victim's record is untouched", $r->hGet("$tk:entities", 'site_1') === $mine && !$r->hExists("$tk:entities", 'evil') && $r->hExists("$tk:entities", 'bench'));
$keyed = function (string $from, string $target) use ($r) {
    $id = bin2hex(random_bytes(8));
    $r->set("{{$from}}:req:$id", json_encode(['command' => 'deregister_service', 'parameters' => ['service_id' => $target], 'requested_at' => microtime(true), 'timeout_ms' => 2000, 'site_id' => $target]));
    $r->xAdd("{{$from}}:gnode:unified:production", '*', ['t' => 'c', 'id' => $id, 'ss' => $from]);
    usleep(500000);
};
$reg = fn (string $id) => $send($id, 'register_service', ['id' => $id, 'shape' => 'site', 'environment' => 'production']);
$announced = function (string $from, string $target) use ($r) {
    $id = uniqid($from . ':0:', true);
    $r->set("{{$from}}:req:$id", json_encode(['command' => 'deregister_service', 'parameters' => ['service_id' => $target], 'requested_at' => microtime(true), 'timeout_ms' => 2000, 'batch_index' => 0, 'site_id' => $target]));
    $r->xAdd("{{$from}}:gnode:unified:production", '*', ['site_id' => $from, 'type' => 'batch', 'request_ids' => json_encode([$id]), 'count' => '1', 'ts' => (string)(microtime(true) * 1000)]);
    usleep(500000);
};
// An inline t=bc message reaches no handler on either binary, so it cannot be shown here; its dispatch is guarded in the code all the same.
foreach (['key-based request' => $keyed, 'client batch announcement' => $announced] as $name => $way) {
    $reg('site_1'); $reg('site_2');
    $way('site_2', 'site_2');
    $check("control: a $name reaches the handler (a site removes itself)", !$r->hExists("$tk:entities", 'site_2'));
    $reg('site_1'); $reg('site_2');
    $way('site_2', 'site_1');
    $check("a $name cannot remove another site", $r->hExists("$tk:entities", 'site_1'));
}
$reg('site_1'); $reg('site_2');
$id = bin2hex(random_bytes(8));
$r->set("{geodineum}:req:$id", json_encode(['command' => 'deregister_service', 'parameters' => ['service_id' => 'site_1'], 'requested_at' => microtime(true), 'timeout_ms' => 2000, 'site_id' => 'geodineum']));
$r->xAdd('{site_2}:gnode:unified:production', '*', ['t' => 'c', 'id' => $id, 'ss' => 'geodineum']);
usleep(500000);
$check('a key-based request naming the operator as ss stays a tenant', $r->hExists("$tk:entities", 'site_1'));
$reg('site_1');
// ---- an entry with no type -------------------------------------------------------------------
// t is required: such an entry is no request, and nothing it names may be answered or overwritten.
$untyped = bin2hex(random_bytes(8));
$r->setex("{site_2}:res:$untyped", 30, 'the real answer');
$r->xAdd('{site_2}:gnode:unified:production', '*', ['id' => $untyped, 'status' => 'ok', 'result' => '{}']);
$bare = bin2hex(random_bytes(8));
$r->xAdd('{site_2}:gnode:unified:production', '*', ['id' => $bare]);
usleep(800000);
$check('an untyped entry does not overwrite the answer keyed under its id', $r->get("{site_2}:res:$untyped") === 'the real answer');
$check('an untyped id gets no answer at all', $r->exists("{site_2}:res:$bare") == 0);
// ---- forged replies ------------------------------------------------------------------------
$U1 = '{site_1}:gnode:unified:production'; $C1 = '{site_1}:gnode:comms:production';
$r->xAdd($C1, '*', ['subject' => 'mark']); $before = $r->xLen($C1);
$r->xAdd('{site_2}:gnode:unified:production', '*', ['t' => 'r', 'ri' => 'forge-1', 'st' => 'ok', 'r' => '{"paid":true}', 'ss' => 'site_2', 'sn' => 'x', '_rr' => $U1]);
$r->xAdd('{site_2}:gnode:unified:production', '*', ['status' => 'ok', 'id' => 'forge-2', 'type' => 'alert', 'subject' => 'forged', '_rr' => $C1]);
$r->xAdd('{site_2}:gnode:unified:production', '*', ['status' => 'ok', 'id' => 'forge-3', '_rr' => '{geodineum}:gnode:forged']);
usleep(800000);
$check("a forged reply never lands in the victim's answers", $r->get('{site_1}:res:forge-1') === false);
$check("a forged reply never reaches the victim's mail queue", $r->xLen($C1) === $before);
$check('a forged reply creates no shared stream', !$r->exists('{geodineum}:gnode:forged'));

// ---- a real relay still gets home (the control) ---------------------------------------------
$op = $send('geodineum', 'register_service', ['id' => 'svc', 'shape' => 'daemon', 'environment' => 'production', 'axes' => ['domain' => ['notification']], 'on' => 'bench']);
$check('control: the operator\'s stream registers a provider', ($op['reply']['status'] ?? '') === 'ok');
$id = uniqid('site_1:', true);
sleep(62); // the daemon discovers the provider's placement stream on its next sweep of streams
$ms = (int)(microtime(true) * 1000);
$r->set('{geodineum}:gnode:heartbeat:production:svc:bench', json_encode(['ts' => $ms, 'comp' => 'svc', 'node' => 'bench']), ['ex' => 120]);
$r->rawCommand('FCALL', 'GNODE_TOPO_HEALTH_DERIVE', 1, $tk, (string)$ms, '60000');
$r->xAdd('{site_1}:gnode:unified:production', '*', ['t' => 'c', 'id' => $id, 'c' => 'ping', 'ss' => 'site_1', 'sn' => 'client', 'ts' => (string)(microtime(true) * 1000),
    '_rt' => '{"layer":"3.3","facets":{"domain":["notification"]}}', 'p' => json_encode(['secret' => 'S1', '_request_id' => $id])]);
$got = null; $on = null; $end = microtime(true) + 6;
while ($got === null && microtime(true) < $end) {
    $it = null;
    while (($keys = $r->scan($it, '{svc}:gnode:unified*', 100)) !== false) {
        foreach ($keys as $k) { foreach ($r->xRevRange($k, '+', '-', 5) as $e) { if (($e['_rr'] ?? '') !== '' && strpos(json_encode($e), 'S1') !== false) { $got = $e; $on = $k; } } }
        if ($it === 0) { break; }
    }
    usleep(100000);
}
$check('control: the command was relayed to the provider', $got !== null);
if ($got === null) { echo '    answer: ', substr((string)$r->get("{site_1}:res:$id"), 0, 240), "\n    keys: ", json_encode($r->keys('{svc}*')), ' ', json_encode($r->keys('*@bench*')), "\n"; }
if ($got !== null) {
    $rid = $got['ri'] ?? $got['id'];
    $r->xAdd('{site_2}:gnode:unified:production', '*', ['t' => 'r', 'ri' => $rid, 'st' => 'ok', 'r' => '{"who":"thief"}', '_rr' => $got['_rr']]);
    usleep(500000);
    $check('another party cannot answer a real relay', $r->get("{site_1}:res:$rid") === false);
    $r->xAdd($on, '*', ['t' => 'r', 'ri' => $rid, 'st' => 'ok', 'r' => '{"who":"svc"}', '_rr' => $got['_rr']]);
    $ans = false; $end = microtime(true) + 4;
    while ($ans === false && microtime(true) < $end) { $ans = $r->get("{site_1}:res:$rid"); usleep(50000); }
    $check('control: the provider\'s real answer gets home', is_string($ans) && strpos($ans, 'svc') !== false);
}

// ---- shared CMS state ------------------------------------------------------------------------
$s1 = $send('site_1', 'template_fragment', ['template_id' => 'home', 'content' => 'mine {{ a }}', 'variables' => (object)[], 'ttl' => 600]);
$check('control: a site stores a template', ($s1['reply']['status'] ?? '') === 'ok');
$send('site_2', 'template_fragment', ['template_id' => 'home', 'content' => '<script>attacker</script>', 'variables' => (object)[], 'ttl' => 600]);
$rend = json_encode($send('site_1', 'render_template', ['template_id' => 'home', 'variables' => ['a' => 'x']])['reply']);
$check("another site cannot overwrite a site's template", strpos($rend, 'attacker') === false && strpos($rend, 'mine x') !== false);
$check('nor reach it through a list include', $refused($send('site_2', 'template_fragment', ['template_id' => 'peek', 'content' => '{% include ["site_1__home"] %}', 'variables' => (object)[], 'ttl' => 60])));
$check("nor render it", strpos(json_encode($send('site_2', 'render_template', ['template_id' => 'site_1__home', 'variables' => ['a' => 'x']])['reply']), 'mine') === false);
$cs = $send('site_1', 'content_store', ['key' => 'c1', 'content' => 'private-of-site-1', 'ttl' => 600]);
$check('control: a site stores content', ($cs['reply']['status'] ?? '') === 'ok');
$own = $send('site_1', 'content_retrieve', ['key' => 'c1']);
$check('control: and reads it back', strpos(json_encode($own['reply']), 'private-of-site-1') !== false);
$check('another site does not read it', strpos(json_encode($send('site_2', 'content_retrieve', ['key' => 'c1'])['reply']), 'private-of-site-1') === false);
$check('nor by naming the key in full', strpos(json_encode($send('site_2', 'content_retrieve', ['key' => '{site_1}:cache:c1'])['reply']), 'private-of-site-1') === false);
$send('site_2', 'content_store', ['key' => 'c1', 'content' => '<script>attacker</script>', 'ttl' => 600]);
$check('and cannot overwrite it', strpos(json_encode($send('site_1', 'content_retrieve', ['key' => 'c1'])['reply']), 'attacker') === false);

// ---- reading what is not yours ---------------------------------------------------------------
foreach (['stream_info', 'stream_group_info', 'stream_consumer_info', 'stream_pending'] as $c) {
    $check("refused: $c on another site's stream", $refused($send('site_2', $c, ['stream' => $U1, 'group' => 'gnode-daemon'])));
}
$check('control: a site reads its own stream', ($send('site_2', 'stream_info', ['stream' => '{site_2}:gnode:unified:production'])['reply']['status'] ?? '') === 'ok');
foreach ([['topo_create', ['name' => 'mine', 'topology_type' => 'custom']], ['registry_list', []], ['channel_list', []],
          ['discover_with_endpoints', ['layer' => '3', 'endpoint_registry' => '{site_1}:gnode:endpoints']],
          ['register_service', ['id' => 'site_2', 'shape' => 'daemon', 'axes' => ['domain' => ['notification']]]],
          ['register_service', ['id' => 'site_2', 'shape' => 'site', 'on' => 'bench']]] as [$c, $p]) {
    $check('refused: ' . $c . ' ' . json_encode(array_keys($p)), $refused($send('site_2', $c, $p)));
}
$env = $send('site_2', 'render_string', ['template' => '{{ get_env(name="HOME") }}', 'variables' => (object)[]]);
$check('a template cannot read the daemon\'s environment', strpos(json_encode($env['reply']), getenv('HOME') ?: '/home/') === false);
$ren = $send('site_2', 'render_string', ['template' => 'hi {{ a }}', 'variables' => ['a' => 'x']]);
$check('control: a template still renders', strpos(json_encode($ren['reply']), 'hi x') !== false);

$check('a site deregisters itself', ($send('site_1', 'deregister_service', ['service_id' => 'site_1'])['reply']['result']['status'] ?? '') === 'deregistered');
$check('ordinary commands still answer', ($send('site_2', 'ping', [])['reply']['status'] ?? '') === 'ok');
echo $fail ? "$fail FAILED\n" : "ALL PASSED\n"; exit($fail ? 1 : 0);
