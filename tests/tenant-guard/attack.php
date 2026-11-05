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
    $r->xAdd("{{$from}}:gnode:unified:production", '*', ['id' => $id, 'ss' => $from]);
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
$reg('site_1');
$check('a site deregisters itself', ($send('site_1', 'deregister_service', ['service_id' => 'site_1'])['reply']['result']['status'] ?? '') === 'deregistered');
$check('ordinary commands still answer', ($send('site_2', 'ping', [])['reply']['status'] ?? '') === 'ok');
echo $fail ? "$fail FAILED\n" : "ALL PASSED\n"; exit($fail ? 1 : 0);
