<?php
// php driver.php <port> <site> <n>: sends money_sum the way gNode-Client does and prints round trips (ms).
[$_, $port, $site, $n] = $argv;
$r = new Redis(); $r->connect('127.0.0.1', (int)$port);
$out = [];
for ($i = 0; $i < (int)$n; $i++) {
    $id = uniqid($site . ':', true);
    $t = microtime(true);
    $r->xAdd("{{$site}}:gnode:unified:production", '*', ['t' => 'c', 'id' => $id, 'c' => 'money_sum', 'ss' => $site, 'sn' => 'client', 'ts' => (string)($t * 1000),
        'p' => json_encode(['currency' => 'EUR', 'values' => ['299.00', '62.79'], '_request_id' => $id])]);
    $key = "{{$site}}:res:$id"; $poll = 0.001; $end = $t + 5; $got = false;
    while (microtime(true) < $end) {
        if (($got = $r->get($key)) !== false) { $r->del($key); break; }
        $poll = min($poll * 2, 0.01); usleep((int)($poll * 1e6));
    }
    if ($got === false) { $out[] = -1; continue; }
    if ($i === 0) { fwrite(STDERR, substr($got, 0, 120) . "\n"); }
    $out[] = (microtime(true) - $t) * 1000;
    usleep(2000);
}
$ok = array_values(array_filter($out, fn ($v) => $v >= 0)); sort($ok); $c = count($ok);
printf("n=%d lost=%d min=%.1f p50=%.1f p95=%.1f max=%.1f\n", (int)$n, (int)$n - $c, $c ? $ok[0] : 0, $c ? $ok[intdiv($c, 2)] : 0, $c ? $ok[(int)($c * 0.95)] : 0, $c ? $ok[$c - 1] : 0);
