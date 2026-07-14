<?php
declare(strict_types=1);
/**
 * check-command-schema.php — drift-checker for COMMAND_SCHEMA.md.
 *
 * COMMAND_SCHEMA.md is a hand-curated reference: its Parameters/Returns/Keys/Args
 * prose is more readable than the raw JSON Schemas the daemon carries, so it is
 * NOT regenerated. Instead this asserts the doc stays in sync with the code that
 * actually ships — the same "code wins" rule as every generated index, applied to
 * a prose doc via verification rather than regeneration.
 *
 * Two machine-truths are compared against the doc:
 *   Part 1 (commands)  — `gnode-daemon dump-schema` emits the compiled command
 *                        inventory (every registered token). The doc's command +
 *                        alias columns must be exactly that token set.
 *   Part 2 (functions) — Σ `function_name =` across daemon/functions/gnode_*.lua.
 *                        Each library section's rows + "— N functions" heading
 *                        must match its file.
 * Plus the header totals ("N commands, M Lua libraries (K functions)").
 *
 * Reflects the build: a base binary yields 60 commands; one built with the CMS
 * extension staged into GNODE_EXT_DIR yields 83 — run the checker against the
 * same build the doc describes.
 *
 *   php scripts/check-command-schema.php                 # base surface
 *   php scripts/check-command-schema.php --bin=/path/to/gnode-daemon
 *
 * Exit 0 = in sync; exit 1 = drift (details printed). No ValKey needed.
 */

$ROOT = dirname(__DIR__);
$DOC  = "$ROOT/COMMAND_SCHEMA.md";
$LUA_DIR = "$ROOT/daemon/functions";

$binOpt = null;
foreach ($argv as $a) if (preg_match('/^--bin=(.+)$/', $a, $m)) $binOpt = $m[1];

if (!is_readable($DOC)) fail_hard("COMMAND_SCHEMA.md not found at $DOC");

$problems = [];
$note = function (string $section, string $msg) use (&$problems) { $problems[] = "[$section] $msg"; };

// ---- locate a binary that supports dump-schema ----------------------------
// A stale binary predating dump-schema exits 2 (unknown subcommand); probe each
// candidate and use the first that returns the expected JSON. Newest-first so a
// fresh debug build wins over an old release binary.
$candidates = $binOpt !== null
    ? [$binOpt]
    : ["$ROOT/daemon/target/release/gnode-daemon", "$ROOT/daemon/target/debug/gnode-daemon"];
usort($candidates, fn($a, $b) => (is_file($b) ? filemtime($b) : 0) <=> (is_file($a) ? filemtime($a) : 0));

$bin = null; $inv = null;
foreach ($candidates as $cand) {
    if (!is_executable($cand)) continue;
    $out = []; $rc = 0;
    exec(escapeshellarg($cand) . ' dump-schema 2>/dev/null', $out, $rc);
    if ($rc !== 0) continue;
    $j = json_decode(implode("\n", $out), true);
    if (is_array($j) && isset($j['commands'], $j['tokens'])) { $bin = $cand; $inv = $j; break; }
}
if ($bin === null) {
    fail_hard("no gnode-daemon binary supports `dump-schema`. Rebuild (cargo build --bin gnode-daemon) "
        . "or pass --bin=<path> to a current binary.");
}
$codeCommandCount = (int) $inv['command_count'];
$codeTokens = array_values(array_unique($inv['tokens']));      // canonical + all aliases
sort($codeTokens);

// ---- code truth: Lua functions --------------------------------------------
$luaFiles = glob("$LUA_DIR/gnode_*.lua") ?: [];
sort($luaFiles);
$codeLibs = [];   // basename(no ext) => [function names]
$codeFuncTotal = 0;
foreach ($luaFiles as $lf) {
    $lib = basename($lf, '.lua');
    $src = (string) file_get_contents($lf);
    preg_match_all('/function_name\s*=\s*[\'"]([^\'"]+)[\'"]/', $src, $mm);
    $fns = array_values(array_unique($mm[1]));
    sort($fns);
    $codeLibs[$lib] = $fns;
    $codeFuncTotal += count($fns);
}
$codeLibCount = count($codeLibs);

// ---- doc truth: parse COMMAND_SCHEMA.md -----------------------------------
$docText = (string) file_get_contents($DOC);
$lines = preg_split('/\r?\n/', $docText);

// header totals: "**Base daemon**: 60 commands, 23 Lua libraries (203 functions)"
$hdrCommands = $hdrLibs = $hdrFuncs = null;
if (preg_match('/^\*\*Base daemon\*\*:\s*(\d+)\s+commands,\s*(\d+)\s+Lua libraries\s*\((\d+)\s+functions\)/mi', $docText, $m)) {
    [$hdrCommands, $hdrLibs, $hdrFuncs] = [(int)$m[1], (int)$m[2], (int)$m[3]];
} else {
    $note('header', 'could not find the "**Base daemon**: N commands, M Lua libraries (K functions)" line');
}

// Split the doc into Part 1 / Part 2 / Extension Libraries regions. Base surface
// only: Part 1 ends at the "### CMS Extension Commands" subsection and Part 2 at
// "## Extension Libraries" — extension commands/libs are documented for the
// with-CMS build and are verified by running the checker against a CMS binary.
$part1 = region_between($docText, '## Part 1', '### CMS Extension Commands');
if ($part1 === '') $part1 = region_between($docText, '## Part 1', '## Part 2');
$part2 = region_between($docText, '## Part 2', '## Extension Libraries'); // base libs only
if ($part2 === '') $part2 = region_between($docText, '## Part 2', "\n## "); // fallback

// Part 1: every backtick token in the Command + Aliases columns of each table row.
// Table rows look like: | `name` | `A`, `B` | ... | ... | ... |
$docTokens = [];
$docCommandRows = 0;
foreach (preg_split('/\r?\n/', $part1) as $ln) {
    if (!preg_match('/^\|\s*`/', $ln)) continue;                 // a table body row (col1 backtick)
    // Split on unescaped pipes only — Parameters cells contain `\|` unions
    // (e.g. "basic"\|"full") that must not be read as column separators.
    $cells = array_map(
        fn($c) => trim(str_replace('\\|', '|', $c)),
        preg_split('/(?<!\\\\)\|/', trim($ln, "| \t"))
    );
    if (count($cells) < 2) continue;
    if (stripos($cells[0], 'Command') !== false) continue;       // header row
    $docCommandRows++;
    // col 0 = canonical, col 1 = aliases; harvest backtick tokens from both.
    foreach ([$cells[0], $cells[1]] as $ci => $cell) {
        if (preg_match_all('/`([^`]+)`/', $cell, $tm)) {
            foreach ($tm[1] as $tok) {
                $tok = trim($tok);
                if ($tok === '' || $tok === '—') continue;
                $docTokens[$tok] = true;
            }
        }
    }
}
$docTokens = array_keys($docTokens);
sort($docTokens);

// Part 2: sections "### gnode_x — N functions[ (extra)]" then table rows of `FN`.
$docLibs = [];       // lib => ['declared'=>N, 'fns'=>[...]]
$curLib = null;
foreach (preg_split('/\r?\n/', $part2) as $ln) {
    if (preg_match('/^###\s+(gnode_[a-z0-9_]+)\s+—\s+(\d+)\s+function/i', $ln, $m)) {
        $curLib = $m[1];
        $docLibs[$curLib] = ['declared' => (int)$m[2], 'fns' => []];
        continue;
    }
    if ($curLib !== null && preg_match('/^\|\s*`([A-Z][A-Z0-9_]+)`/', $ln, $m)) {
        $docLibs[$curLib]['fns'][] = $m[1];
    }
}

// ---- compare: Part 1 -------------------------------------------------------
$undoc = array_values(array_diff($codeTokens, $docTokens));   // in code, not in doc
$phantom = array_values(array_diff($docTokens, $codeTokens)); // in doc, not in code
if ($undoc)   $note('Part 1', 'command tokens registered in code but MISSING from the doc: ' . implode(', ', $undoc));
if ($phantom) $note('Part 1', 'command tokens documented but NOT in code (phantom): ' . implode(', ', $phantom));
if ($docCommandRows !== $codeCommandCount) {
    $note('Part 1', "command-row count mismatch: doc has $docCommandRows rows, code registers $codeCommandCount commands");
}

// ---- compare: Part 2 -------------------------------------------------------
$docLibNames = array_keys($docLibs);
$missLibDoc = array_values(array_diff(array_keys($codeLibs), $docLibNames));
$missLibCode = array_values(array_diff($docLibNames, array_keys($codeLibs)));
if ($missLibDoc)  $note('Part 2', 'Lua libraries present as files but with no doc section: ' . implode(', ', $missLibDoc));
if ($missLibCode) $note('Part 2', 'Lua library sections in doc with no matching file (base): ' . implode(', ', $missLibCode));

foreach ($codeLibs as $lib => $fns) {
    if (!isset($docLibs[$lib])) continue;
    $docFns = $docLibs[$lib]['fns'];
    $miss = array_values(array_diff($fns, $docFns));
    $extra = array_values(array_diff($docFns, $fns));
    if ($miss)  $note('Part 2', "$lib: functions in the .lua file but not documented: " . implode(', ', $miss));
    if ($extra) $note('Part 2', "$lib: functions documented but not in the .lua file: " . implode(', ', $extra));
    if ($docLibs[$lib]['declared'] !== count($fns)) {
        $note('Part 2', "$lib: heading says {$docLibs[$lib]['declared']} functions, file has " . count($fns));
    }
}

// ---- compare: header totals ------------------------------------------------
if ($hdrCommands !== null && $hdrCommands !== $codeCommandCount) {
    $note('header', "commands: header says $hdrCommands, code registers $codeCommandCount");
}
if ($hdrLibs !== null && $hdrLibs !== $codeLibCount) {
    $note('header', "Lua libraries: header says $hdrLibs, file glob has $codeLibCount");
}
if ($hdrFuncs !== null && $hdrFuncs !== $codeFuncTotal) {
    $note('header', "functions: header says $hdrFuncs, file glob has $codeFuncTotal");
}

// ---- report ----------------------------------------------------------------
fwrite(STDOUT, "gNode COMMAND_SCHEMA.md drift-check\n");
fwrite(STDOUT, "  binary : $bin\n");
fwrite(STDOUT, sprintf("  code   : %d commands (%d tokens), %d Lua libraries, %d functions\n",
    $codeCommandCount, count($codeTokens), $codeLibCount, $codeFuncTotal));
fwrite(STDOUT, sprintf("  doc    : %d command rows, %d Lua sections; header %s/%s/%s\n",
    $docCommandRows, count($docLibs),
    $hdrCommands ?? '?', $hdrLibs ?? '?', $hdrFuncs ?? '?'));

if (!$problems) {
    fwrite(STDOUT, "\n\xE2\x9C\x93 in sync — COMMAND_SCHEMA.md matches the compiled command surface.\n");
    exit(0);
}
fwrite(STDOUT, "\n\xE2\x9C\x97 " . count($problems) . " drift issue(s):\n");
foreach ($problems as $p) fwrite(STDOUT, "  - $p\n");
fwrite(STDOUT, "\nUpdate COMMAND_SCHEMA.md to match the code (code wins), then re-run.\n");
exit(1);

// ---- helpers ---------------------------------------------------------------
function region_between(string $text, string $start, string $end): string {
    $s = strpos($text, $start);
    if ($s === false) return '';
    $e = strpos($text, $end, $s + strlen($start));
    return $e === false ? substr($text, $s) : substr($text, $s, $e - $s);
}
function fail_hard(string $msg): void {
    fwrite(STDERR, "check-command-schema: $msg\n");
    exit(2);
}
