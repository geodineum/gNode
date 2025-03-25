<?php
namespace GSD\Storage;

/**
 * Storage Interface for GSD
 */
interface StorageInterface {
    public function isConnected(): bool;
    public function set(string $key, $value, int $ttl = 0): bool;
    public function get(string $key);
    public function delete(string $key): bool;
    public function exists(string $key): bool;
    public function keys(string $pattern): array;
    public function xAdd(string $key, string $id, array $fields): string;
    public function xGroupCreate(string $key, string $group, string $id, bool $mkstream = false): bool;
    public function xRead(array $streams, int $count = null, int $block = null): array;
    public function xDel(string $key, array $ids): int;
}
