<?php
namespace GSD\Utils;

/**
 * Utility functions for stream operations
 */
class StreamUtils {
    /**
     * Create stream name with correct format
     */
    public static function getStreamName(string $siteId, string $streamPrefix, string $nodeId, string $type): string {
        return sprintf('{%s}:%s:stream:%s:%s', $siteId, $streamPrefix, $nodeId, $type);
    }
    
    /**
     * Create unique request ID
     */
    public static function generateRequestId(string $prefix = 'req_'): string {
        return uniqid($prefix, true);
    }
    
    /**
     * Parse response from JSON
     */
    public static function parseResponse(string $responseJson): ?array {
        $response = json_decode($responseJson, true);
        
        if (json_last_error() !== JSON_ERROR_NONE) {
            error_log("Error parsing response JSON: " . json_last_error_msg());
            return null;
        }
        
        return $response;
    }
}
