// Content Compressor Module for gNode
//
// This module provides gzip compression/decompression for content delivery.
// Achieves 60-80% bandwidth reduction for text content with minimal CPU overhead.
//
// Key Features:
// - gzip compression with best compression ratio
// - Transparent decompression
// - Round-trip validation
// - Base64 encoding for ValKey storage
// - Compression ratio tracking

use flate2::Compression;
use flate2::read::{GzEncoder, GzDecoder};
use std::io::Read;
use thiserror::Error;
use log::{debug, warn};

/// Errors that can occur during compression/decompression
#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("Compression failed: {0}")]
    Compression(String),

    #[error("Decompression failed: {0}")]
    Decompression(String),

    #[error("Base64 encoding failed: {0}")]
    Base64Encode(String),

    #[error("Base64 decoding failed: {0}")]
    Base64Decode(String),

    #[error("Content too large (limit: {limit} bytes, actual: {actual} bytes)")]
    ContentTooLarge { limit: usize, actual: usize },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Statistics about the compression process
#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
    pub duration_ms: u64,
    pub algorithm: String,
}

impl CompressionStats {
    pub fn new(original_size: usize, compressed_size: usize, duration_ms: u64) -> Self {
        let compression_ratio = if original_size > 0 {
            (original_size - compressed_size) as f64 / original_size as f64
        } else {
            0.0
        };

        Self {
            original_size,
            compressed_size,
            compression_ratio,
            duration_ms,
            algorithm: "gzip".to_string(),
        }
    }
}

/// Maximum content size for compression (100MB)
const MAX_CONTENT_SIZE: usize = 100 * 1024 * 1024;

/// Minimum size threshold for compression (worth compressing)
const MIN_COMPRESSION_SIZE: usize = 200; // Don't compress tiny content

/// Compress content using gzip with best compression
///
/// # Arguments
///
/// * `content` - The content bytes to compress
///
/// # Returns
///
/// Returns the compressed bytes or an error
///
/// # Examples
///
/// ```
/// let original = b"Hello World!".repeat(100);
/// let compressed = compress_gzip(&original).unwrap();
/// assert!(compressed.len() < original.len());
/// ```
pub fn compress_gzip(content: &[u8]) -> Result<Vec<u8>, CompressionError> {
    if content.len() > MAX_CONTENT_SIZE {
        return Err(CompressionError::ContentTooLarge {
            limit: MAX_CONTENT_SIZE,
            actual: content.len(),
        });
    }

    let mut encoder = GzEncoder::new(content, Compression::best());
    let mut compressed = Vec::new();

    encoder.read_to_end(&mut compressed)
        .map_err(|e| CompressionError::Compression(e.to_string()))?;

    Ok(compressed)
}

/// Decompress gzip content
///
/// # Arguments
///
/// * `compressed` - The compressed bytes
///
/// # Returns
///
/// Returns the decompressed bytes or an error
pub fn decompress_gzip(compressed: &[u8]) -> Result<Vec<u8>, CompressionError> {
    let mut decoder = GzDecoder::new(compressed);
    let mut decompressed = Vec::new();

    decoder.read_to_end(&mut decompressed)
        .map_err(|e| CompressionError::Decompression(e.to_string()))?;

    Ok(decompressed)
}

/// Compress content to gzip and encode as base64 (for ValKey storage)
///
/// This is the recommended method for storing compressed content in ValKey,
/// as it handles both compression and encoding in one step.
///
/// # Arguments
///
/// * `content` - The content string to compress
///
/// # Returns
///
/// Returns (base64_encoded_compressed, stats) or an error
pub fn compress_and_encode(content: &str) -> Result<(String, CompressionStats), CompressionError> {
    let start = std::time::Instant::now();
    let original_size = content.len();

    // Don't compress tiny content (overhead not worth it)
    if original_size < MIN_COMPRESSION_SIZE {
        let duration_ms = start.elapsed().as_millis() as u64;
        debug!("Content too small for compression ({} bytes), returning original", original_size);
        return Ok((
            content.to_owned(),
            CompressionStats::new(original_size, original_size, duration_ms)
        ));
    }

    // Compress
    let compressed = compress_gzip(content.as_bytes())?;
    let compressed_size = compressed.len();

    // Base64 encode for safe storage
    let encoded = base64::encode(&compressed);

    let duration_ms = start.elapsed().as_millis() as u64;
    let stats = CompressionStats::new(original_size, compressed_size, duration_ms);

    debug!(
        "Compressed {} bytes to {} bytes ({:.1}% reduction) in {}ms",
        original_size, compressed_size, stats.compression_ratio * 100.0, duration_ms
    );

    Ok((encoded, stats))
}

/// Decode from base64 and decompress gzip
///
/// # Arguments
///
/// * `encoded_compressed` - Base64-encoded gzip content
///
/// # Returns
///
/// Returns the decompressed string or an error
pub fn decode_and_decompress(encoded_compressed: &str) -> Result<String, CompressionError> {
    // Try to decode from base64
    let compressed = base64::decode(encoded_compressed)
        .map_err(|e| CompressionError::Base64Decode(e.to_string()))?;

    // Decompress
    let decompressed = decompress_gzip(&compressed)?;

    // Convert to string
    String::from_utf8(decompressed)
        .map_err(|e| CompressionError::Decompression(format!("UTF-8 conversion failed: {}", e)))
}

/// Compress content with automatic encoding detection
///
/// This function determines whether compression is beneficial and automatically
/// encodes the result for ValKey storage.
///
/// # Arguments
///
/// * `content` - The content to compress
/// * `content_type` - MIME type for optimization hints
///
/// # Returns
///
/// Returns (compressed_content, should_decompress, stats)
/// - compressed_content: Either compressed+encoded or original
/// - should_decompress: Whether decompression is needed
/// - stats: Compression statistics
pub fn compress_smart(
    content: &str,
    content_type: &str,
) -> Result<(String, bool, CompressionStats), CompressionError> {
    let start = std::time::Instant::now();
    let original_size = content.len();

    // Don't compress small content
    if original_size < MIN_COMPRESSION_SIZE {
        let duration_ms = start.elapsed().as_millis() as u64;
        let stats = CompressionStats::new(original_size, original_size, duration_ms);
        return Ok((content.to_owned(), false, stats));
    }

    // Don't compress already-compressed formats
    if is_already_compressed(content_type) {
        debug!("Content type {} is already compressed, skipping", content_type);
        let duration_ms = start.elapsed().as_millis() as u64;
        let stats = CompressionStats::new(original_size, original_size, duration_ms);
        return Ok((content.to_owned(), false, stats));
    }

    // Try compression
    let (encoded, stats) = compress_and_encode(content)?;

    // Check if compression was beneficial (at least 10% reduction)
    if stats.compression_ratio < 0.10 {
        warn!(
            "Compression not beneficial for {}: only {:.1}% reduction",
            content_type, stats.compression_ratio * 100.0
        );
        let duration_ms = start.elapsed().as_millis() as u64;
        let stats = CompressionStats::new(original_size, original_size, duration_ms);
        return Ok((content.to_owned(), false, stats));
    }

    Ok((encoded, true, stats))
}

/// Check if content type is already compressed
fn is_already_compressed(content_type: &str) -> bool {
    matches!(
        content_type,
        "image/jpeg" | "image/jpg" | "image/png" | "image/gif" | "image/webp" |
        "video/mp4" | "video/webm" | "audio/mp3" | "audio/ogg" |
        "application/zip" | "application/gzip" | "application/x-gzip" |
        "application/x-bzip2" | "application/x-7z-compressed"
    )
}

/// Validate that content can be successfully round-tripped through compression
pub fn validate_round_trip(content: &str) -> Result<bool, CompressionError> {
    let (encoded, stats) = compress_and_encode(content)?;

    // Skip validation if no compression occurred
    if stats.compression_ratio == 0.0 {
        return Ok(true);
    }

    let decompressed = decode_and_decompress(&encoded)?;

    Ok(content == decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_basic() {
        let original = b"Hello World! ".repeat(100);
        let compressed = compress_gzip(&original).unwrap();
        let decompressed = decompress_gzip(&compressed).unwrap();

        assert!(compressed.len() < original.len());
        assert_eq!(original, decompressed);
    }

    #[test]
    fn test_compress_and_encode() {
        let content = "This is a test content that will be compressed. ".repeat(50);
        let (encoded, stats) = compress_and_encode(&content).unwrap();

        assert!(stats.compressed_size < stats.original_size);
        assert!(stats.compression_ratio > 0.5); // Should get at least 50% reduction
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_decode_and_decompress() {
        let content = "Test content for round-trip validation. ".repeat(30);
        let (encoded, _) = compress_and_encode(&content).unwrap();
        let decoded = decode_and_decompress(&encoded).unwrap();

        assert_eq!(content, decoded);
    }

    #[test]
    fn test_round_trip_validation() {
        let content = "Content for validation testing. ".repeat(25);
        assert!(validate_round_trip(&content).unwrap());
    }

    #[test]
    fn test_small_content_not_compressed() {
        let small_content = "tiny";
        let (result, should_decompress, stats) = compress_smart(small_content, "text/plain").unwrap();

        assert_eq!(result, small_content);
        assert!(!should_decompress);
        assert_eq!(stats.compression_ratio, 0.0);
    }

    #[test]
    fn test_already_compressed_formats() {
        let content = "x".repeat(1000); // Large enough to normally compress
        let (result, should_decompress, stats) = compress_smart(&content, "image/jpeg").unwrap();

        assert_eq!(result, content);
        assert!(!should_decompress);
        assert_eq!(stats.compression_ratio, 0.0);
    }

    #[test]
    fn test_compressible_text() {
        let content = "This is highly compressible text content! ".repeat(100);
        let (encoded, should_decompress, stats) = compress_smart(&content, "text/html").unwrap();

        assert!(should_decompress);
        assert!(stats.compression_ratio > 0.6); // Should get >60% reduction
        assert!(encoded.len() < content.len());

        // Verify decompression works
        let decoded = decode_and_decompress(&encoded).unwrap();
        assert_eq!(content, decoded);
    }

    #[test]
    fn test_compression_stats() {
        let content = "Test ".repeat(500);
        let (_, stats) = compress_and_encode(&content).unwrap();

        assert!(stats.original_size > 0);
        assert!(stats.compressed_size > 0);
        assert!(stats.compressed_size < stats.original_size);
        assert!(stats.compression_ratio > 0.0 && stats.compression_ratio < 1.0);
        assert!(stats.duration_ms < 1000); // Should be very fast
        assert_eq!(stats.algorithm, "gzip");
    }

    #[test]
    fn test_unicode_content() {
        let content = "Hello 世界! Привет мир! مرحبا العالم! ".repeat(50);
        let (encoded, _) = compress_and_encode(&content).unwrap();
        let decoded = decode_and_decompress(&encoded).unwrap();

        assert_eq!(content, decoded);
    }

    #[test]
    fn test_empty_content() {
        let content = "";
        let (result, should_decompress, stats) = compress_smart(content, "text/plain").unwrap();

        assert_eq!(result, content);
        assert!(!should_decompress);
        assert_eq!(stats.original_size, 0);
    }
}
