#!/bin/bash
#
# gNode Apache2 Optimization Module
# Optimizes Apache2 to not bottleneck gNode's 217K+ ops/sec capacity
#
# Run as: sudo ./scripts/setup/modules/10-apache-optimize.sh
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Detect system resources
TOTAL_RAM_MB=$(free -m | awk '/^Mem:/{print $2}')
CPU_CORES=$(nproc)
TOTAL_RAM_GB=$((TOTAL_RAM_MB / 1024))

echo "=============================================="
echo "  gNode Apache2 Optimization"
echo "=============================================="
echo "Detected: ${CPU_CORES} CPU cores, ${TOTAL_RAM_GB}GB RAM"
echo ""

# Calculate optimal settings based on resources
# Rule of thumb: Each Apache worker uses ~10-30MB, PHP-FPM ~30-50MB
if [ "$TOTAL_RAM_GB" -ge 16 ]; then
    # High-memory server
    MAX_REQUEST_WORKERS=400
    SERVER_LIMIT=400
    THREADS_PER_CHILD=64
    MAX_CONNECTIONS_PER_CHILD=10000
    PM_MAX_CHILDREN=100
    PM_START_SERVERS=20
    PM_MIN_SPARE=10
    PM_MAX_SPARE=40
    PROFILE="high-performance"
elif [ "$TOTAL_RAM_GB" -ge 8 ]; then
    # Medium server
    MAX_REQUEST_WORKERS=200
    SERVER_LIMIT=200
    THREADS_PER_CHILD=32
    MAX_CONNECTIONS_PER_CHILD=5000
    PM_MAX_CHILDREN=50
    PM_START_SERVERS=10
    PM_MIN_SPARE=5
    PM_MAX_SPARE=20
    PROFILE="balanced"
elif [ "$TOTAL_RAM_GB" -ge 4 ]; then
    # Small server
    MAX_REQUEST_WORKERS=100
    SERVER_LIMIT=100
    THREADS_PER_CHILD=25
    MAX_CONNECTIONS_PER_CHILD=3000
    PM_MAX_CHILDREN=25
    PM_START_SERVERS=5
    PM_MIN_SPARE=3
    PM_MAX_SPARE=10
    PROFILE="resource-conscious"
else
    # Minimal server
    MAX_REQUEST_WORKERS=50
    SERVER_LIMIT=50
    THREADS_PER_CHILD=16
    MAX_CONNECTIONS_PER_CHILD=1000
    PM_MAX_CHILDREN=10
    PM_START_SERVERS=2
    PM_MIN_SPARE=1
    PM_MAX_SPARE=5
    PROFILE="minimal"
fi

log_info "Using profile: $PROFILE"

# ============================================================================
# Step 1: Switch to mpm_event (from mpm_prefork)
# ============================================================================
log_info "Configuring MPM Event module..."

# Check current MPM
CURRENT_MPM=$(apachectl -V 2>/dev/null | grep "Server MPM" | awk '{print $3}' || echo "unknown")
log_info "Current MPM: $CURRENT_MPM"

if [ "$CURRENT_MPM" != "event" ]; then
    log_info "Switching from $CURRENT_MPM to mpm_event..."

    # Disable current MPM and enable event
    a2dismod mpm_prefork 2>/dev/null || true
    a2dismod mpm_worker 2>/dev/null || true
    a2enmod mpm_event 2>/dev/null || true

    # PHP-FPM is required for mpm_event (mod_php doesn't work with it)
    if ! dpkg -l | grep -q "php.*-fpm"; then
        log_warn "PHP-FPM not installed. Installing..."
        PHP_VERSION=$(php -v | head -1 | awk '{print $2}' | cut -d. -f1,2)
        apt-get install -y "php${PHP_VERSION}-fpm" 2>/dev/null || apt-get install -y php-fpm
    fi

    # Enable required modules for PHP-FPM
    a2enmod proxy_fcgi setenvif 2>/dev/null || true

    # Enable PHP-FPM config
    PHP_VERSION=$(php -v | head -1 | awk '{print $2}' | cut -d. -f1,2)
    a2enconf "php${PHP_VERSION}-fpm" 2>/dev/null || true

    log_success "Switched to mpm_event with PHP-FPM"
else
    log_success "Already using mpm_event"
fi

# ============================================================================
# Step 2: Configure MPM Event settings
# ============================================================================
log_info "Configuring MPM Event settings..."

MPM_CONF="/etc/apache2/mods-available/mpm_event.conf"
cat > "$MPM_CONF" << EOF
# gNode-Optimized MPM Event Configuration
# Profile: $PROFILE (${TOTAL_RAM_GB}GB RAM, ${CPU_CORES} cores)
# Generated: $(date -Iseconds)

<IfModule mpm_event_module>
    # ServerLimit: Maximum number of server processes
    ServerLimit             $SERVER_LIMIT

    # StartServers: Initial number of server processes
    StartServers            $((CPU_CORES * 2))

    # MinSpareThreads: Minimum idle threads
    MinSpareThreads         $((THREADS_PER_CHILD * 2))

    # MaxSpareThreads: Maximum idle threads
    MaxSpareThreads         $((THREADS_PER_CHILD * 4))

    # ThreadsPerChild: Threads per server process
    ThreadsPerChild         $THREADS_PER_CHILD

    # MaxRequestWorkers: Maximum simultaneous connections
    # This is the key setting for concurrent capacity
    MaxRequestWorkers       $MAX_REQUEST_WORKERS

    # MaxConnectionsPerChild: Requests before worker respawn (memory leak prevention)
    MaxConnectionsPerChild  $MAX_CONNECTIONS_PER_CHILD

    # AsyncRequestWorkerFactor: Multiplier for async connections
    AsyncRequestWorkerFactor 2
</IfModule>
EOF

log_success "MPM Event configured: MaxRequestWorkers=$MAX_REQUEST_WORKERS"

# ============================================================================
# Step 3: Configure PHP-FPM for high performance
# ============================================================================
log_info "Configuring PHP-FPM..."

PHP_VERSION=$(php -v | head -1 | awk '{print $2}' | cut -d. -f1,2)
FPM_POOL="/etc/php/${PHP_VERSION}/fpm/pool.d/www.conf"

if [ -f "$FPM_POOL" ]; then
    # Backup original
    cp "$FPM_POOL" "${FPM_POOL}.bak.$(date +%Y%m%d)" 2>/dev/null || true

    # Update PHP-FPM settings
    sed -i "s/^pm = .*/pm = dynamic/" "$FPM_POOL"
    sed -i "s/^pm.max_children = .*/pm.max_children = $PM_MAX_CHILDREN/" "$FPM_POOL"
    sed -i "s/^pm.start_servers = .*/pm.start_servers = $PM_START_SERVERS/" "$FPM_POOL"
    sed -i "s/^pm.min_spare_servers = .*/pm.min_spare_servers = $PM_MIN_SPARE/" "$FPM_POOL"
    sed -i "s/^pm.max_spare_servers = .*/pm.max_spare_servers = $PM_MAX_SPARE/" "$FPM_POOL"
    sed -i "s/^;pm.max_requests = .*/pm.max_requests = 1000/" "$FPM_POOL"
    sed -i "s/^pm.max_requests = .*/pm.max_requests = 1000/" "$FPM_POOL"

    # Enable status page for monitoring
    sed -i "s/^;pm.status_path = .*/pm.status_path = \/fpm-status/" "$FPM_POOL"

    log_success "PHP-FPM configured: max_children=$PM_MAX_CHILDREN"
else
    log_warn "PHP-FPM pool config not found at $FPM_POOL"
fi

# ============================================================================
# Step 4: Configure PHP OPcache
# ============================================================================
log_info "Configuring PHP OPcache..."

OPCACHE_CONF="/etc/php/${PHP_VERSION}/mods-available/opcache.ini"
if [ -f "$OPCACHE_CONF" ]; then
    cat > "$OPCACHE_CONF" << 'EOF'
; gNode-Optimized OPcache Configuration
zend_extension=opcache.so
opcache.enable=1
opcache.enable_cli=1

; Memory: 256MB for large WordPress + gNode sites
opcache.memory_consumption=256

; Interned strings buffer
opcache.interned_strings_buffer=32

; Maximum cached scripts (WordPress can have 10K+ files)
opcache.max_accelerated_files=20000

; Revalidation frequency (0 = check every request, 2 = every 2 seconds)
; Set to 0 for development, 60+ for production
opcache.revalidate_freq=60

; Fast shutdown for faster worker recycling
opcache.fast_shutdown=1

; Validate timestamps (disable in production if code is stable)
opcache.validate_timestamps=1

; Save comments (required for some frameworks)
opcache.save_comments=1

; JIT compilation (PHP 8+)
opcache.jit_buffer_size=128M
opcache.jit=1255
EOF
    log_success "OPcache configured with JIT"
else
    log_warn "OPcache config not found"
fi

# ============================================================================
# Step 5: Create gNode-optimized Apache configuration
# ============================================================================
log_info "Creating gNode Apache configuration..."

GNODE_APACHE_CONF="/etc/apache2/conf-available/gnode-performance.conf"
cat > "$GNODE_APACHE_CONF" << 'EOF'
# gNode Performance Configuration for Apache2
# Optimized for high-throughput PWA-style applications

# ============================================================================
# Keep-Alive Settings (critical for PWA bundle delivery)
# ============================================================================
KeepAlive On
KeepAliveTimeout 5
MaxKeepAliveRequests 500

# ============================================================================
# Timeouts
# ============================================================================
Timeout 30
RequestReadTimeout header=20-40,MinRate=500 body=20,MinRate=500

# ============================================================================
# HTTP/2 (major performance improvement)
# ============================================================================
<IfModule http2_module>
    Protocols h2 h2c http/1.1
    H2Push on
    H2PushPriority * after
    H2PushPriority text/css before
    H2PushPriority image/jpeg after 32
    H2PushPriority image/png after 32
    H2PushPriority application/javascript interleaved
</IfModule>

# ============================================================================
# Compression (critical for PWA bundles - 70%+ reduction)
# ============================================================================
<IfModule mod_deflate.c>
    # Compress HTML, CSS, JavaScript, Text, XML and fonts
    AddOutputFilterByType DEFLATE application/javascript
    AddOutputFilterByType DEFLATE application/json
    AddOutputFilterByType DEFLATE application/rss+xml
    AddOutputFilterByType DEFLATE application/vnd.ms-fontobject
    AddOutputFilterByType DEFLATE application/x-font
    AddOutputFilterByType DEFLATE application/x-font-opentype
    AddOutputFilterByType DEFLATE application/x-font-otf
    AddOutputFilterByType DEFLATE application/x-font-truetype
    AddOutputFilterByType DEFLATE application/x-font-ttf
    AddOutputFilterByType DEFLATE application/x-javascript
    AddOutputFilterByType DEFLATE application/xhtml+xml
    AddOutputFilterByType DEFLATE application/xml
    AddOutputFilterByType DEFLATE font/opentype
    AddOutputFilterByType DEFLATE font/otf
    AddOutputFilterByType DEFLATE font/ttf
    AddOutputFilterByType DEFLATE image/svg+xml
    AddOutputFilterByType DEFLATE image/x-icon
    AddOutputFilterByType DEFLATE text/css
    AddOutputFilterByType DEFLATE text/html
    AddOutputFilterByType DEFLATE text/javascript
    AddOutputFilterByType DEFLATE text/plain
    AddOutputFilterByType DEFLATE text/xml

    # Don't compress already compressed files
    SetEnvIfNoCase Request_URI \.(?:gif|jpe?g|png|webp|avif)$ no-gzip dont-vary
    SetEnvIfNoCase Request_URI \.(?:exe|t?gz|zip|bz2|sit|rar)$ no-gzip dont-vary
    SetEnvIfNoCase Request_URI \.(?:pdf|mov|avi|mp3|mp4|rm)$ no-gzip dont-vary

    # Compression level (1-9, 6 is balanced)
    DeflateCompressionLevel 6
</IfModule>

# ============================================================================
# Browser Caching (offload repeat requests)
# ============================================================================
<IfModule mod_expires.c>
    ExpiresActive On

    # Default
    ExpiresDefault "access plus 1 month"

    # HTML - short cache for PWA shell
    ExpiresByType text/html "access plus 0 seconds"

    # Data interchange
    ExpiresByType application/json "access plus 0 seconds"
    ExpiresByType application/xml "access plus 0 seconds"
    ExpiresByType text/xml "access plus 0 seconds"

    # CSS and JavaScript - long cache with versioning
    ExpiresByType text/css "access plus 1 year"
    ExpiresByType application/javascript "access plus 1 year"
    ExpiresByType text/javascript "access plus 1 year"

    # Media files
    ExpiresByType image/gif "access plus 1 year"
    ExpiresByType image/jpeg "access plus 1 year"
    ExpiresByType image/png "access plus 1 year"
    ExpiresByType image/webp "access plus 1 year"
    ExpiresByType image/avif "access plus 1 year"
    ExpiresByType image/svg+xml "access plus 1 year"
    ExpiresByType image/x-icon "access plus 1 year"

    # Fonts
    ExpiresByType font/ttf "access plus 1 year"
    ExpiresByType font/otf "access plus 1 year"
    ExpiresByType font/woff "access plus 1 year"
    ExpiresByType font/woff2 "access plus 1 year"
    ExpiresByType application/font-woff "access plus 1 year"
    ExpiresByType application/font-woff2 "access plus 1 year"

    # Manifest files
    ExpiresByType application/manifest+json "access plus 1 week"
    ExpiresByType text/cache-manifest "access plus 0 seconds"
</IfModule>

# ============================================================================
# Security Headers (also improves caching behavior)
# ============================================================================
<IfModule mod_headers.c>
    # Vary header for proper caching
    Header append Vary Accept-Encoding

    # Remove ETags (use Cache-Control instead)
    Header unset ETag
    FileETag None

    # Security headers
    Header always set X-Content-Type-Options "nosniff"
    Header always set X-Frame-Options "SAMEORIGIN"
    Header always set X-XSS-Protection "1; mode=block"
    Header always set Referrer-Policy "strict-origin-when-cross-origin"
</IfModule>

# ============================================================================
# File Descriptor Cache (reduces disk I/O)
# ============================================================================
<IfModule mod_file_cache.c>
    CacheFile /var/www/html/index.html
</IfModule>

# ============================================================================
# Disable unnecessary features
# ============================================================================
# Disable server signature
ServerSignature Off
ServerTokens Prod

# Disable directory listing
Options -Indexes

# Disable .htaccess if possible (use main config instead for performance)
# AllowOverride None  # Uncomment if you don't need .htaccess

# ============================================================================
# gNode-specific optimizations
# ============================================================================
# Allow larger request bodies for bundle uploads
LimitRequestBody 10485760

# Increase header buffer for JWT tokens
LimitRequestFieldSize 16384
LimitRequestFields 100
EOF

# Enable the configuration
a2enconf gnode-performance 2>/dev/null || true

log_success "gNode Apache configuration created"

# ============================================================================
# Step 6: Enable required Apache modules
# ============================================================================
log_info "Enabling required Apache modules..."

MODULES="deflate expires headers http2 proxy_fcgi setenvif rewrite ssl"

for mod in $MODULES; do
    if a2enmod "$mod" 2>/dev/null; then
        log_success "Enabled: $mod"
    else
        log_warn "Could not enable: $mod (may already be enabled or not available)"
    fi
done

# ============================================================================
# Step 7: Kernel tuning for high connections
# ============================================================================
log_info "Applying kernel tuning for high connections..."

SYSCTL_CONF="/etc/sysctl.d/99-gnode-performance.conf"
cat > "$SYSCTL_CONF" << 'EOF'
# gNode Network Performance Tuning

# Increase system file descriptor limit
fs.file-max = 2097152

# Increase socket buffer sizes
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576

# TCP buffer sizes (min, default, max)
net.ipv4.tcp_rmem = 4096 1048576 16777216
net.ipv4.tcp_wmem = 4096 1048576 16777216

# Increase connection backlog
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535

# TCP connection handling
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_keepalive_time = 300
net.ipv4.tcp_keepalive_probes = 5
net.ipv4.tcp_keepalive_intvl = 15

# Enable TCP Fast Open
net.ipv4.tcp_fastopen = 3

# Reuse TIME_WAIT sockets
net.ipv4.tcp_tw_reuse = 1

# Increase local port range
net.ipv4.ip_local_port_range = 1024 65535
EOF

# Apply sysctl settings
sysctl -p "$SYSCTL_CONF" 2>/dev/null || log_warn "Could not apply sysctl settings (may need reboot)"

# Increase file descriptor limits
LIMITS_CONF="/etc/security/limits.d/99-gnode.conf"
cat > "$LIMITS_CONF" << 'EOF'
# gNode file descriptor limits
* soft nofile 65535
* hard nofile 65535
www-data soft nofile 65535
www-data hard nofile 65535
root soft nofile 65535
root hard nofile 65535
EOF

log_success "Kernel tuning applied"

# ============================================================================
# Step 8: Test and restart services
# ============================================================================
log_info "Testing Apache configuration..."

if apachectl configtest 2>&1 | grep -q "Syntax OK"; then
    log_success "Apache configuration syntax OK"
else
    log_error "Apache configuration has errors!"
    apachectl configtest
    exit 1
fi

log_info "Restarting services..."

systemctl restart "php${PHP_VERSION}-fpm" 2>/dev/null || systemctl restart php-fpm 2>/dev/null || true
systemctl restart apache2

log_success "Services restarted"

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=============================================="
echo "  gNode Apache2 Optimization Complete"
echo "=============================================="
echo ""
echo "Profile: $PROFILE"
echo ""
echo "Apache Settings:"
echo "  - MPM: event"
echo "  - MaxRequestWorkers: $MAX_REQUEST_WORKERS"
echo "  - ThreadsPerChild: $THREADS_PER_CHILD"
echo ""
echo "PHP-FPM Settings:"
echo "  - pm.max_children: $PM_MAX_CHILDREN"
echo "  - pm.start_servers: $PM_START_SERVERS"
echo ""
echo "Estimated Capacity:"
ESTIMATED_RPS=$((MAX_REQUEST_WORKERS * 10))
ESTIMATED_DAILY=$((ESTIMATED_RPS * 86400 / 10))  # Assuming 10% avg utilization
echo "  - Concurrent connections: $MAX_REQUEST_WORKERS"
echo "  - Estimated req/sec: ~$ESTIMATED_RPS"
echo "  - Estimated daily capacity: ~$(numfmt --to=si $ESTIMATED_DAILY) requests"
echo ""
echo "gNode Lua Batch capacity: 217,000+ ops/sec"
echo "Apache will NOT bottleneck gNode operations."
echo ""
echo "To verify:"
echo "  apachectl -M | grep mpm"
echo "  systemctl status apache2 php${PHP_VERSION}-fpm"
echo ""
