#!/bin/bash
# gNode Setup System - WordPress Detection Library

# Source common library if not already loaded
if [[ -z "${COMMON_LIB_LOADED:-}" ]]; then
    source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
    COMMON_LIB_LOADED=1
fi

#######################################
# WordPress Detection
#######################################

detect_wordpress_sites() {
    local base_dir=${1:-"/var/www"}
    local -a sites=()

    debug "Scanning for WordPress installations in: $base_dir"

    # Find all wp-config.php files
    while IFS= read -r config_file; do
        local site_dir=$(dirname "$config_file")
        local site_name=$(basename "$site_dir")

        # Skip if site_dir is actually a subdirectory (e.g., public_html)
        if [[ "$site_name" == "public_html" || "$site_name" == "htdocs" ]]; then
            site_name=$(basename "$(dirname "$site_dir")")
            # Don't change site_dir, wp-cli needs the actual path
        fi

        # Get WordPress info
        local wp_info
        wp_info=$(detect_wp_info "$site_dir")

        if [[ -n "$wp_info" ]]; then
            echo "$wp_info"
        fi
    done < <(find "$base_dir" -maxdepth 3 -name "wp-config.php" -type f 2>/dev/null)
}

detect_wp_info() {
    local wp_path=$1
    local wp_cli=${WP_CLI:-$(command -v wp 2>/dev/null || echo "wp")}

    if [[ ! -f "$wp_path/wp-config.php" ]]; then
        return 1
    fi

    # Extract site name from path
    local site_name
    if [[ "$wp_path" =~ /var/www/([^/]+) ]]; then
        site_name="${BASH_REMATCH[1]}"
    else
        site_name=$(basename "$wp_path")
    fi

    # Get WordPress version
    local wp_version="unknown"
    if [[ -x "$wp_cli" ]]; then
        wp_version=$("$wp_cli" core version --path="$wp_path" --allow-root 2>/dev/null || echo "unknown")
    elif [[ -f "$wp_path/wp-includes/version.php" ]]; then
        wp_version=$(grep "wp_version = " "$wp_path/wp-includes/version.php" | cut -d"'" -f2 || echo "unknown")
    fi

    # Get active theme
    local active_theme="unknown"
    if [[ -x "$wp_cli" ]]; then
        active_theme=$("$wp_cli" theme list --status=active --field=name --path="$wp_path" --allow-root 2>/dev/null | head -1 || echo "unknown")
    fi

    # Check if multisite
    local is_multisite="false"
    if [[ -x "$wp_cli" ]]; then
        if "$wp_cli" core is-installed --network --path="$wp_path" --allow-root 2>/dev/null; then
            is_multisite="true"
        fi
    fi

    # Get site URL
    local site_url="unknown"
    if [[ -x "$wp_cli" ]]; then
        site_url=$("$wp_cli" option get siteurl --path="$wp_path" --allow-root 2>/dev/null || echo "unknown")
    fi

    # Check gCore installation
    local gcore_installed="false"
    if [[ -d "$wp_path/wp-content/plugins/gCore" ]] || [[ -d "$wp_path/wp-content/mu-plugins/gCore" ]]; then
        gcore_installed="true"
    fi

    # Output as JSON line
    python3 -c "import json; print(json.dumps({
        'name': '$site_name',
        'path': '$wp_path',
        'version': '$wp_version',
        'theme': '$active_theme',
        'multisite': $is_multisite,
        'url': '$site_url',
        'gcore_installed': $gcore_installed
    }))"
}

detect_wp_themes() {
    local wp_path=$1
    local themes_dir="$wp_path/wp-content/themes"

    if [[ ! -d "$themes_dir" ]]; then
        return 1
    fi

    find "$themes_dir" -maxdepth 1 -type d -o -type l | tail -n +2 | while read -r theme_dir; do
        local theme_name=$(basename "$theme_dir")
        local theme_type="directory"

        if [[ -L "$theme_dir" ]]; then
            theme_type="symlink"
            local target=$(readlink "$theme_dir")
            echo "${theme_name}|${theme_type}|${target}"
        else
            echo "${theme_name}|${theme_type}|"
        fi
    done
}

detect_wp_plugins() {
    local wp_path=$1
    local wp_cli=${WP_CLI:-$(command -v wp 2>/dev/null || echo "wp")}

    if [[ ! -x "$wp_cli" ]]; then
        return 1
    fi

    "$wp_cli" plugin list --format=json --path="$wp_path" --allow-root 2>/dev/null
}

#######################################
# System Detection
#######################################

detect_system_info() {
    python3 -c "
import json, os, platform, subprocess

def get_command_output(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL).decode().strip()
    except:
        return 'unknown'

info = {
    'os': platform.system(),
    'os_version': platform.release(),
    'architecture': platform.machine(),
    'hostname': platform.node(),
    'python_version': platform.python_version(),
    'php_version': get_command_output('php -v | head -1 | cut -d\" \" -f2'),
    'user': os.getenv('USER', 'unknown'),
    'home': os.getenv('HOME', 'unknown'),
    'shell': os.getenv('SHELL', 'unknown')
}

print(json.dumps(info, indent=2))
"
}

detect_valkey_status() {
    local valkey_service=${1:-"valkey-gnode.service"}
    local valkey_host=${2:-"127.0.0.1"}
    local valkey_port=${3:-"47445"}

    local status="stopped"
    local enabled="false"
    local version="unknown"

    if systemctl is-active --quiet "$valkey_service"; then
        status="running"
    fi

    if systemctl is-enabled --quiet "$valkey_service" 2>/dev/null; then
        enabled="true"
    fi

    # Try to get version
    if check_port "$valkey_host" "$valkey_port" 2; then
        version=$(timeout 2 bash -c "echo 'INFO server' | nc $valkey_host $valkey_port" 2>/dev/null | grep "redis_version" | cut -d: -f2 | tr -d '\r' || echo "unknown")
    fi

    python3 -c "import json; print(json.dumps({
        'service': '$valkey_service',
        'status': '$status',
        'enabled': $enabled,
        'version': '$version',
        'host': '$valkey_host',
        'port': $valkey_port
    }))"
}

detect_gnode_daemon_status() {
    local daemon_service=${1:-"gnode-daemon.service"}
    local daemon_path=${2:-"${GNODE_DAEMON_BIN:-${GNODE_DIR:-/opt/gNode}/daemon/target/release/gnode-daemon}"}

    local status="stopped"
    local enabled="false"
    local version="unknown"
    local uptime="0"

    if systemctl is-active --quiet "$daemon_service"; then
        status="running"
    fi

    if systemctl is-enabled --quiet "$daemon_service" 2>/dev/null; then
        enabled="true"
    fi

    # Get version if daemon exists
    if [[ -x "$daemon_path" ]]; then
        version=$("$daemon_path" --version 2>/dev/null | head -1 || echo "unknown")
    fi

    # Get uptime if running
    if [[ "$status" == "running" ]]; then
        uptime=$(systemctl show "$daemon_service" -p ActiveEnterTimestamp --value)
    fi

    python3 -c "import json; print(json.dumps({
        'service': '$daemon_service',
        'status': '$status',
        'enabled': $enabled,
        'version': '$version',
        'path': '$daemon_path',
        'uptime': '$uptime'
    }))"
}

detect_gnode_client_status() {
    local client_path=${1:-"${GNODE_CLIENT_DIR:-/opt/gNode-Client}"}
    local composer_name="geodineum/gnode-client"

    local installed="false"
    local version="unknown"
    local composer_installed="false"

    if [[ -d "$client_path" ]]; then
        installed="true"

        # Try to get version from composer.json
        if [[ -f "$client_path/composer.json" ]]; then
            version=$(python3 -c "import json; print(json.load(open('$client_path/composer.json')).get('version', 'unknown'))" 2>/dev/null || echo "unknown")
        fi
    fi

    # Check if installed via Composer in gCore
    if [[ -d "${GCORE_DIR:-/opt/geodineum/gCore}/vendor/$composer_name" ]]; then
        composer_installed="true"
    fi

    python3 -c "import json; print(json.dumps({
        'path': '$client_path',
        'installed': $installed,
        'version': '$version',
        'composer_installed': $composer_installed
    }))"
}

detect_gcore_status() {
    local gcore_path=${1:-"${GCORE_DIR:-/opt/geodineum/gCore}"}

    local installed="false"
    local version="unknown"
    local composer_ready="false"

    if [[ -d "$gcore_path" ]]; then
        installed="true"

        # Check composer installation
        if [[ -d "$gcore_path/vendor" ]] && [[ -f "$gcore_path/vendor/autoload.php" ]]; then
            composer_ready="true"
        fi

        # Try to get version
        if [[ -f "$gcore_path/composer.json" ]]; then
            version=$(python3 -c "import json; print(json.load(open('$gcore_path/composer.json')).get('version', 'dev'))" 2>/dev/null || echo "unknown")
        fi
    fi

    python3 -c "import json; print(json.dumps({
        'path': '$gcore_path',
        'installed': $installed,
        'version': '$version',
        'composer_ready': $composer_ready
    }))"
}

#######################################
# System Report
#######################################

generate_system_report() {
    local output_file=${1:-}

    info "Generating system report..."

    local report=$(python3 -c "
import json
from datetime import datetime

report = {
    'timestamp': datetime.now().isoformat(),
    'system': $(detect_system_info),
    'valkey': $(detect_valkey_status),
    'gnode_daemon': $(detect_gnode_daemon_status),
    'gnode_client': $(detect_gnode_client_status),
    'gcore': $(detect_gcore_status),
    'wordpress_sites': []
}

print(json.dumps(report, indent=2))
")

    # Add WordPress sites
    local wp_sites=()
    while IFS= read -r site_info; do
        wp_sites+=("$site_info")
    done < <(detect_wordpress_sites)

    if [[ ${#wp_sites[@]} -gt 0 ]]; then
        report=$(echo "$report" | python3 -c "
import json, sys
report = json.load(sys.stdin)
sites = []
$(for site in "${wp_sites[@]}"; do echo "sites.append($site)"; done)
report['wordpress_sites'] = sites
print(json.dumps(report, indent=2))
")
    fi

    if [[ -n "$output_file" ]]; then
        echo "$report" > "$output_file"
        success "System report saved to: $output_file"
    else
        echo "$report"
    fi
}

#######################################
# Interactive Site Selection
#######################################

select_wordpress_sites() {
    local -a all_sites=()
    local -a site_names=()
    local -a site_paths=()

    info "Detecting WordPress installations..."

    while IFS= read -r site_json; do
        all_sites+=("$site_json")
        local name=$(echo "$site_json" | python3 -c "import json, sys; print(json.load(sys.stdin)['name'])")
        local path=$(echo "$site_json" | python3 -c "import json, sys; print(json.load(sys.stdin)['path'])")
        local version=$(echo "$site_json" | python3 -c "import json, sys; print(json.load(sys.stdin)['version'])")
        local theme=$(echo "$site_json" | python3 -c "import json, sys; print(json.load(sys.stdin)['theme'])")

        site_names+=("$name (WP $version, Theme: $theme)")
        site_paths+=("$path")
    done < <(detect_wordpress_sites)

    if [[ ${#all_sites[@]} -eq 0 ]]; then
        warn "No WordPress installations found in /var/www"
        return 1
    fi

    success "Found ${#all_sites[@]} WordPress installation(s)"
    echo ""

    # Display sites
    for i in "${!site_names[@]}"; do
        echo "  $((i+1)). ${site_names[$i]}"
        echo "      Path: ${site_paths[$i]}"
    done
    echo ""

    # Ask user to select
    local selections=$(ask_multiselect "Select sites to configure with gNode/gCore:" "${site_names[@]}")

    # Return selected sites as JSON array
    local selected_sites="[]"
    while IFS= read -r selected_name; do
        for i in "${!site_names[@]}"; do
            if [[ "${site_names[$i]}" == "$selected_name" ]]; then
                selected_sites=$(echo "$selected_sites" | python3 -c "
import json, sys
sites = json.load(sys.stdin)
sites.append(${all_sites[$i]})
print(json.dumps(sites))
")
                break
            fi
        done
    done <<< "$selections"

    echo "$selected_sites"
}
