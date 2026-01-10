#!/bin/bash
# Entrypoint for AI CLI Sandbox container
# Starts socat port forwarding (if configured) and then executes the command

set -e

# Create aliases for auto-approve mode (sandbox is already isolated)
# ~/.claude is mounted from host, so we use aliases instead of modifying settings
if ! grep -q "AI Sandbox: Auto-approve aliases" /home/user/.bashrc 2>/dev/null; then
    cat >> /home/user/.bashrc << 'EOF'

# AI Sandbox: Auto-approve aliases (container is isolated)
alias claude='claude --dangerously-skip-permissions'
alias copilot='copilot --allow-all-tools'
EOF
fi

# ============================================================================
# SOCAT PORT FORWARDING - pgflock services
# ============================================================================
# Forward ports from container localhost to host for pgflock access
run_socat() {
    local port=$1
    local name=$2
    while true; do
        # backlog=5000: For high-concurrency scenarios
        socat TCP-LISTEN:${port},bind=127.0.0.1,fork,reuseaddr,backlog=5000 TCP:host.docker.internal:${port} 2>&1
        sleep 1
    done
}

# pgflock ports (from .pgflock/config.yaml)
run_socat 5050 "pgflock-postgres" &
run_socat 5151 "pgflock-locker" &

# Give socat a moment to start
sleep 0.3
# ============================================================================

# Execute the command passed to the container
exec "$@"
