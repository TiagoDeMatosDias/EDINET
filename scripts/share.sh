#!/usr/bin/env bash
# Start Shade Research and share it on a public https://<random>.trycloudflare.com
# address through a Cloudflare quick tunnel. No Cloudflare account, port
# forwarding, or firewall change is needed; the address changes on every run.
#
#   scripts/share.sh                  # serve on port 8000 and print the link
#   scripts/share.sh --port 8443      # another local port
#   scripts/share.sh --build          # rebuild the frontend first
#   scripts/share.sh --no-server      # share a server that is already running
#
# The server keeps listening on 127.0.0.1 only; cloudflared makes an outbound
# connection to Cloudflare and relays visitors to it. Accounts are always on.
# Ctrl-C stops both.
set -euo pipefail

# When double-clicked in a file manager there is no terminal attached, so all
# output would be invisible. Re-execute ourselves inside a terminal window.
if [[ ! -t 0 && ! -t 1 ]]; then
  for term in konsole gnome-terminal xfce4-terminal xterm; do
    if command -v "$term" >/dev/null 2>&1; then
      case "$term" in
        konsole)        exec "$term" -p title "Shade Share" -e "$0" "$@" ;;
        gnome-terminal) exec "$term" -- "$0" "$@" ;;
        xfce4-terminal) exec "$term" -e "$0" "$@" ;;
        xterm)          exec "$term" -e "$0" "$@" ;;
      esac
    fi
  done
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=8000
BUILD=0
ASSUME_YES=0
START_SERVER=1

usage() { awk 'NR>1 { if ($0 ~ /^#/) { sub(/^# ?/, ""); print } else exit }' "$0"; }
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port) PORT="$2"; shift 2 ;;
    --build) BUILD=1; shift ;;
    --yes|-y) ASSUME_YES=1; shift ;;
    --no-server) START_SERVER=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

say() { printf '\033[1m%s\033[0m\n' "$*"; }
warn() { printf '\033[33m%s\033[0m\n' "$*" >&2; }
die() { printf '\033[31m%s\033[0m\n' "$*" >&2; exit 1; }

PYTHON="$ROOT/.venv3/bin/python"
[[ -x "$PYTHON" ]] || PYTHON="$(command -v python3 || true)"
[[ -n "$PYTHON" ]] || die "Python not found; create .venv3 first (see Readme.md)."
command -v curl >/dev/null || die "curl is required."

if [[ "${EDINET_AUTH_MODE:-accounts}" != "accounts" ]]; then
  die "EDINET_AUTH_MODE=${EDINET_AUTH_MODE} would publish the workspace without sign-in. Unset it to share."
fi
export EDINET_AUTH_MODE=accounts

# -- frontend -----------------------------------------------------------------
# The built bundle is not tracked, so it can be older than the sources. A stale
# bundle hides newer pages and shortcuts (for example the Admin link), so
# rebuild it whenever anything it is built from is newer than the last build.
needs_build() {
  local dist="$ROOT/frontend-v2/dist/index.html"
  [[ -f "$dist" ]] || return 0
  find "$ROOT/frontend-v2/src" "$ROOT/frontend-v2/index.html" \
    "$ROOT/frontend-v2/package.json" "$ROOT/frontend-v2/vite.config.ts" \
    -newer "$dist" -print -quit 2>/dev/null | grep -q .
}
if [[ $START_SERVER -eq 1 ]] && { [[ $BUILD -eq 1 ]] || needs_build; }; then
  say "Building the frontend…"
  (cd "$ROOT/frontend-v2" && { [[ -d node_modules ]] || npm ci; } && npm run build >/dev/null)
fi

# -- cloudflared ----------------------------------------------------------------
CLOUDFLARED="$(command -v cloudflared || true)"
if [[ -z "$CLOUDFLARED" ]]; then
  CLOUDFLARED="$ROOT/tools/bin/cloudflared"
  if [[ ! -x "$CLOUDFLARED" ]]; then
    case "$(uname -m)" in
      x86_64|amd64) ARCH=amd64 ;;
      aarch64|arm64) ARCH=arm64 ;;
      armv7l|armv6l) ARCH=arm ;;
      *) die "No cloudflared download for $(uname -m); install it from https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/" ;;
    esac
    [[ "$(uname -s)" == "Linux" ]] || die "Install cloudflared (e.g. 'brew install cloudflared') and run again."
    say "Downloading cloudflared ($ARCH) to tools/bin…"
    mkdir -p "$ROOT/tools/bin"
    curl -fsSL -o "$CLOUDFLARED.part" "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-$ARCH"
    chmod +x "$CLOUDFLARED.part" && mv "$CLOUDFLARED.part" "$CLOUDFLARED"
  fi
fi
"$CLOUDFLARED" --version >/dev/null 2>&1 || die "cloudflared at $CLOUDFLARED does not run."
if [[ -f "$HOME/.cloudflared/config.yaml" || -f "$HOME/.cloudflared/config.yml" ]]; then
  warn "~/.cloudflared/config.yaml exists; quick tunnels ignore it but may refuse to start. Move it aside if the tunnel fails."
fi

# -- server ---------------------------------------------------------------------
if [[ $START_SERVER -eq 1 ]] && curl -sk --max-time 2 "https://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
  die "Something already answers on port $PORT. Stop it, choose another port with --port, or share it with --no-server."
fi
mkdir -p "$ROOT/logs"
SERVER_LOG="$ROOT/logs/share-server.log"
TUNNEL_LOG="$ROOT/logs/share-tunnel.log"
SERVER_PID=""
TUNNEL_PID=""
cleanup() {
  trap - EXIT INT TERM
  [[ -n "$TUNNEL_PID" ]] && kill "$TUNNEL_PID" 2>/dev/null || true
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  say "Stopped. The link no longer works."
}
trap cleanup EXIT INT TERM

server_alive() { [[ -z "$SERVER_PID" ]] || kill -0 "$SERVER_PID" 2>/dev/null; }
if [[ $START_SERVER -eq 1 ]]; then
  say "Starting the server on https://127.0.0.1:$PORT …"
  (cd "$ROOT" && exec "$PYTHON" main.py --no-reload --port "$PORT") >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 180); do
    curl -sk --max-time 2 "https://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
    server_alive || die "The server stopped; see $SERVER_LOG"
    sleep 1
  done
fi
curl -sk --max-time 2 "https://127.0.0.1:$PORT/health" >/dev/null 2>&1 || die "No server answers on https://127.0.0.1:$PORT; see $SERVER_LOG"
status_mode="$(curl -sk --max-time 5 "https://127.0.0.1:$PORT/api/auth/status" | "$PYTHON" -c "import json,sys; print(json.load(sys.stdin).get('mode'))" 2>/dev/null || true)"
[[ "$status_mode" == "accounts" ]] || die "The server on port $PORT does not require sign-in (mode: ${status_mode:-unknown}); refusing to share it."


status_field() { curl -sk --max-time 5 "https://127.0.0.1:$PORT/api/auth/status" | "$PYTHON" -c "import json,sys; print(json.load(sys.stdin).get('$1'))"; }

# Before anything is public: the first account to register becomes the administrator.
if [[ "$(status_field bootstrap_required)" == "True" ]]; then
  warn "No account exists yet, and the first one to register becomes the administrator."
  if [[ $ASSUME_YES -eq 1 ]]; then
    die "Create your administrator account at https://127.0.0.1:$PORT/register first, then run this again."
  fi
  say "Create it now at  https://127.0.0.1:$PORT/register  (accept the self-signed certificate)."
  say "Waiting for the administrator account…"
  until [[ "$(status_field bootstrap_required)" == "False" ]]; do
    server_alive || die "The server stopped; see $SERVER_LOG"
    sleep 2
  done
fi
if [[ "$(status_field registration_open)" == "True" ]]; then
  warn "Registration is open: anyone with the link can create an account."
  warn "For invitation-only access: Admin (G N) > Access > Registration: Invitation only, then share invitation links."
fi

# -- tunnel ---------------------------------------------------------------------
say "Opening a Cloudflare quick tunnel…"
"$CLOUDFLARED" tunnel --no-autoupdate --url "https://127.0.0.1:$PORT" --no-tls-verify >"$TUNNEL_LOG" 2>&1 &
TUNNEL_PID=$!
URL=""
for _ in $(seq 1 60); do
  URL="$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' "$TUNNEL_LOG" | head -n1 || true)"
  [[ -n "$URL" ]] && break
  kill -0 "$TUNNEL_PID" 2>/dev/null || die "cloudflared stopped; see $TUNNEL_LOG"
  sleep 1
done
[[ -n "$URL" ]] || die "No tunnel address after a minute; see $TUNNEL_LOG"

# The address resolves a few seconds after it is printed.
for _ in $(seq 1 30); do
  curl -s --max-time 5 "$URL/health" >/dev/null 2>&1 && break
  sleep 1
done

COPIED=""
if command -v wl-copy >/dev/null 2>&1 && printf '%s' "$URL" | wl-copy 2>/dev/null; then COPIED=" (copied)"
elif command -v xclip >/dev/null 2>&1 && printf '%s' "$URL" | xclip -selection clipboard 2>/dev/null; then COPIED=" (copied)"
fi

printf '\n'
say "  Share this link:  $URL$COPIED"
printf '\n'
echo "  Visitors sign in with their own accounts. Admin (G N) makes invitation links."
echo "  Logs: $SERVER_LOG, $TUNNEL_LOG"
echo "  Quick tunnels are for trying things out: the address changes on every run,"
echo "  there is no uptime guarantee, and at most 200 requests can be in flight."
echo "  Press Ctrl-C to stop sharing."
printf '\n'

# Stop when either process ends.
while server_alive && kill -0 "$TUNNEL_PID" 2>/dev/null; do sleep 2; done
server_alive || warn "The server stopped; see $SERVER_LOG"
kill -0 "$TUNNEL_PID" 2>/dev/null || warn "The tunnel closed; see $TUNNEL_LOG"
