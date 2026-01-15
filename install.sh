#!/bin/sh
set -eu

REPO_URL="${REPO_URL:-https://github.com/eugenepyvovarov/codeagents-server-cc-proxy.git}"
INSTALL_DIR="${INSTALL_DIR:-/opt/claude-proxy}"
DATA_DIR="${DATA_DIR:-/opt/claude-proxy/data}"
LOG_DIR="${LOG_DIR:-/var/log/claude-proxy}"

log_step() {
  step="$1"
  status="$2"
  shift 2 || true
  if [ "$#" -gt 0 ]; then
    printf '[proxy] step=%s status=%s %s\n' "$step" "$status" "$*"
  else
    printf '[proxy] step=%s status=%s\n' "$step" "$status"
  fi
}

fail() {
  step="$1"
  msg="$2"
  log_step "$step" "error" "msg=\"$msg\""
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

require_root() {
  if [ "$(id -u)" != "0" ]; then
    fail "preflight" "must run as root (use sudo)"
  fi
}

python_ok() {
  python3 - <<'PY'
import sys
sys.exit(0 if sys.version_info >= (3, 10) else 1)
PY
}

select_python() {
  for candidate in python3.11 python3.10 python3; do
    if command_exists "$candidate"; then
      if "$candidate" - <<'PY'
import sys
sys.exit(0 if sys.version_info >= (3, 10) else 1)
PY
      then
        printf '%s' "$candidate"
        return 0
      fi
    fi
  done
  return 1
}

detect_default_branch() {
  branch=$(git -C "$INSTALL_DIR" ls-remote --symref origin HEAD 2>/dev/null | awk '/^ref:/{print $2}' | sed 's#refs/heads/##')
  if [ -z "$branch" ]; then
    branch="main"
  fi
  printf '%s' "$branch"
}

ensure_repo() {
  log_step "repo" "ok" "msg=\"syncing\""
  if [ -d "$INSTALL_DIR/.git" ]; then
    git -C "$INSTALL_DIR" remote set-url origin "$REPO_URL"
    git -C "$INSTALL_DIR" fetch --prune origin
  else
    rm -rf "$INSTALL_DIR"
    git clone "$REPO_URL" "$INSTALL_DIR"
  fi

  git -C "$INSTALL_DIR" config --global --add safe.directory "$INSTALL_DIR" >/dev/null 2>&1 || true

  branch=$(detect_default_branch)
  git -C "$INSTALL_DIR" checkout -f "$branch"
  git -C "$INSTALL_DIR" reset --hard "origin/$branch"
}

setup_venv() {
  python_bin=$(select_python || true)
  if [ -z "$python_bin" ]; then
    fail "python" "python3 >= 3.10 not found"
  fi

  "$python_bin" -m venv "$INSTALL_DIR/.venv"
  "$INSTALL_DIR/.venv/bin/python" -m pip install --upgrade pip
  "$INSTALL_DIR/.venv/bin/python" -m pip install -r "$INSTALL_DIR/requirements.txt"
}

install_claude_cli() {
  if ! command_exists npm; then
    fail "node" "npm not found"
  fi
  npm install -g @anthropic-ai/claude-code
}

ensure_healthz() {
  if ! command_exists curl; then
    fail "healthz" "curl not found"
  fi
  if ! curl -fsS http://127.0.0.1:8787/healthz >/dev/null 2>&1; then
    fail "healthz" "proxy did not respond on 127.0.0.1:8787"
  fi
  log_step "healthz" "ok"
}

start_supervisord() {
  if supervisorctl status >/dev/null 2>&1; then
    return 0
  fi

  if command_exists systemctl; then
    systemctl enable --now supervisor >/dev/null 2>&1 || true
    systemctl enable --now supervisord >/dev/null 2>&1 || true
  fi

  if supervisorctl status >/dev/null 2>&1; then
    return 0
  fi

  if command_exists service; then
    service supervisor start >/dev/null 2>&1 || true
    service supervisord start >/dev/null 2>&1 || true
  fi

  if supervisorctl status >/dev/null 2>&1; then
    return 0
  fi

  if command_exists supervisord; then
    if [ -f /etc/supervisor/supervisord.conf ]; then
      supervisord -c /etc/supervisor/supervisord.conf >/dev/null 2>&1 || true
    elif [ -f /etc/supervisord.conf ]; then
      supervisord -c /etc/supervisord.conf >/dev/null 2>&1 || true
    else
      supervisord >/dev/null 2>&1 || true
    fi
  fi

  if ! supervisorctl status >/dev/null 2>&1; then
    fail "supervisord" "supervisord not running"
  fi
}

setup_supervisor_program() {
  conf_dir=""
  if [ -d /etc/supervisor/conf.d ]; then
    conf_dir=/etc/supervisor/conf.d
  elif [ -d /etc/supervisord.d ]; then
    conf_dir=/etc/supervisord.d
  else
    fail "supervisord" "no supervisor conf.d directory found"
  fi

  mkdir -p "$LOG_DIR"
  template="$INSTALL_DIR/deploy/supervisord/claude-proxy.conf"
  target="$conf_dir/claude-proxy.conf"

  sed \
    -e "s#__INSTALL_DIR__#$INSTALL_DIR#g" \
    -e "s#__LOG_DIR__#$LOG_DIR#g" \
    "$template" > "$target"

  start_supervisord
  supervisorctl reread
  supervisorctl update
  supervisorctl restart claude-proxy
}

install_linux_packages() {
  pkg="$1"

  case "$pkg" in
    apt)
      apt-get update -y
      apt-get install -y curl ca-certificates git build-essential python3 python3-venv python3-pip nodejs npm supervisor
      ;;
    dnf)
      dnf install -y curl ca-certificates git gcc make python3 python3-pip nodejs npm supervisor
      ;;
    yum)
      yum install -y curl ca-certificates git gcc make python3 python3-pip nodejs npm supervisor
      ;;
    pacman)
      pacman -Sy --noconfirm curl ca-certificates git base-devel python python-pip nodejs npm supervisor
      ;;
    *)
      fail "packages" "unsupported package manager"
      ;;
  esac
}

ensure_linux_python() {
  if python_ok; then
    return 0
  fi

  case "$1" in
    apt)
      apt-get install -y python3.11 python3.11-venv python3.11-distutils python3.11-dev >/dev/null 2>&1 || true
      ;;
    dnf|yum)
      $1 install -y python3.11 python3.11-devel >/dev/null 2>&1 || true
      ;;
    pacman)
      pacman -Sy --noconfirm python >/dev/null 2>&1 || true
      ;;
  esac

  if ! python_ok; then
    fail "python" "python3 >= 3.10 required; install a newer python"
  fi
}

install_linux() {
  log_step "os_detect" "ok" "detail=linux"

  if command_exists apt-get; then
    pkg="apt"
  elif command_exists dnf; then
    pkg="dnf"
  elif command_exists yum; then
    pkg="yum"
  elif command_exists pacman; then
    pkg="pacman"
  else
    fail "os_detect" "unsupported linux (no supported package manager)"
  fi

  log_step "packages" "ok" "detail=$pkg"
  install_linux_packages "$pkg"
  ensure_linux_python "$pkg"

  install_claude_cli

  mkdir -p "$DATA_DIR"
  ensure_repo
  setup_venv
  setup_supervisor_program
  ensure_healthz
}

ensure_brew() {
  if command_exists brew; then
    echo "$(command -v brew)"
    return 0
  fi

  if [ -x /opt/homebrew/bin/brew ]; then
    echo "/opt/homebrew/bin/brew"
    return 0
  fi

  if [ -x /usr/local/bin/brew ]; then
    echo "/usr/local/bin/brew"
    return 0
  fi

  log_step "brew" "ok" "msg=\"installing\""
  if ! command_exists curl; then
    fail "brew" "curl not found"
  fi

  NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" || \
    fail "brew" "failed to install homebrew"

  if command_exists brew; then
    echo "$(command -v brew)"
    return 0
  fi

  if [ -x /opt/homebrew/bin/brew ]; then
    echo "/opt/homebrew/bin/brew"
    return 0
  fi

  if [ -x /usr/local/bin/brew ]; then
    echo "/usr/local/bin/brew"
    return 0
  fi

  fail "brew" "brew not found after install"
}

run_as_user() {
  user="$1"
  shift
  if [ "$user" = "root" ]; then
    "$@"
  else
    sudo -u "$user" -H "$@"
  fi
}

install_macos() {
  log_step "os_detect" "ok" "detail=darwin"

  brew_bin=$(ensure_brew)
  brew_user="${SUDO_USER:-root}"

  run_as_user "$brew_user" "$brew_bin" install python@3.11 node

  brew_prefix=$(run_as_user "$brew_user" "$brew_bin" --prefix)
  export PATH="$brew_prefix/bin:$brew_prefix/sbin:/usr/bin:/bin:/usr/sbin:/sbin"

  install_claude_cli

  mkdir -p "$DATA_DIR"
  ensure_repo
  setup_venv

  mkdir -p "$LOG_DIR"
  template="$INSTALL_DIR/deploy/launchd/com.codeagents.claude-proxy.plist"
  target="/Library/LaunchDaemons/com.codeagents.claude-proxy.plist"

  sed \
    -e "s#__INSTALL_DIR__#$INSTALL_DIR#g" \
    -e "s#__LOG_DIR__#$LOG_DIR#g" \
    -e "s#__BREW_PREFIX__#$brew_prefix#g" \
    "$template" > "$target"

  /bin/launchctl bootout system "$target" >/dev/null 2>&1 || true
  /bin/launchctl bootstrap system "$target"
  /bin/launchctl kickstart -k system/com.codeagents.claude-proxy

  ensure_healthz
}

main() {
  require_root
  os_name=$(uname -s)
  case "$os_name" in
    Linux)
      install_linux
      ;;
    Darwin)
      install_macos
      ;;
    *)
      fail "os_detect" "unsupported OS: $os_name"
      ;;
  esac
  log_step "complete" "ok"
}

main "$@"
