#!/usr/bin/env bash
# Double-click to build release/linux/ShadeResearch (see docs/BUILDING.md).
#
# The first run creates .venv-build/linux with the CPU build of torch and the
# build dependencies; later runs reinstall them only when pyproject.toml or
# constraints.txt changed. Arguments are passed on to scripts/build.py.
set -euo pipefail
self="$(readlink -f "$0")"
cd "$(dirname "$self")"

# A file manager starts this without a terminal: open one so the build is visible.
if [ ! -t 1 ] && [ -z "${SHADE_BUILD_WINDOW:-}" ]; then
    export SHADE_BUILD_WINDOW=1
    for terminal in konsole gnome-terminal ptyxis xterm; do
        command -v "$terminal" >/dev/null || continue
        case "$terminal" in
            gnome-terminal | ptyxis) exec "$terminal" -- "$self" "$@" ;;
            *) exec "$terminal" -e "$self" "$@" ;;
        esac
    done
    echo "No terminal emulator found; run ./build.sh from a terminal." >&2
    exit 1
fi

finish() {
    status=$?
    echo
    if [ "$status" -eq 0 ]; then echo "Done."; else echo "FAILED (exit code $status)."; fi
    if [ -n "${SHADE_BUILD_WINDOW:-}" ]; then read -r -p "Press Enter to close this window... " || true; fi
}
trap finish EXIT

venv=.venv-build/linux
python="$venv/bin/python"
if [ ! -x "$python" ]; then
    for candidate in python3.13 python3.12; do
        command -v "$candidate" >/dev/null || continue
        echo "Creating $venv with $candidate"
        "$candidate" -m venv "$venv"
        break
    done
    if [ ! -x "$python" ]; then
        echo "Python 3.13 or 3.12 is required: neither python3.13 nor python3.12 was found." >&2
        exit 1
    fi
fi

if ! cmp -s pyproject.toml "$venv/pyproject.toml" || ! cmp -s constraints.txt "$venv/constraints.txt"; then
    "$python" -m pip install torch --index-url https://download.pytorch.org/whl/cpu
    "$python" -m pip install -e ".[build]" -c constraints.txt
    # pip leaves this beside the source; the environment keeps its own record.
    rm -rf edinet_workstation.egg-info
    cp pyproject.toml constraints.txt "$venv/"
fi

"$python" -B scripts/build.py "$@"
