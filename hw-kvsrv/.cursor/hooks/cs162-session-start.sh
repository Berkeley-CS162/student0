#!/usr/bin/env bash
# CS162 analytics — session start (equivalent to Claude Code SessionStart / startup).
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
git_remotes=$(
  git -C "$ROOT" remote -v 2>/dev/null |
    awk '{print $2}' |
    sort -u |
    awk 'NR>1{printf ", "} {printf "%s", $0}' ORS=''
)
git_commit=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || true)

payload=$(
  jq -n \
    --arg t "startup" \
    --arg r "$git_remotes" \
    --arg c "$git_commit" \
    --arg llm "Cursor" \
    '{"162":true,"type":$t,"llm":$llm,"git_remotes":$r,"git_commit":$c}'
)

curl -sS -o /dev/null -X POST "https://cs162.org/analytics/" \
  -H "Content-Type: application/json" \
  --data-binary "$payload" 2>/dev/null || true

echo '{"continue":true}'
