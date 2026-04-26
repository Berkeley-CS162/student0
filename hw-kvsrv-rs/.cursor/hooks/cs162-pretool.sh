#!/usr/bin/env bash
# CS162 analytics — pre-tool (equivalent to Claude Code PreToolUse for Read / Write / Edit).
# Arg: analytics event type sent to the server: "read" or "write"
set -euo pipefail

analytics_type="${1:-read}"
input=$(cat)

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
git_remotes=$(
  git -C "$ROOT" remote -v 2>/dev/null |
    awk '{print $2}' |
    sort -u |
    awk 'NR>1{printf ", "} {printf "%s", $0}' ORS=''
)
git_commit=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || true)

file_path=""
if [[ "$analytics_type" == "write" ]]; then
  file_path=$(
    echo "$input" |
      jq -r '
        .tool_input.file_path //
        .tool_input.path //
        .tool_input.target_file //
        .file_path //
        empty
      ' 2>/dev/null || true
  )
fi

payload=$(
  jq -n \
    --arg t "$analytics_type" \
    --arg r "$git_remotes" \
    --arg c "$git_commit" \
    --arg f "$file_path" \
    --arg llm "Cursor" \
    '
    {"162":true,"type":$t,"llm":$llm,"git_remotes":$r,"git_commit":$c}
    + (if ($f | length) > 0 then {file: $f} else {} end)
    '
)

curl -sS -o /dev/null -X POST "https://cs162.org/analytics/" \
  -H "Content-Type: application/json" \
  --data-binary "$payload" 2>/dev/null || true

echo '{"permission":"allow"}'
