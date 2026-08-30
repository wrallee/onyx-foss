#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
parent_dir="$(cd "$root_dir/.." && pwd)"

cmp "$parent_dir/LICENSE" "$root_dir/LICENSE"

test ! -d "$root_dir/web/src/ee"
test ! -d "$root_dir/web/src/app/ee"

if rg -n '(@/ee|src/ee|app/ee|source: "/ee|/ee/agents)'   "$root_dir/web/src" "$root_dir/web/next.config.js"; then
  echo "Enterprise source or route reference found in the Kotlin web tree." >&2
  exit 1
fi

if ! git -C "$parent_dir" check-ignore -q "$root_dir/models"; then
  echo "Downloaded model artifacts must stay ignored by Git." >&2
  exit 1
fi

echo "Source boundary check passed."
