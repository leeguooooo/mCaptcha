#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

if ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]; then
    echo "Refusing to deploy a dirty worktree: the build hash would not describe the image." >&2
    exit 1
fi

git_hash=$(git rev-parse HEAD)
exec flyctl deploy --build-arg "GIT_HASH=$git_hash" "$@"
