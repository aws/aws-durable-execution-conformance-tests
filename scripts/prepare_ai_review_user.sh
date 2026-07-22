#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <claude-review|codex-review>" >&2
  exit 2
fi

review_user="$1"
home_dir="/home/${review_user}"

case "$review_user" in
  claude-review)
    private_dirs=("${home_dir}/.claude" "${home_dir}/tmp")
    ;;
  codex-review)
    private_dirs=("${home_dir}/.codex")
    ;;
  *)
    echo "unsupported AI review user: $review_user" >&2
    exit 2
    ;;
esac

sudo adduser \
  --system \
  --home "$home_dir" \
  --shell /bin/bash \
  --group "$review_user"

sudo install \
  -d \
  -m 700 \
  -o "$review_user" \
  -g "$review_user" \
  "${private_dirs[@]}"

sudo chown -R "runner:${review_user}" "$GITHUB_WORKSPACE"
sudo chmod -R g-w,o-rwx "$GITHUB_WORKSPACE"
sudo chmod -R g+rX "$GITHUB_WORKSPACE"
