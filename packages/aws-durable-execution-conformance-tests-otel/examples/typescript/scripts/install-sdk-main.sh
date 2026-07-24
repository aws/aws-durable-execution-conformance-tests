#!/usr/bin/env bash

set -euo pipefail

cleanup_dir=""
if [[ -n "${SDK_SOURCE_DIR:-}" ]]; then
  sdk_dir="$SDK_SOURCE_DIR"
else
  cleanup_dir="$(mktemp -d)"
  sdk_dir="$cleanup_dir/aws-durable-execution-sdk-js"
  git clone --depth 1 --branch main \
    https://github.com/aws/aws-durable-execution-sdk-js.git \
    "$sdk_dir"
fi
trap '[[ -z "$cleanup_dir" ]] || rm -rf "$cleanup_dir"' EXIT

pack_dir="$(mktemp -d)"
trap 'rm -rf "$pack_dir"; [[ -z "$cleanup_dir" ]] || rm -rf "$cleanup_dir"' EXIT

npm ci --prefix "$sdk_dir"
npm run build --prefix "$sdk_dir" \
  --workspace packages/aws-durable-execution-sdk-js
npm run build --prefix "$sdk_dir" \
  --workspace packages/aws-durable-execution-sdk-js-otel
npm pack --prefix "$sdk_dir" \
  --workspace packages/aws-durable-execution-sdk-js \
  --pack-destination "$pack_dir" >/dev/null
npm pack --prefix "$sdk_dir" \
  --workspace packages/aws-durable-execution-sdk-js-otel \
  --pack-destination "$pack_dir" >/dev/null

npm install --no-save --package-lock=false "$pack_dir"/*.tgz
