#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 OPENTELEMETRY_LAMBDA_CHECKOUT OUTPUT_ZIP" >&2
  exit 2
fi

checkout=$1
output_zip=$2
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
collector_dir="$checkout/collector"
components_dir="$collector_dir/lambdacomponents"
awss3_version=v0.151.0

if [[ ! -f "$collector_dir/go.mod" || ! -f "$components_dir/exporter/pkg.go" ]]; then
  echo "Expected an opentelemetry-lambda checkout at $checkout" >&2
  exit 1
fi

cp "$script_dir/lambda/awss3.go" "$components_dir/exporter/awss3.go"
cp "$script_dir/config.yaml" "$collector_dir/config-s3.yaml"
(
  cd "$components_dir"
  go mod edit \
    "-require=github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter@$awss3_version"
  go mod tidy
)

build_tags=lambdacomponents.custom,lambdacomponents.receiver.otlp,lambdacomponents.exporter.awss3
BUILDTAGS="$build_tags" make -C "$collector_dir" package GOARCH=amd64

mkdir -p "$(dirname -- "$output_zip")"
cp "$collector_dir/build/opentelemetry-collector-layer-amd64.zip" "$output_zip"
