# AWS Durable Execution OpenTelemetry Conformance

Optional OpenTelemetry integration suites for
`aws-durable-execution-conformance-tests`. Installing this distribution adds
the `otel-invocation`, `otel-execution`, and `otel-long-running` suites to the
existing runner through a Python entry point; it does not install a second
conformance CLI.

The SDK handlers and SAM templates that exercise these suites live in each SDK
repository, not here. This package provides the language-neutral requirements,
the shared reusable workflow, the OpenTelemetry Collector build, and the
telemetry validators. Every SDK repository owns its own handlers and passes
their location to the shared workflow through the required `examples_dir`
input.

## Install

```bash
pip install aws-durable-execution-conformance-tests-otel
```

The package requires a compatible `>=1.0,<2.0` core runner and owns all OTel
protocol dependencies, telemetry parsing, exporter profiles, backend adapters,
validators, and requirement resources. Core `0.2.0` introduced the extension
API used to discover these suites; this release of the package requires core
`1.x` and cannot be installed against core `0.2.x` or earlier.

The suites pair the same execution scenarios with view-specific telemetry
contracts. Invocation-view requirements assert spans emitted around each
Lambda invocation. Execution-view requirements assert the terminal `Workflow`
hierarchy and invocation links emitted across the durable execution.
Both views require the backend-propagated trace to contain `Workflow`,
`Invocation`, and operation spans for one durable execution. `Workflow` is
parented either to the remote context propagated by Lambda or to a valid
same-trace ambient span created from that context. `Invocation` uses a valid
same-trace ambient span or falls back to the propagated remote parent. The
propagated parent is not guaranteed to be the durable backend server span
itself, and OpenTelemetry does not expose an ancestor chain that an SDK could
traverse. A deterministic synthetic execution root is used only when no valid
remote parent can be constructed.
The long-running suite applies both invocation and execution views to waits,
retry delays, callbacks, and chained invokes that can remain suspended for up
to one day. Java, JavaScript, and Python all run both views. Each SDK
repository schedules its own runs: the orchestrator launches a suite when no
run is active and validates active executions on later runs, and callers can
also dispatch it manually to launch or check a run. This repository no longer
hosts per-language preset workflows.

## Run

```bash
durable-execution-conformance \
  --template path/to/template.yaml \
  --language python \
  --suite otel-invocation otel-execution \
  --otel-exporter community \
  --otel-backend collector \
  --otel-endpoint https://otel-collector.example/v1/traces \
  --otel-backend-endpoint s3://example-telemetry/durable-execution
```

The SDK test template must accept the non-secret parameters `OtelLayerArn`,
`OtelExecWrapper`, `OtelServiceName`, `OtelTracesExporter`, and, for OTLP,
`OtelExporterEndpoint`, `OtelSecretEnvironmentNames`, and a `NoEcho`
`OtelExporterHeaders` parameter mapped to `OTEL_EXPORTER_OTLP_HEADERS`.
Templates that support the Lambda-hosted S3 collector also accept optional
`OtelCollectorLayerArn`, `OtelCollectorBucket`, and `OtelCollectorPrefix`
parameters.
`--otel-service-name` configures the OpenTelemetry resource identity used by
the deployed function and the backend lookup identity. The long-running
`launch` and `run` commands persist this value for their deferred validation.
Credentials and OTLP headers remain in environment variables or the CI secret
store; the runner redacts the secret parameter from commands and SAM output.

`TelemetryAssertions.span_assertions` can select one or an exact number of
canonical spans and assert any properties, nested attributes, parent
relationships, and timestamp ordering. Every span must start at or before it
ends. By default, an asserted parent must contain its child's complete
timespan; `$allow_outside: true` permits an execution-scoped span to extend
beyond an invocation-scoped parent.
`before`, `after`, and `inside` compare a selected span with one other span.
`inside` can target a span that is not the selected span's parent.
`$linked: true` restricts the relation to spans linked by the selected span.
Complete-contract cases can require every plugin span and every attribute
under a stable prefix to be asserted. See the
[contribution guide](CONTRIBUTING.md#add-a-requirement) for the requirement
syntax and supported span fields.

## Support Matrix

| Exporter | Backend | Credentials |
|---|---|---|
| ADOT | X-Ray | AWS credential chain |
| Community layer | Datadog | `DATADOG_ACCESS_TOKEN`, `DATADOG_API_KEY` |
| Community layer | Dash0 | `DASH0_AUTH_TOKEN` |
| Community layer | AWS S3 collector | AWS credential chain |

Java, JavaScript/Node.js, and Python wrapper settings are included. Provide the
ADOT layer ARN with `--otel-layer-arn` or the runtime-specific
`ADOT_<RUNTIME>_LAYER_ARN` environment variable. The hosted integration
workflow discovers the latest Python layer from the ADOT release.

## Reusable Workflow

The hosted tests use one language-neutral reusable workflow for suites and
long-running views:

- `.github/workflows/opentelemetry-orchestrator.yml`

SDK repositories invoke it with repository and runtime metadata, the
`examples_dir` path to their handlers, plus three optional shell hooks. The
orchestrator resolves test revisions once before launching any workers:

- `setup_command` installs or selects any SDK toolchain.
- `contract_test_command` validates the example and template contract.
- `prepare_command` builds the handlers against the resolved SDK commit.

The hooks run in one Bash process after the optional SDK checkout, so setup
exports remain available to the contract and preparation commands. They receive
`SDK_CHECKOUT`, `SDK_REF`, and the shared workflow environment, including
`EXAMPLES_DIR`. A caller can therefore keep setup logic with its SDK:

```yaml
setup_command: bash "$SDK_CHECKOUT/.github/scripts/setup-conformance-toolchain.sh"
```

The shared workflows do not enumerate SDK toolchains. A future Go, Rust, or
other runtime can supply its own setup and build commands without changing the
orchestrators, workers, or setup action. For example, a Rust caller can select
its toolchain with `rustup`:

```yaml
jobs:
  conformance:
    uses: aws/aws-durable-execution-conformance-tests/.github/workflows/opentelemetry-orchestrator.yml@main
    with:
      language: rust
      resource_prefix: rs
      sdk_repository: example/aws-durable-execution-sdk-rust
      sdk_ref: ${{ github.sha }}
      examples_dir: .build/durable-sdk/conformance-tests-otel
      setup_command: |
        rustup toolchain install stable --profile minimal
        rustup default stable
      prepare_command: >-
        cargo build --release --manifest-path "$EXAMPLES_DIR/Cargo.toml"
      adot_layer_arn: arn:aws:lambda:us-west-2:123456789012:layer:example-rust-adot:1
      collector_compatible_runtime: provided.al2023
      collector_otlp_endpoint: http://localhost:4318
    secrets: inherit
```

The runtime still needs conformance templates, exporter support, and test
handlers before its jobs can pass; adding that support does not require another
copy of the workflow.

## Dash0

The `dash0` backend queries Dash0's OTLP/JSON spans API with `POST /api/spans`.
Pass the regional API base URL through `--otel-backend-endpoint` or
`DASH0_API_URL`, and set `DASH0_AUTH_TOKEN` to a read-capable token. Set
`DASH0_DATASET` when traces are stored outside the organization's default
dataset. The backend first locates a correlated span by service name and
durable execution ARN, then retrieves every span in that trace with adaptive
sampling disabled.

The shared suite workflow runs Dash0 beside X-Ray for every calling language
using the `us-west-2` Dash0 API and ingress endpoints. It uses
`DASH0_AUTH_TOKEN` for both queries and the standard OTLP authorization header.
When the token is not configured, the workflows skip Dash0 while continuing to
run the other telemetry backends.

## Datadog

The `datadog` backend queries `POST /api/v2/spans/events/search`. Set
`DATADOG_ACCESS_TOKEN` to a read-capable OAuth access token. Pass a non-default
API base URL through `--otel-backend-endpoint`, or select another Datadog site
with `DD_SITE`. The backend locates a correlated span by service name and
durable execution ARN, follows cursor pagination, and accumulates newly indexed
spans across polling attempts. Conformance spans carry the execution ARN used
by this query, so the backend does not need a second full-trace search.
Datadog search results expose millisecond timestamps, so temporal assertions
allow at most 1 ms of backend-specific rounding at span boundaries.

The shared Java, Python, and JavaScript suite workflow uses the generic
`https://otlp.datadoghq.com` OTLP base endpoint and runs Datadog beside X-Ray
and Dash0. The JavaScript examples reuse the Lambda layer's global tracer
provider so endpoint and authentication settings are applied once instead of
creating a second unauthenticated exporter. The workflow reads the API access
token from `DATADOG_ACCESS_TOKEN` and the intake API key from
`DATADOG_API_KEY`, formatting it as the `dd-api-key` OTLP header.
When either credential is not configured, the workflows skip Datadog while
continuing to run the other telemetry backends.

Configure the Datadog account once with a 100% APM retention filter for
`service:durable-execution-conformance` before running a suite. The shared
workflows do not modify retention settings. The
`scripts/configure-datadog-retention.py` helper can create or update the filter
when run manually with `DATADOG_API_KEY` and `DATADOG_APPLICATION_KEY`.
Complete retention is required because the suites validate every plugin span
and cannot pass against a sampled or partially indexed trace.

## AWS S3 Collector

The `collector` backend reads trace files written by the OpenTelemetry
Collector Contrib
[`awss3exporter`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/awss3exporter).
This repository does not implement an exporter. The shared collector assets
live outside the SDK examples so those examples can consume the same builder
after they move to their SDK repositories. Start `otelcol-contrib` with the
included [configuration](collector/config.yaml):

```bash
AWS_REGION=us-west-2 \
OTEL_S3_BUCKET=example-telemetry \
OTEL_S3_PREFIX=durable-execution \
otelcol-contrib --config collector/config.yaml
```

The sample receives OTLP over HTTP or gRPC and uses the exporter's
`otlp_json` marshaler with gzip compression. The backend also supports
`otlp_proto` objects and the exporter's uncompressed, gzip, and zstd modes.
Pass the collector's reachable OTLP endpoint through `--otel-endpoint` and its
S3 destination through `--otel-backend-endpoint s3://bucket/prefix` (or
`OTEL_COLLECTOR_S3_URI`). The runner's AWS identity needs `s3:ListBucket` on
the bucket and `s3:GetObject` under the prefix; the collector identity needs
write access.

The stock OpenTelemetry Lambda collector layer does not include
`awss3exporter`. The shared
[`build-lambda-layer.sh`](collector/build-lambda-layer.sh) adds that
upstream component to a pinned `opentelemetry-lambda` checkout and builds a
custom extension layer containing `config-s3.yaml`. Each calling language's
conformance run publishes temporary language-compatible layer
versions, sends each function's OTLP traffic to the local extension, queries the
resulting S3 objects through the `collector` backend, and removes every
temporary stack, bucket, and layer version afterward.

## SDK Handlers

This package bundles no example handlers. Each SDK repository owns the handlers
and SAM templates that satisfy these requirements, and points the shared
orchestrator at them with the required `examples_dir` input:

| SDK | Handlers |
| --- | --- |
| Java | `conformance-tests-otel/` in `aws/aws-durable-execution-sdk-java` |
| JavaScript | `aws/aws-durable-execution-sdk-js` |
| Python | `aws/aws-durable-execution-sdk-python` |

A conforming project supplies one function per requirement, maps each to its
requirement ID through `TestingMetadata.TestDescription`, declares the
`OtelServiceName` parameter, and provides `template.yaml` plus
`template-long-running.yaml`.

### Adding a requirement

Requirements and handlers live in different repositories, so a new case lands in
two steps, requirement first:

1. In this repository, add `test-requirements/<suite>/<suite>-<n>.yaml` with the
   expected spans and attributes, and bump `case_count` for that suite in
   `opentelemetry-orchestrator.yml` so failure diagnostics dump the new
   function's log group.
2. In each SDK repository, add the handler and register it in `template.yaml`
   (and `template-long-running.yaml` for long-running cases) with a
   `TestingMetadata.TestDescription` naming the requirement ID.

Ordering is forgiving. A published requirement with no handler reports
`UNCOVERED`, and a handler whose requirement ID is not yet published is filtered
out before invocation, because the runner keeps only the template functions whose
description IDs appear in the selected suite. So step 1 can merge while SDKs catch
up, and neither half breaks the other.

If the requirement turns out to be wrong once an SDK writes the handler against
it, correct the requirement here first, then land the handler; the
`Requirement correctness dispute` issue template captures the evidence. The
`OpenTelemetry Conformance Tests` workflow runs the Python SDK's handlers against the
requirements on every PR here, so a change that breaks an existing case fails on
that PR rather than in the next SDK to bump.

`UNCOVERED` is non-blocking under the default `--fail-on failed` and blocking
under `failed+uncovered`. To adopt a requirement without shipping a handler,
declare it under a function's `TestingMetadata.NotImplemented` with a reason;
that reports `NOT_IMPLEMENTED`, which never blocks.

An SDK caller pins the orchestrator workflow by SHA (`uses: ...@<sha>`) and
selects the requirement revision with `conformance_test_ref`. A caller that pins
`conformance_test_ref` to a SHA sees requirement changes only when it bumps; one
that tracks `main` sees them on its next run.

## Third-Party Plugins

Additional profiles and backends register entry points in:

- `aws_durable_execution_conformance_tests_otel.exporters`
- `aws_durable_execution_conformance_tests_otel.backends`

Names must be unique. A backend factory exposes `name` and
`create(options, region=...)`; an exporter profile exposes `name`,
`supported_backends`, and `configure(options)`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidance on adding OTel requirements,
SDK test handlers, provider-neutral assertions, and test coverage.
