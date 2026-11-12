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

The package requires a compatible `>=1.1.0,<2.0.0` core runner and owns all OTel
protocol dependencies, telemetry parsing, exporter profiles, backend adapters,
validators, and requirement resources. Core `0.2.0` introduced the extension
API used to discover these suites. Core `1.1.0` adds the callback-delivery phase
gate and history absence checks required by cases 25–26; core `1.0.x` and earlier
cannot run this catalog. Package installation and extension discovery enforce
the same minimum version.

To validate an unreleased checkout, install both local packages together so the
core requirement is satisfied by that checkout:

```bash
python -m pip install ./packages/aws-durable-execution-conformance-tests ./packages/aws-durable-execution-conformance-tests-otel
```

The workspace Hatch environments used by CI likewise declare both local
packages. Built-wheel verification must install the matching core wheel with
the OTel wheel; it does not require publishing the new core version first.

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
The long-running requirements and CLI remain available for manual use, but the
reusable long-running workflow is disabled. It is retained as a compatibility
no-op for existing callers and does not launch, poll, or update executions.

Case 25 separates durable outcome from error details. A callback failure with an
exactly empty service error payload still requires an ERROR callback span in both
views, even when the handler catches the failed child, replays through two durable
barriers, and returns `caught/one/two` successfully. Case 17 retains the unhandled
failure path. Explicit FAILED, TIMED_OUT and STOPPED operation ends are failures even
when details are absent. A real SDK exception may produce an exception event even
when the service error payload is empty; case 25 does not constrain event counts.
The cloud case covers FAILED, while the other terminal mappings require SDK unit
coverage. Pending/retrying, successful operations and unrelated cancellation retain their
existing contracts. See [launch coverage](LAUNCH_ISSUE_COVERAGE.md) for the replay
assertions and backend observation limits.

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

The hosted tests use one language-neutral reusable workflow for the invocation
and execution suites:

- `.github/workflows/opentelemetry-orchestrator.yml`

The orchestrator runs only the invocation and execution suites; it does not
call `opentelemetry-long-running.yml`. That standalone compatibility workflow
retains all existing inputs and outputs, returns `check_status: skipped` and
`rollover_ready: false`, and performs no test, deployment, or artifact operations.

The suite workflow schedules only ADOT/X-Ray and the AWS S3 collector. The
Datadog and Dash0 adapters remain available for manual runs. The former
backend-enable inputs and credential secrets remain accepted as ignored,
deprecated compatibility fields.

SDK repositories invoke it with repository and runtime metadata, the
`examples_dir` path to their handlers, plus three optional shell hooks. The
orchestrator resolves test revisions once before launching any workers:

- `setup_command` installs or selects any SDK toolchain.
- `contract_test_command` validates the example and template contract.
- `prepare_command` builds the handlers against the resolved SDK commit.

The optional `case_count` input defaults to `27`. A caller with a pinned legacy
20-, 24- or 26-case catalog can set `case_count: 20`, `case_count: 24` or `case_count: 26` while keeping its existing
`conformance_test_ref`, and update only the reusable workflow revision to receive
workflow maintenance fixes. Other counts are rejected before workers start.
This input controls diagnostic log enumeration; it does not select or filter
requirements. The pinned test revision determines the catalog, and both suite
workers still fail on failed or uncovered requirements. Long-running requirements
are not executed by the hosted workflows.

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

The shared suite workflow does not schedule Dash0. Use the runner directly
with `DASH0_AUTH_TOKEN` and the appropriate regional API and ingress endpoints
to exercise this backend manually.

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

The shared suite workflow does not schedule Datadog. Use the runner directly
with `DATADOG_ACCESS_TOKEN`, `DATADOG_API_KEY`, and the generic
`https://otlp.datadoghq.com` OTLP base endpoint to exercise this backend
manually.

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

## Launch regression coverage

Cases 21–26 cover completed-operation replay exports, active context in SDK user functions, invocation-retry status, errorless callback failure, and first external callback completion followed by later replays. See [the coverage matrix and callback boundaries](LAUNCH_ISSUE_COVERAGE.md).

Case 27 (`chained-invoke-suspend`) covers the short suspended-child scenario
previously exercised by `otel-long-running-4`: a caller invokes a child that
performs a one-second durable wait and then returns its original input. Both
views require exactly two caller and two child invocations, child trace
correlation, and the appropriate wait parentage and continuation links.
The ordinary short suite runs this case; the long-running workflow remains disabled.

## LMI cohort evidence

The optional core `lmi-lifecycle` runner calls
`validate_lmi_telemetry(evidence, phase="observations" | "final")` for requirements
7 and 8. Both phases run the same telemetry checks, so releasing a fault cannot
turn missing pre-release telemetry into an attribution pass. The envelope must
identify the exact plugin artifact in `artifact.plugin`.

For OTel-enabled evidence, normalize lifecycle timestamps to UTC Unix epoch
milliseconds. Canonical spans retain UTC datetime timestamps; their observed
intervals must lie between the originating request entry and
`observationEndedAtMs`. Late spans may outlive wrapper return, but must retain
their original request and execution attribution. Flush observations use the
same clock and window.

The `telemetry.views.invocation` and `telemetry.views.execution` objects each
contain the following records. Collect both through invocation-local plugin
configuration or immutable handler variants; do not mutate a shared provider to
switch views.

| Field | Evidence |
| --- | --- |
| `collection` | `status: complete`, `scope: cohort`, and `requestIds` containing every launched request, including retries. Incomplete collection is a failure, even for unsampled requests. |
| `carriers` | One record per request containing `requestId`, `environmentId`, `workerId`, `processId`, `source`, the actual X-Ray `header`, and boolean `expectedSampled`. |
| `spans` | Records containing an independently observed origin `requestId` and `span`, serialized with the existing `span_to_dict`. Origin IDs must come from fixture observations, not the SDK attributes being checked. |
| `infrastructureSpans` | Optional canonical backend infrastructure spans needed to establish the runtime-parent path. SDK spans cannot be moved here. |
| `exportOutcomes` | One `requestId` and `status` per request: `delivered`, `unsampled`, `export-error`, or `timeout-loss`. Export failure additionally records the observed `error` and controlled `fixture`. |
| `assertions` | A mapping from execution ARN to the ordinary `TelemetryAssertions` schema, including `span_assertions`. The existing validator runs these after checking attribution across the whole cohort. |

The genuine carrier sources are `invocation-context` for Node.js,
`runtime-caller-context` for Java before worker dispatch, and `process-environment`
for Python. Identity must match the actual request and process. Keep injected
missing, empty, malformed and compatibility carriers in separately labeled
diagnostic fixtures; they cannot satisfy these genuine-runtime cases. `Root`,
`Parent` and explicit sampling are parsed by the existing strict X-Ray parser.
An ambient auto-instrumentation parent requires an observed path to the genuine
runtime parent; a missing path remains incomplete evidence.

`CollectorBackend.collect_cohort()` reuses the S3 backend's object parsing and
normalization but retains all traces under an explicitly owned prefix. Ordinary
execution-filtered queries cannot establish absence of wrongly attributed spans.
This method returns one snapshot, not a quiescence verdict: the SDK adapter owns
the bounded export/collection window and must retain all observations. It must
not label an incomplete listing or interrupted collection as `complete`. Other
backends can adopt the same evidence contract when their resource-scoped queries
can establish equivalent completeness.

Requirement 7 requires sampled delivery for the originally selected victim,
peer and warm-reuse request in both views. Each lifecycle `late_activity` event
also contains `telemetry.invocation` and `telemetry.execution` with the actual
`traceId` and `spanId` observed through the public tracer. The collected span must
retain that event's original request ownership and contain its observation time.
A span emitted earlier or attributed to the warm invocation cannot substitute.
Duplicate exports of the same correlated span remain permitted. An intentional unsampled fixture may demonstrate sampling,
but cannot substitute for their attribution coverage. Requirement 8 permits
`export-error` with fixture `bounded-export-error`, or `timeout-loss` with fixture
`bounded-flush-timeout`, provided there is a matching observed flush outcome and
healthy-peer delivery. Neither state means telemetry was delivered after timeout.

Requirement 8 also records these fields under `telemetry`:

- `providers`: `providerId`, `exporterId`, `environmentId`, `processId`,
  `createdAtMs`, and optional `shutdownAtMs`.
- `providerBindings`: one `requestId` and `providerId` per launched request.
- `flushes`: one `requestId`, `startedAtMs`, `finishedAtMs`, positive `budgetMs`,
  and `outcome` (`completed`, `error`, or `timeout`) per request. Failed outcomes
  also carry the observed `error`.

Providers must precede their invocations, remain alive while owners are active,
and retain their identity across requests in the same process. Include an
identified sequential reuse of the same worker/process; overlap alone cannot
establish provider survival after invocation completion. Flush budgets
come from the configured test profile; they are not a product SLA. These checks
do not promise hard termination of non-cooperative I/O, unconditional export
after timeout, or exactly-once export. SDK fixtures and real-LMI adoption remain
tracked in the core suite's adoption table.

### Python lifecycle regression coverage

The opt-in `coverageProfile: python-lifecycle` maps Python SDK PR #743's
observability fixtures to 48 mandatory cells: twelve scenario selectors, both
OTel views, and deployed native concurrency 1 and 2. It supplements the 28 base
lifecycle cells. Each cell declares one `otelView` and collects exactly that view
under `telemetry.views`. The original generic LMI profile still collects both
views in a cohort. No real-LMI or SDK adoption result is implied by the local
positive controls.

`validate_lifecycle_telemetry` checks all plugin-enabled lifecycle scenarios,
including deadline, timeout retry, pending cleanup, late operation and root
return/failure with work in flight. Call it together with the core lifecycle
validator. The matrix fixes the required scenarios, views, configurations and
`behavior_budgets`; the adapter must not select weaker assertions after observing
results. `regression.budgets.exportMs` is the configured exporter-call bound,
alongside the core `observationGraceMs` and applicable lifecycle budgets.

Use the existing owned-event fields (`requestId`, `ownerRequestId`,
`executionArn`, `atMs`, `sequence`) for these observations:

| Event | Additional observed fields |
| --- | --- |
| `otel_enter` | `providerId`, `exporterId`, `contextBefore: {traceId, spanId}`; both context IDs may be null. |
| `otel_factory` | The actual `pluginInstanceId` and factory `pluginExecutionArn`. Python creates a distinct plugin instance per native request. |
| `otel_export_started` | Actual exporter entry with `traceId` and `spanId`. |
| `otel_export` | Matching exporter completion, with `outcome: success | failure | held`. |
| `otel_exit` | Post-call `contextAfter: {traceId, spanId}` and observed integer `exporterShutdowns`, which must be zero. |
| `export_held` / `export_released` | The same case-owned `gateId`; held export also binds `traceId` and `spanId`, and normal release records `mode: normal`. |

The real boundary order is `otel_enter`, factory creation inside the SDK call,
export completion, actual SDK `wrapper_return`/`wrapper_raise`, then the
`otel_exit` diagnostic. The latter observes restored context after the SDK call;
it is not the SDK completion timestamp. An adapter may record the saved SDK
boundary after emitting a diagnostic, but must retain that original boundary
time. Normalize semantic event order separately from raw emission order and
retain raw emission sequence/timestamps in the referenced evidence. Held export
must prevent actual SDK completion until normal release, independently of when
post-call diagnostics arrive. Emergency release cannot establish this result.

Every `telemetry.views[otelView].spans` record retains its independent export
request, a boolean `sampled` from the actual SpanContext, and the canonical
`span_to_dict` payload. Failed exporter attempts retain their actual canonical
spans and matching failure events; they do not claim backend delivery. Carrier
records use the genuine Python `process-environment` source. The sampled and
unsampled runtime cases additionally retain `callerHeader` from the actual Invoke
request, allowing the runtime Root and sampling decision to be compared with the
caller. Missing, empty, malformed and injected carriers are local negative
controls, never substitutes for genuine runtime coverage.

The validators require one logical Invocation span per sampled request and one
terminal Workflow with its actual success/failure status in terminal fixtures.
Logical identities are counted across replay; identical transport re-exports are
retained without promising exactly-once export. A Workflow can start in the
original request and export from its terminal request, retaining the original
runtime parent. Export failure must be followed by a successful invocation in the
same worker/process with the same provider. Held peers must remain active and
make progress after another request ends.

`regression.warmSequence` identifies six distinct executions and their actual
`fixture` values in the order `[success, failure, replay]` twice. Their invocation
records retain `fixture` and the checkpointed `marker`. Replays must satisfy the
same stored-result, stored-failure, callback/wait and retry proof used by the core
lifecycle validator. Warm process/provider identity is observed rather than
assumed from consecutive requests. These object-allocation and fixture details
belong to this explicit Python profile, not the language-neutral generic suite.

### Language-neutral export lifecycle

The `export-lifecycle` coverage profile exercises both `sync` and
`async-bounded-flush` modes independently of language, with explicit mode-specific
budgets and completion evidence. See the [shared profile guide](../aws-durable-execution-conformance-tests/src/aws_durable_execution_conformance_tests/lmi/export-lifecycle.md).
Java handler-owner/thread, HandlerScoped forwarding and MDC checks remain SDK-owned.
