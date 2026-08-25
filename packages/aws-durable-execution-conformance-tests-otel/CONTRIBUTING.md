# Contributing OpenTelemetry Test Cases

This guide covers changes to the `otel-invocation`, `otel-execution`, and
`otel-long-running` conformance suites. Read the repository
[contribution guide](../../CONTRIBUTING.md) first for the general development,
security, and pull-request requirements.

## What Belongs in the Suite

An OTel requirement should describe stable integration behavior that every
supported SDK can implement and every supported telemetry backend can observe.
Good requirements assert outcomes such as:

- Telemetry is emitted for a durable execution.
- Spans remain correlated across Lambda invocations.
- Retry, success, and failure outcomes are represented.
- A continuation has a parent or span-link relationship.
- Trace identifiers emitted to logs match the active trace.

Do not standardize provider-specific response fields or language-specific
implementation details. Stable SDK-wide span names, durable attributes, and
parent relationships are part of the portable telemetry contract and should be
asserted completely. Backend adapters must normalize provider responses into
the canonical `Trace` model before the requirement is evaluated.

Each requirement must exercise the SDK's public OTel integration. Do not emit
synthetic telemetry in a test handler to make an unsupported behavior pass.

## Add a Requirement

Add invocation-view requirements as
`test-requirements/otel-invocation/otel-invocation-N.yaml` and execution-view
requirements as
`test-requirements/otel-execution/otel-execution-N.yaml`. Add day-scale
requirements as `test-requirements/otel-long-running/otel-long-running-N.yaml`,
with invocation assertions in `TelemetryAssertions` and execution assertions in
`ExecutionTelemetryAssertions`. Requirement IDs are global and must not be
reused.

When both plugins cover a scenario, keep `Input`, `AsyncInvoke`,
`ExpectedExecutionHistory`, and `ExpectedResult` identical between the paired
case numbers. Only the human-readable description and `TelemetryAssertions`
should differ. Reuse the same example workflow for both deployed functions and
select the plugin in deployment configuration.

An OTel requirement contains the normal execution expectations plus a
`TelemetryAssertions` mapping:

```yaml
description: OpenTelemetry preserves correlation across a durable continuation
Input:
  scenario: continuation
AsyncInvoke: true
ExpectedExecutionHistory:
  - EventId: 1
    EventType: ExecutionStarted
ExpectedResult:
  ExecutionStatus: SUCCEEDED
TelemetryAssertions:
  minimum_spans: 2
  minimum_invocations: 2
  require_execution_correlation: true
```

The currently supported telemetry assertions are:

| Key | Meaning |
|---|---|
| `minimum_spans` | Minimum number of normalized spans; defaults to `1`. |
| `minimum_invocations` | Minimum canonical durable invocation span occurrences; identical spans are counted separately. Defaults to `1`. |
| `require_execution_correlation` | Require the durable execution ARN on the trace; defaults to `true`. |
| `require_unique_root_per_trace` | Reject any trace ID containing more than one distinct parentless span. Duplicate exports with the same span ID count once. |
| `require_single_trace_per_execution` | Require every span carrying one durable execution ARN to use the same trace ID. Different chained executions may use different trace IDs. |
| `require_parented_spans` | Require every span matching one partial span selector, or any selector in a sequence, to have a non-null parent span ID. The parent span does not need to be present in the backend response. |
| `require_all_spans` | Require every normalized span to match at least one span assertion. |
| `span_assertion_scope` | Limit complete span coverage to spans matching this partial selector. |
| `exact_attribute_prefixes` | Require assertions to enumerate every attribute under the listed prefixes. |
| `span_assertions` | Select one span and assert arbitrary canonical properties or metadata. |

`span_assertions` accepts one mapping or a list of mappings. Each `select`
mapping matches exactly one span by default. Set `count` to an exact positive
number for repeated spans such as invocation or continuation spans. The
assertions are evaluated in order, and a span selected by one assertion is not
available to later selectors. The corresponding `expect` mapping is applied to
every match and is otherwise a partial assertion, so unlisted properties and
metadata are ignored. Add `expect_by_occurrence` with one mapping per
chronologically ordered match when repeated spans must have distinct shapes.
Each occurrence mapping overrides the common `expect` keys for that match:

```yaml
TelemetryAssertions:
  span_assertions:
    select:
      name: durable step
      attributes:
        durable.operation.type: step
    expect:
      status: OK
      service_name: ${SERVICE_NAME}
      parent:
        name: durable execution
        attributes:
          durable.operation.type: execution
      attributes:
        durable.operation.outcome: success
```

```yaml
TelemetryAssertions:
  span_assertions:
    select:
      name: durable wait
    count: 2
    expect:
      status: OK
    expect_by_occurrence:
      - links:
          - name: Workflow
      - links:
          - name: durable wait
          - name: Workflow
```

Use `require_all_spans: true` when a case defines the complete emitted span
set. Use `span_assertion_scope` to exclude infrastructure spans from the
backend response, for example by selecting spans with
`attributes.durable.execution.arn`. Combine it with
`exact_attribute_prefixes: [durable.]` and list every `durable.*` key under
each `expect.attributes` mapping. Attributes from ADOT, Lambda resource
detection, and telemetry backends remain outside that prefix and do not make
the requirement provider-specific.

The OTel validator binds `${SERVICE_NAME}` to the configured
`--otel-service-name`. Use this placeholder for `expect.service_name` so a
requirement remains independent of the deployed resource name.

Both `select` and `expect` can use any canonical span property: `trace_id`,
`span_id`, `parent_span_id`, `name`, `start_time`, `end_time`, `status`,
`service_name`, `attributes`, or `links`. Nested mappings support arbitrary
attribute metadata without interpreting provider-specific keys. Sequence
assertions compare length, order, and nested values. Each `expect.links` item
resolves the linked span within the trace and applies a partial span assertion,
using the same mechanism as `expect.parent`. When duplicate exports share the
linked trace and span IDs, add `count` to the link item to require an exact
positive number of candidates matching its other properties; the default is
`1`. Add `$occurrence` to require the link to target the 1-based chronological
occurrence among spans matching the item's other properties. The
`$any_of` matcher accepts a non-empty sequence of alternative expected values.
Use it when repeated spans intentionally have one of a small set of shapes. The
optional `expect.parent` mapping resolves the selected span's `parent_span_id`
within the same trace and applies the same partial matching constructs to that
parent span. The selected child must start at or after that parent starts and
end at or before that parent ends.

Every normalized span must have `start_time <= end_time`. An `expect` mapping
can also select exactly one other span with `before`, `after`, or `inside`.
These relationships require the selected span's end to be at or before the
other span's start, its start to be at or after the other span's end, or its
complete timespan to be contained by the other span, respectively. `inside`
is independent of `parent_span_id` and can target a non-parent span. Add
`$linked: true` to a temporal selector to restrict it to spans linked by the
selected span:

```yaml
TelemetryAssertions:
  span_assertions:
    select:
      name: second attempt
    expect:
      after:
        name: first attempt
      inside:
        $linked: true
        name: retry window
```

Capture dynamic values with placeholders in `ExpectedExecutionHistory`, then
reuse those placeholders in telemetry assertions. For example, `Id: ${STEP1}`
binds the operation ID from history so
`durable.operation.id: ${STEP1}` asserts its exact telemetry value. The runner
also provides `${EXECUTION_ARN}` for execution-correlation attributes. Every
other telemetry placeholder must be bound by the requirement's expected
history.

Keep `ExpectedExecutionHistory` and `ExpectedResult` focused on the execution
behavior needed to produce the telemetry. Keep `TelemetryAssertions` portable
across the complete exporter/backend support matrix.

Set `require_unique_root_per_trace: true` for trace-view requirements. A
backend may return a correlated subset without the upstream root, so the
assertion permits zero observed roots for a trace ID. It rejects two or more
distinct parentless spans because that creates a disconnected trace forest and
makes backend-derived entry point, duration, critical path, and sampling
behavior ambiguous.

Set `require_single_trace_per_execution: true` when the complete durable
execution must occupy one trace. The validator groups spans by the supported
execution ARN attributes, so a chained target execution can use another trace
without splitting either execution internally.

Use `require_parented_spans` when an upstream parent may not be included in the
normalized backend response. The assertion requires a non-null
`parent_span_id`; unlike `expect.parent`, it does not require the referenced
span to be available for property or timestamp validation:

```yaml
require_parented_spans:
  - name: Workflow
  - name: Invocation
```

Set `expect.parent.$allow_unresolved: true` when a known parent should be
validated if it is returned, but may be omitted from a provider query. The
selected span must still have a non-null parent ID. Other parent properties are
checked whenever the referenced span is present. Combine it with
`$allow_outside: true` when the child duration can extend beyond that upstream
span.

An `expect.parent` mapping containing only `$allow_unresolved: true` and
`$allow_outside: true` requires a valid parent ID without constraining the
parent's identity or duration. Use this for `Workflow`, whose parent may be the
propagated remote context or a same-trace ambient infrastructure span.

## Durable trace topology

When `_X_AMZN_TRACE_ID` contains valid `Root` and `Parent` fields, `Root`
defines the canonical execution trace. The propagated `Parent` is the immediate
remote context delivered to the runtime; it is not guaranteed to be the
durable backend server span itself.

`Workflow` inherits `Root` and may use either of these parents:

- the remote context reconstructed from `Parent`; or
- the current ambient span when it is valid and has the same trace ID.

Auto-instrumentation can create a local handler span beneath the propagated
remote context before the SDK runs. OpenTelemetry `SpanContext` does not expose
parent or ancestor pointers, so an SDK cannot prove or traverse that complete
chain. Same-trace membership is the portable validation for the ambient
alternative. Reject unrelated ambient context.

`Invocation` uses a valid same-trace ambient span when available and otherwise
uses the reconstructed remote parent.

Parent and sampling resolution are independent:

| Header state | Canonical trace ID | Workflow parent | Sampling |
|---|---|---|---|
| Valid `Root`, `Parent`, `Sampled=1` | Reuse `Root` | Remote `Parent` or same-trace ambient span | Preserve sampled |
| Valid `Root`, `Parent`, `Sampled=0` | Reuse `Root` | Remote `Parent` or same-trace ambient span | Preserve not-sampled |
| Valid `Root`, `Parent`, no valid `Sampled` | Reuse `Root` | Remote `Parent` or same-trace ambient span | Do not invent an authoritative decision; selected-parent and configured sampler behavior applies |
| Valid `Root`, missing or invalid `Parent`, `Sampled=1` or `Sampled=0` | Reuse `Root` | Synthetic execution root | Preserve the explicit decision |
| Valid `Root`, missing or invalid `Parent`, no valid `Sampled` | Reuse `Root` | Synthetic execution root | Configured root sampler decides |
| Missing or invalid `Root` | Derive from execution ARN and stable execution start time | Synthetic execution root | Configured root sampler decides |

Only `Sampled=0` and `Sampled=1` are authoritative upstream decisions. Missing
`Sampled` does not replace a valid remote parent with a synthetic root. An
implementation that selects the remote context represents an absent decision
with an unset sampled bit; `ParentBased` treats that parent as not sampled,
while a directly configured non-parent-based trace-ID-ratio sampler can decide
from the stable canonical trace ID. An implementation that selects an ambient
span uses the sampling state already established for that span.

```text
Propagated remote parent
├── Workflow
├── Same-trace ambient Lambda span
│   └── Invocation
└── Invocation  [when no valid same-trace ambient span exists]

Propagated remote parent
└── Same-trace ambient Lambda span
    ├── Workflow
    └── Invocation
```

When no valid remote parent can be constructed:

```text
Synthetic execution root
├── Workflow
└── Invocation
```

Requirements must allow the Workflow parent to be external or omitted from the
retrieved backend subset. When it is present, it may be a same-trace
infrastructure span without durable attributes. Do not require the
execution-scoped Workflow interval to be temporally contained by an ambient
invocation span: the Workflow start represents the whole durable execution,
while the selected local parent can cover only the terminal Lambda invocation.

The catalog uses separate requirements when two public plugins intentionally
produce different trace views. Invocation-view cases assert per-invocation
operation hierarchy: operations are children of `Invocation`, link to
`Workflow`, and continuation or replay segments also link to the initial
logical operation span. Execution-view operations are children of `Workflow`
and link to the current `Invocation`. Both views require `Workflow`,
`Invocation`, and operations for one durable execution to share the canonical
trace ID. Backends may still assemble correlated traces for different durable
execution ARNs, such as a chained target execution. Cases where no workflow or
operation span completes assert the `Invocation` span alone.

## Add SDK Test Handlers

The matching test handler and deployment template belong in each supported SDK
repository. For Java, JavaScript/Node.js, and Python:

1. Map the function to the new requirement ID with
   `TestingMetadata.TestDescription`.
2. Prefix the handler filename with the case ID and suffix the deployed
   `FunctionName` with it (for example, `otel_5_scenario.py` and
   `${AWS::StackName}-otel-invocation-5`).
3. Implement the `Input.scenario` contract with the SDK's public durable
   execution and OTel APIs.
4. Accept the OTel template parameters documented in the package
   [README](README.md).
5. Exercise the scenario with the S3 collector before using a hosted backend.
6. Declare a missing handler under any function resource's
   `TestingMetadata.NotImplemented`; its `reason` may be empty.

Use the same scenario semantics in every SDK. Runtime setup can differ, but the
observable execution and telemetry behavior must satisfy the same requirement.

## Extend Telemetry Assertions

Most new cases should compose the existing assertions. Add a new assertion only
when it captures a stable, provider-neutral invariant.

When a new assertion is necessary:

1. Implement it in
   [`validators.py`](src/aws_durable_execution_conformance_tests_otel/validators.py)
   against the canonical `Trace` model.
2. Add passing and failing coverage in
   [`test_validators.py`](tests/test_validators.py).
3. Update every affected backend normalizer and its tests if the canonical
   model needs additional data.
4. Return actionable diagnostics without including credentials, headers, or
   provider-specific payloads.
5. Document the new key in the table above.

Do not read raw backend payloads from a requirement validator. Provider-specific
translation belongs in the relevant backend module; shared OTLP translation
belongs in `normalizers.py`.

## Update Package Tests

Add the new requirement ID to the expected set in
[`test_resources.py`](tests/test_resources.py). Add focused tests for any code
path introduced by the case:

- Validator behavior belongs in `test_validators.py`.
- Canonical response conversion belongs in `test_normalizers.py`.
- Retrieval and polling behavior belongs in `test_backends.py` or
  `test_polling.py`.
- Exporter or support-matrix changes belong in `test_exporters.py`.

Use fakes and deterministic payloads in unit tests. Unit tests must not require
AWS or third-party credentials.

## Validate the Change

Run the workspace checks from the repository root:

```bash
hatch run test:otel
hatch run test:all
hatch run types:check
hatch fmt --check packages scripts
hatch run yaml:lint
hatch run dist:all
```

`hatch run dist:all` verifies both archives and installs the built wheels in
isolation to confirm extension discovery and packaged requirement loading.

For an end-to-end run, start OpenTelemetry Collector Contrib with the shared
[`awss3exporter` configuration](collector/config.yaml):

```bash
AWS_REGION=us-west-2 \
OTEL_S3_BUCKET=example-telemetry \
OTEL_S3_PREFIX=durable-execution \
otelcol-contrib --config collector/config.yaml
```

Then run the conformance CLI with
`--suite otel-invocation otel-execution`,
`--otel-exporter community`, `--otel-backend collector`, the collector's
reachable OTLP endpoint, and `--otel-backend-endpoint s3://bucket/prefix`.
The backend supports the exporter's `otlp_json` and `otlp_proto` marshalers,
with no compression, gzip, or zstd. Hosted-backend coverage should be added
separately and must read all credentials from environment variables or CI
secrets.

For Lambda-hosted tests, use the package-level
[`build-lambda-layer.sh`](collector/build-lambda-layer.sh) with the
pinned upstream collector release. The Python, Java, and JavaScript S3
collector workflows publish the custom `awss3exporter` layer, grant
prefix-scoped S3 access, assert the exported spans, and delete all temporary
resources without changing the corresponding X-Ray workflows. Keep this
shared collector implementation outside SDK-specific example directories so
examples hosted in separate SDK repositories can use the same build logic.

## Pull-Request Checklist

- The requirement is language-neutral and provider-neutral.
- Java, JavaScript/Node.js, and Python handlers are implemented or their gaps
  are declared.
- The S3 collector exercises the new behavior.
- Unit tests cover success and actionable failure diagnostics.
- Requirement discovery works from both source and built wheels.
- No secrets or provider credentials appear in fixtures, diagnostics, or
  artifacts.
