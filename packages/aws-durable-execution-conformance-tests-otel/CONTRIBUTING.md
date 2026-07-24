# Contributing OpenTelemetry Test Cases

This guide covers changes to the `otel` conformance suite. Read the repository
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

Add the next unused, sequential `otel-N.yaml` file under
[`test-requirements/otel`](test-requirements/otel). Requirement IDs are global
and must not be reused.

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
| `minimum_invocations` | Minimum distinct Lambda invocation IDs; defaults to `1`. |
| `require_execution_correlation` | Require the durable execution ARN on the trace; defaults to `true`. |
| `require_all_spans` | Require every normalized span to match at least one span assertion. |
| `span_assertion_scope` | Limit complete span coverage to spans matching this partial selector. |
| `exact_attribute_prefixes` | Require assertions to enumerate every attribute under the listed prefixes. |
| `span_assertions` | Select one span and assert arbitrary canonical properties or metadata. |

`span_assertions` accepts one mapping or a list of mappings. Each `select`
mapping matches exactly one span by default. Set `count` to an exact positive
number for repeated spans such as invocation or continuation spans. The
corresponding `expect` mapping is applied to every match and is otherwise a
partial assertion, so unlisted properties and metadata are ignored:

```yaml
TelemetryAssertions:
  span_assertions:
    select:
      name: durable step
      attributes:
        durable.operation.type: step
    expect:
      status: OK
      service_name: conformance
      parent:
        name: durable execution
        attributes:
          durable.operation.type: execution
      attributes:
        durable.operation.outcome: success
```

Use `require_all_spans: true` when a case defines the complete emitted span
set. Use `span_assertion_scope` to exclude infrastructure spans from the
backend response, for example by selecting spans with
`attributes.durable.execution.arn`. Combine it with
`exact_attribute_prefixes: [durable.]` and list every `durable.*` key under
each `expect.attributes` mapping. Attributes from ADOT, Lambda resource
detection, and telemetry backends remain outside that prefix and do not make
the requirement provider-specific.

Both `select` and `expect` can use any canonical span property: `trace_id`,
`span_id`, `parent_span_id`, `name`, `start_time`, `end_time`, `status`,
`service_name`, `attributes`, or `links`. Nested mappings support arbitrary
attribute metadata without interpreting provider-specific keys. Sequence
assertions compare length, order, and nested values. Each `expect.links` item
resolves the linked span within the trace and applies a partial span assertion,
using the same mechanism as `expect.parent`. The
`$any_of` matcher accepts a non-empty sequence of alternative expected values.
Use it when repeated spans intentionally have one of a small set of shapes. The
optional `expect.parent` mapping resolves the selected span's `parent_span_id`
within the same trace and applies the same partial matching constructs to that
parent span.

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

The catalog uses separate requirements when two public plugins intentionally
produce different trace views. Invocation-view cases assert per-invocation
operation hierarchy. Execution-view cases assert a terminal `Workflow` root,
operations parented beneath it, attempts parented beneath their operation, and
links from operation spans to the Lambda invocation that observed them.

## Add SDK Test Handlers

The matching test handler and deployment template belong in each supported SDK
repository. For Java, JavaScript/Node.js, and Python:

1. Map the function to the new requirement ID with
   `TestingMetadata.TestDescription`.
2. Prefix the handler filename with the case ID and suffix the deployed
   `FunctionName` with it (for example, `otel_5_scenario.py` and
   `${AWS::StackName}-otel-5`).
3. Implement the `Input.scenario` contract with the SDK's public durable
   execution and OTel APIs.
4. Accept the OTel template parameters documented in the package
   [README](README.md).
5. Exercise the scenario with the S3 collector before using a hosted backend.
6. Declare a missing handler under `TestingMetadata.NotImplemented`; its
   `reason` may be empty.

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

For an end-to-end run, start OpenTelemetry Collector Contrib with the example
[`awss3exporter` configuration](examples/collector/config.yaml):

```bash
AWS_REGION=us-west-2 \
OTEL_S3_BUCKET=example-telemetry \
OTEL_S3_PREFIX=durable-execution \
otelcol-contrib --config examples/collector/config.yaml
```

Then run the conformance CLI with `--suite otel`,
`--otel-exporter community`, `--otel-backend collector`, the collector's
reachable OTLP endpoint, and `--otel-backend-endpoint s3://bucket/prefix`.
The backend supports the exporter's `otlp_json` and `otlp_proto` marshalers,
with no compression, gzip, or zstd. Hosted-backend coverage should be added
separately and must read all credentials from environment variables or CI
secrets.

For Lambda-hosted tests, use
[`build-lambda-layer.sh`](examples/collector/build-lambda-layer.sh) with the
pinned upstream collector release. The Python, Java, and TypeScript S3
collector workflows publish the custom `awss3exporter` layer, grant
prefix-scoped S3 access, assert the exported spans, and delete all temporary
resources without changing the corresponding X-Ray workflows.

## Pull-Request Checklist

- The requirement is language-neutral and provider-neutral.
- Java, JavaScript/Node.js, and Python handlers are implemented or their gaps
  are declared.
- The S3 collector exercises the new behavior.
- Unit tests cover success and actionable failure diagnostics.
- Requirement discovery works from both source and built wheels.
- No secrets or provider credentials appear in fixtures, diagnostics, or
  artifacts.
