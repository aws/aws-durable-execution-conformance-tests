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

Do not standardize provider-specific response fields, exact span names, every
span attribute, or language-specific implementation details. Backend adapters
must normalize provider responses into the canonical `Trace` model before the
requirement is evaluated.

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
  require_continuation: true
  required_outcomes:
    - success
```

The currently supported telemetry assertions are:

| Key | Meaning |
|---|---|
| `minimum_spans` | Minimum number of normalized spans; defaults to `1`. |
| `minimum_invocations` | Minimum distinct Lambda invocation IDs; defaults to `1`. |
| `require_execution_correlation` | Require the durable execution ARN on the trace; defaults to `true`. |
| `require_continuation` | Require an in-trace parent or span-link relationship. |
| `require_log_trace_correlation` | Require backend-provided log trace IDs to match the active trace. |
| `required_outcomes` | Require outcomes such as `retry`, `success`, or `failure`. |
| `span_assertions` | Select one span and assert arbitrary canonical properties or metadata. |

`span_assertions` accepts one mapping or a list of mappings. Each `select`
mapping must match exactly one span. The corresponding `expect` mapping is a
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
      parent_span_id: "*"
      attributes:
        durable.operation.outcome: success
```

Both `select` and `expect` can use any canonical span property: `trace_id`,
`span_id`, `parent_span_id`, `name`, `start_time`, `end_time`, `status`,
`service_name`, `attributes`, or `links`. Nested mappings support arbitrary
attribute metadata without interpreting provider-specific keys. Use `"*"` when
a property must exist but its value is intentionally dynamic. Sequence
assertions, including `links`, compare length, order, and nested values.

Keep `ExpectedExecutionHistory` and `ExpectedResult` focused on the execution
behavior needed to produce the telemetry. Keep `TelemetryAssertions` portable
across the complete exporter/backend support matrix.

## Add SDK Test Handlers

The matching test handler and deployment template belong in each supported SDK
repository. For Java, JavaScript/Node.js, and Python:

1. Map a function to the new requirement ID using
   `TestingMetadata.TestDescription`.
2. Implement the `Input.scenario` contract with the SDK's public durable
   execution and OTel APIs.
3. Accept the OTel template parameters documented in the package
   [README](README.md).
4. Exercise the scenario with the test collector before using a hosted backend.
5. Declare a genuine SDK gap as `NOT_IMPLEMENTED` with a reason instead of
   omitting the requirement.

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

For an end-to-end local run, start the test collector:

```bash
durable-execution-otel-collector --host 0.0.0.0 --port 4318
```

Then run the conformance CLI with `--suite otel`,
`--otel-exporter community`, and `--otel-backend collector`. Hosted-backend
coverage should be added separately and must read all credentials from
environment variables or CI secrets.

## Pull-Request Checklist

- The requirement is language-neutral and provider-neutral.
- Java, JavaScript/Node.js, and Python handlers are implemented or their gaps
  are declared.
- The test collector exercises the new behavior.
- Unit tests cover success and actionable failure diagnostics.
- Requirement discovery works from both source and built wheels.
- No secrets or provider credentials appear in fixtures, diagnostics, or
  artifacts.
