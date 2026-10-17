# Contributing Workflow Insight Test Cases

This guide covers the `insight` conformance suite. Read the repository
[contribution guide](../../CONTRIBUTING.md) first for the general development,
security, and pull-request requirements.

## What Belongs in the Suite

An insight requirement describes an observable property of the record the SDK's
`workflowInsight({...})` plugin emits for a real durable execution. Good
requirements assert stable outcomes such as:

- a terminal record is emitted with the right `status`, `input`, and `output`;
- an operation carries the right `type`/`subType`/`name`/`attempt`/`status`;
- `emitMode`, `samplingRate`, `content`, and `operationDetail` behave as
  documented (e.g. sampled-out or on-failure-success emits **no** record);
- truncation flags (`truncated`, `droppedOperations`, `droppedInput`,
  `droppedOutput`) reflect an oversized record.

Each handler must use the SDK's real plugin and a real exporter injected from
deployment configuration — never emit synthetic records or hand-roll a shape to
make a case pass. Restrict `emitMode` to `on-complete` / `on-failure`;
`on-change` coalesces intermediate records nondeterministically and is not
assertable.

## Add a Requirement

Add requirements as `test-requirements/insight/insight-N.yaml`. Requirement IDs
are global and must not be reused. A requirement contains the normal execution
expectations plus an `InsightAssertions` mapping:

```yaml
description: Insight basic success emits one terminal record
Input: World
AsyncInvoke: true
ExpectedResult:
  ExecutionStatus: SUCCEEDED
InsightAssertions:
  requires: [OPERATIONS_ARRAY]   # optional; skipped when the sink can't express it
  record_count: 1                # or min_record_count / max_record_count
  records:                       # ordered by emittedAt, index-aligned, partial
    - expect:
        recordType: WorkflowInsight
        schemaVersion: '1.0'
        status: SUCCEEDED
        input: World
        output: Hello, World!
      absent: [error, truncated, droppedInput, droppedOutput]
      operations:
        - select: {name: greet}
          count: 1
          expect: {type: STEP, subType: Step, status: SUCCEEDED, attempt: 1, id: ${OP1}}
          absent: [parentId, result]
      operations_by_name:        # requires OPERATIONS_BY_NAME
        greet:
          expect: {type: STEP, count: 1, failedCount: 0, status: SUCCEEDED}
          absent: [result, error]
```

### Matcher semantics

- `'*'` matches any present value; `/regex/` (or `${/regex/}`) is a regex search;
  `$any_of: [...]` is a set of alternatives.
- `${NAME}` binds on first match and must stay consistent within the requirement.
  Core-resolved placeholders and `EXECUTION_ARN` are substituted before matching.
- Mappings match partially (only listed keys are checked); sequences match in
  order with exact length.
- `absent:` asserts a key is **not present** — a key present with a `null` value
  is a violation. Use it for the config-omit vs size-drop distinction and for
  `operationDetail: top-level` child suppression.
- `record_count: 0` is the absence assertion (sampling / on-failure-success).

### Determinism

Assert only stable fields: `recordType`, `schemaVersion`, `status`, operation
`type`/`subType`/`name`/`attempt`/`status`, `input`, `output`, truncation flags,
and summary `count`/`failedCount`. Wildcard timestamps, durations, `region`,
`accountId`, `functionQualifier`, `executionName`. Operation `id`/`parentId` are
deterministic but opaque — bind them as `${OP1}` or match `/^[0-9a-f]{16}$/`,
never hardcode. Assert `error.name` only, never `error.message`.

## Sinks and Capabilities

The default `s3` sink reads the lossless `operations` array
(`OPERATIONS_ARRAY`); the `cloudwatch` sink reads the name-keyed
`operationsByName` summary (`OPERATIONS_BY_NAME`). A requirement that needs the
per-occurrence array must declare `requires: [OPERATIONS_ARRAY]` so it is
reported UNCOVERED rather than failed under a summary-only sink. Do not derive
one shape from the other to force coverage.
