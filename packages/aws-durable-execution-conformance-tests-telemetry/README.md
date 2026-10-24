# aws-durable-execution-conformance-tests-telemetry

Optional Durable Execution Telemetry conformance suite for `aws-durable-execution-conformance-tests`. This
distribution adds the `telemetry` suite to the core runner through the
`aws_durable_execution_conformance_tests.extensions` entry point.

The suite checks that the SDK's Durable Execution Telemetry plugin, configured with the
CloudWatch Logs exporter, delivers one complete `DurableExecutionTelemetry` record for a
successful single-step execution.

The SDK handler and SAM template that exercises this suite live in each SDK repository, not here.

## Install

```bash
pip install aws-durable-execution-conformance-tests-telemetry
```

Version of this package `0.1.x` requires a compatiable core runner (`>=1.0.0,<2.0.0`).

## Run

```bash
durable-execution-conformance \
  --template <path/to/template.yaml> \
  --language js \
  --region us-west-2 \
  --suite telemetry \
  --report console json \
  --parameter-overrides LambdaExecutionRoleArn=<role-arn>
```

Options added by this package:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--telemetry-log-group` | `/durable-execution-conformance/conformance-tests-<name>/durable-execution-telemetry` | CloudWatch Logs group that receives the records. |
| `--telemetry-wait-seconds` | `10` | Seconds to wait before the first CloudWatch Logs query. Raise it when ingestion is slow. |

The log group is resolved in this order: an explicit
`--parameter-overrides TelemetryLogGroupName=<group>`, then
`--telemetry-log-group`, then the default. This follows the core rule that
explicit overrides take precedence over values supplied by extensions
([core README, lines 113–115](../aws-durable-execution-conformance-tests/README.md#L113-L115)).
When both flags are given, the parameter override is used and
`--telemetry-log-group` is ignored. The validator queries the group that was
deployed.

After CloudWatch retrieval completes, the extension writes `<id>-telemetry.json`
to the history directory, whether telemetry assertions pass or fail. This includes
empty retrieval results. Configuration and retrieval failures are reported through
the runner’s error reporting and do not produce a telemetry artifact. The directory
is the core `--history-dir` option (default `output/`), the same directory that
receives the execution history files.

## Template contract

The extension passes one SAM parameter, `TelemetryLogGroupName`, with the
resolved log group name. The template must:

1. Declare `TelemetryLogGroupName` as a `String` parameter.
2. Create the group with an `AWS::Logs::LogGroup` resource named from that
   parameter. Extensions have no pre-deploy hook, so CloudFormation must create
   the group before the runner invokes a function.
3. Pass the name to the function, for example through the
   `TELEMETRY_LOG_GROUP_NAME` environment variable, and configure the
   `CloudWatchLogsExporter` with it.

The function's execution role must allow `logs:CreateLogStream` and
`logs:PutLogEvents` on that group. The runner identity must allow
`logs:FilterLogEvents` on it.

## What is asserted

`TelemetryAssertions.records` is an ordered list. Entry `i` describes the
`i`-th record after sorting by CloudWatch event timestamp and ingestion time.
`entry.record` is the complete expected record:

```yaml
TelemetryAssertions:
  records:
    - record:
        recordType: DurableExecutionTelemetry
        schemaVersion: '1.0'
        emittedAt: &iso8601_ms '${/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/}'
        executionArn: ${EXECUTION_ARN}
        executionName: ${EXECUTION_NAME}
        functionName: ${FUNCTION_NAME}
        functionQualifier: ${FUNCTION_QUALIFIER}
        region: ${REGION}
        accountId: ${ACCOUNT_ID}
        status: SUCCEEDED
        startTime: *iso8601_ms
        endTime: *iso8601_ms
        durationMs: {$integer: {min: 0}}
        input: {value: 41}
        output: {value: 42}
        operationsByName:
          increment:
            type: STEP
            count: 1
            failedCount: 0
            status: SUCCEEDED
            subType: Step
            minDurationMs: {$integer: {min: 0}}
            maxDurationMs: {$integer: {min: 0}}
            totalDurationMs: {$integer: {min: 0}}
            maxAttempt: 1
```

The number of fetched records must equal the list length. When the counts
differ, that is the only error reported. An empty list passes when no record
was emitted.

Fetched records are sorted by event timestamp, then ingestion time, ascending.
The artifact’s raw events list preserves the order in which the retriever first observed each event. Only the parsed records list is sorted by timestamp.

For each `(record, entry)` pair the validator compares the record against
`entry.record` as one nested mapping, on that record alone. At every mapping
level, the record must carry exactly the listed keys, and every value is
checked recursively. So `operationsByName` must have exactly the listed
operation names, each summary must have exactly the listed keys, and `input`
and `output` must have exactly the listed payload keys. A key the record
carries but the entry does not list is reported as `unexpected keys`.

This check exists to detect a record that differs between SDKs. A key one SDK
emits and another omits is such a difference. So is a different number format
or a different timestamp format. Key order is not such a difference. A JSON
object is unordered, and no destination reads a field by its position. So the
check ignores the order of keys in a mapping. A list is ordered, so the check
compares list items in order. The entry therefore
describes the record of the reference implementation, the JS SDK, and the
other SDKs must match it.

### Placeholders

A requirement cannot know the execution ARN when it is written. So the entry
names the identity fields through placeholders. Core binds `${EXECUTION_ARN}`;
this suite binds the rest from the parsed ARN before substituting the
assertions:

| Placeholder | Value |
| --- | --- |
| `${EXECUTION_ARN}` | The execution ARN under test. |
| `${EXECUTION_NAME}` | The execution name segment of the ARN. |
| `${FUNCTION_NAME}` | The function name segment of the ARN. |
| `${FUNCTION_QUALIFIER}` | The qualifier segment of the ARN, such as `$LATEST`. |
| `${REGION}` | The region segment of the ARN. |
| `${ACCOUNT_ID}` | The account segment of the ARN. |

A requirement must not bind these names itself under `Variables`. The
extension reports the collision as an error.

### Expected values

Expected values follow the rules of the core execution-history matcher, plus
`$integer`. Two rules are stricter here. A mapping is closed: the record must
carry exactly the listed keys. A number matches only a number of the same JSON
type.

| Expected value | Rule |
| --- | --- |
| `'*'` | The field must be present. Any value passes, including `null`. Use it only for values whose type is not worth asserting. |
| `'${/regex/}'` | `str(actual)` must match the regex. Use it for string formats, such as ISO-8601 timestamps. A number also matches when its decimal text does, so do not use it to assert a numeric type. |
| `{$integer: {min: N}}` | `actual` must be a JSON integer: not a string, boolean, null, or fraction. `min` is optional and inclusive. Use it for per-run counts and durations, such as `durationMs: {$integer: {min: 0}}`. |
| mapping | `actual` must be a mapping with exactly the listed keys, in any order. Every key is checked recursively. |
| list | `actual` must be a list of the same length. Items are checked pairwise. |
| anything else | Equality. A boolean matches only a boolean, so `count: 1` does not match `true`. A number matches only a number of the same JSON type, so `count: 1` does not match `1.0`. |

### Timestamps

`emittedAt`, `startTime`, and `endTime` must have the form
`YYYY-MM-DDTHH:MM:SS.sssZ`: UTC, `Z` suffix, exactly three fraction digits.
The width is fixed because CloudWatch Logs Insights and DynamoDB compare these
values as text, and text order equals time order only when every value has the
same width. Any other width, offset, or suffix fails the check.

Every error is prefixed `record[i]:`; `i` is the index into the `records`
list in the artifact.

The CloudWatch Logs exporter replaces the `operations` array with an
`operationsByName` map. That map has no operation IDs, so the suite does not
compare operation IDs against the execution history.

## Retrieval

Records are fetched with the core `CloudWatchLogRetriever`. The retriever first
sleeps `--telemetry-wait-seconds` (default 10). It then polls `FilterLogEvents` for the full core poll budget of 120 seconds. Events seen on any poll are kept, since a later response can omit a record that an earlier one returned. So each case takes at least 130 seconds. A record that is still absent when polling stops fails the
case as `records: expected 1 record(s), got 0`. Events that fail to parse, or carry a different `recordType`, are listed under `malformed` in the artifact
and are not counted.
