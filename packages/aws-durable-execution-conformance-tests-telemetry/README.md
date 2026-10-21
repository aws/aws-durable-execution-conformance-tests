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

The extension writes `<id>-telemetry.json` to the history directory on every
run, pass or fail. The directory is the core `--history-dir` option (default
`output/`), the same directory that receives the execution history files.

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
`i`-th record the plugin emitted for the execution:

```yaml
TelemetryAssertions:
  records:
    - record:
        schemaVersion: '1.0'
        status: SUCCEEDED
        input: {value: 41}
        output: {value: 42}
      forbid_fields: [error]
      operations_by_name:
        increment: {type: STEP, status: SUCCEEDED, count: 1, failedCount: 0}
```

The number of fetched records must equal the list length. When the counts
differ, that is the only error reported. An empty list passes when no record
was emitted.

Fetched records are matched to entries in the order the retriever returned
them. Every current requirement expects one record, so no ordering rule is
defined yet.

For each `(record, entry)` pair the validator checks, on that record alone:

- every key under `entry.record` by equality (`schemaVersion`, `status`,
  `input`, `output`, ...);
- `executionArn`, `functionName`, `functionQualifier`, `region`, `accountId`
  derived from the execution ARN;
- every key under `entry.forbid_fields` is absent;
- `operationsByName` has exactly the names under `entry.operations_by_name`,
  and each listed operation has the expected `type`, `status`, `count`,
  `failedCount` and no `error`.

`recordType` is not asserted because only `DurableExecutionTelemetry` records
are parsed. Unknown fields are ignored. Every error is prefixed `record[i]:`;
`i` is the index into the `records` list in the artifact.

The CloudWatch Logs exporter replaces the `operations` array with an
`operationsByName` map. That map has no operation IDs, so the suite does not
compare operation IDs against the execution history.

## Retrieval

Records are fetched with the core `CloudWatchLogRetriever`, which waits
`--telemetry-wait-seconds`, then queries for a fixed ten-second window. A record
that lands after that window fails the case as "no DurableExecutionTelemetry record".
