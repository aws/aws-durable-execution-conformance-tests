# AWS Durable Execution Workflow Insight Conformance

Optional Workflow Insight integration suite for
`aws-durable-execution-conformance-tests`. Installing this distribution adds the
`insight` suite to the existing runner through a Python entry point; it does not
install a second conformance CLI.

## Install

```bash
pip install aws-durable-execution-conformance-tests-insight
```

The package requires a compatible `>=1.0,<2.0` core runner and owns its
canonical record model, wire normalizers, sink adapters, the `InsightAssertions`
matcher, and its requirement resources.

The suite pairs ordinary durable-execution scenarios with assertions about the
**Workflow Insight** records the SDK's `workflowInsight({...})` plugin emits
through a configured exporter (a step succeeds, a retry bumps `attempt`, a
sampled-out execution emits nothing, a truncated record sets `truncated`, etc.).
Only JavaScript ships an insight plugin today, so Python and Java are
`NotImplemented` for this suite.

## Run

```bash
durable-execution-conformance \
  --template path/to/template.yaml \
  --language js \
  --suite insight \
  --insight-sink s3 \
  --insight-sink-endpoint s3://example-insight/workflow-insight
```

The runner deploys the functions, invokes each once, then fetches the emitted
records from the chosen sink and diffs them against each requirement's
`InsightAssertions`. On a mismatch the normalized record(s) are written to
`<history-dir>/<id>-insight.json`.

## Sinks

Each sink retrieves records **execution-scoped** on the record's top-level
`executionArn` (which equals the runner's execution ARN) and returns them
ordered by `emittedAt`. Retrieval retries until the records satisfy the
assertions or the poll budget elapses, so ingestion lag is tolerated; an
absence assertion (`record_count: 0`) passes only after the window closes.

| `--insight-sink` | Reads | Emits shape | Capability |
|---|---|---|---|
| `s3` (default) | objects under `s3://bucket/prefix` written by `S3Exporter` | canonical `operations` array | `OPERATIONS_ARRAY` |
| `cloudwatch` | `FilterLogEvents` on the function's log group (`LambdaLogExporter`) | `operationsByName` map | `OPERATIONS_BY_NAME` |

CLI flags: `--insight-sink`, `--insight-sink-endpoint`
(`s3://bucket/prefix`, or a CloudWatch log group name), `--insight-poll-timeout`,
`--insight-poll-interval`, `--insight-poll-attempts`.

`deployment_parameters()` injects the S3 bucket/prefix (`InsightSink=s3`, `InsightS3Bucket` /
`InsightS3Prefix`) into SAM for the s3 sink; the cloudwatch sink pins `InsightSink=cloudwatch` and reads the
function's own log group and injects nothing. `validation_client_services()`
declares the boto3 clients the sink needs (`s3` or `logs`). The runner's AWS
identity needs `s3:ListBucket` + `s3:GetObject` (s3 sink) or
`logs:FilterLogEvents` (cloudwatch sink).

## Capability gating

A requirement may declare `InsightAssertions.requires: [OPERATIONS_ARRAY]`.
When the selected sink cannot express that shape (e.g. the CloudWatch sink,
which carries only the name-keyed `operationsByName` summary), the requirement
is reported **UNCOVERED** and skipped — never failed. Per-occurrence arrays are
genuinely absent from point-access stores, so this is a capability gate, not a
workaround.

## Third-Party Sinks

Additional sinks register an entry point in
`aws_durable_execution_conformance_tests_insight.sinks`. A sink factory exposes
`name`, `capability`, optional `client_services` / `deployment_parameters` /
`validate_configuration`, and a `create_with_clients(options, *, region,
function_name, aws_clients)` returning a `PollingSink`. Names must be unique.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidance on adding insight
requirements, SDK test handlers, and the `InsightAssertions` schema.
