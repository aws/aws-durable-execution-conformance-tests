# TypeScript Workflow Insight Conformance Examples

This SAM project implements every Workflow Insight conformance scenario with the
AWS Durable Execution SDK for JavaScript and its Workflow Insight plugin:

- [`@aws/durable-execution-sdk-js`](https://www.npmjs.com/package/@aws/durable-execution-sdk-js)
- `@aws/durable-execution-sdk-js-insight`

The project runs on Node.js 22 and bundles one CommonJS entry point per
scenario. Each handler wraps a workflow with `withDurableExecution` +
`workflowInsight({...})`, using the SDK's **real** exporters and config options —
there is no fake exporter and no synthetic record emission.

## Sink selection

`handlers/common.ts` builds the sink exporter from the environment, the same way
the OTel examples select their plugin mode:

| `INSIGHT_SINK` | Exporter | Emits | Capability |
|---|---|---|---|
| `cloudwatch` (default) | `LambdaLogExporter` (`console.log` to the function's log group) | `operationsByName` map | `OPERATIONS_BY_NAME` |
| `s3` | `S3Exporter` (flat layout under a prefix) | canonical `operations` array | `OPERATIONS_ARRAY` |

The `s3` sink reads `INSIGHT_S3_BUCKET` and `INSIGHT_S3_PREFIX`; the function's
execution role must grant `s3:PutObject` on that bucket/prefix (see the IAM note
at the top of `template.yaml`). The `cloudwatch` sink needs no extra IAM.

Requirements that assert on the `operations` array (e.g. top-level filtering,
truncation drop order) require the `s3` sink; requirements that assert on the
`operationsByName` summary (e.g. repeated operation names) require the
`cloudwatch` sink. The runner gates each requirement on the sink's capability.

## Scenarios

| Requirement | Handler | Plugin config | Behavior |
|---|---|---|---|
| `insight-1` | `insight_basic_success.handler` | defaults | One STEP/Step op, attempt 1, input/output echoed. |
| `insight-2` | `insight_execution_failure.handler` | defaults | Step fails (no retry) → FAILED execution + FAILED op. |
| `insight-3` | `insight_on_failure_success.handler` | `emitMode: on-failure` | Success → no record. |
| `insight-4` | `insight_on_failure_failure.handler` | `emitMode: on-failure` | Failure → exactly one FAILED record. |
| `insight-5` | `insight_wait_single_record.handler` | defaults | Wait/resume → single terminal record, no RUNNING. |
| `insight-6` | `insight_step_retry.handler` | defaults | Real `retryStrategy`; op `attempt`/summary `maxAttempt` reflect the retry. |
| `insight-7` | `insight_repeated_operation_name.handler` | defaults | Same name on 3 steps → summary `count: 3`, `result`/`error` dropped. |
| `insight-8` | `insight_sampling_excluded.handler` | `samplingRate: 0` | Sampled out → no record. |
| `insight-9` | `insight_content_omitted.handler` | `content.input/output: false` | Input/output omitted; no dropped-* flags. |
| `insight-10` | `insight_errors_excluded.handler` | `content.operations.includeErrors: false` | FAILED execution but op `error` excluded. |
| `insight-11` | `insight_operation_result_opt_in.handler` | `content.operations.overrides` | Op `result` opted in (identity), equals checkpointed value. |
| `insight-12` | `insight_truncation.handler` | tiny `maxRecordSizeBytes` + oversized results | Real size limiter → `truncated: true`, `droppedOperations > 0`. |
| `insight-13` | `insight_top_level_only.handler` | default `operationDetail: top-level` | Parallel: only the CONTEXT parent; children dropped. |
| `insight-14` | `insight_full_tree.handler` | `operationDetail: full-tree` | Child context + nested step; child `parentId` == parent `id`. |
| `insight-15` | `insight_subtype_coverage.handler` | `full-tree` | step/wait/callback/child-context type+subType pairs. |

## Build

Node.js 22 or newer is required. The Insight plugin's API is newer than any
published npm artifact, so `scripts/install-sdk-main.sh` builds and installs the
SDK core and Insight packages from source before the examples compile. Set
`SDK_SOURCE_DIR` to build from an existing local checkout instead of cloning
`main`.

```bash
cd packages/aws-durable-execution-conformance-tests-insight/examples/typescript
npm ci
npm run install-sdk-main          # or: SDK_SOURCE_DIR=/path/to/aws-durable-execution-sdk-js npm run install-sdk-main
npm run typecheck
npm run build                     # -> dist/*.js
sam build --template-file template.yaml
```

## Run against the suite

Deploy and validate with the conformance runner, choosing a sink. Example for
the S3 sink (canonical `operations` array):

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-insight/examples/typescript/template.yaml \
  --language javascript \
  --suite insight \
  --insight-sink s3 \
  --insight-sink-endpoint "s3://$INSIGHT_BUCKET/insight" \
  --parameter-overrides \
    LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
    InsightSink=s3 \
    InsightS3Bucket="$INSIGHT_BUCKET" \
    InsightS3Prefix=insight
```

For the CloudWatch sink (`operationsByName` summary), pass `--insight-sink
cloudwatch` and `InsightSink=cloudwatch` (the default); no bucket is needed.
