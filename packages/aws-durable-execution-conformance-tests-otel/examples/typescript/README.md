# TypeScript OpenTelemetry Conformance Examples

This SAM project implements all OpenTelemetry conformance scenarios with the
AWS Durable Execution SDK for JavaScript and its OpenTelemetry plugin:

- [`@aws/durable-execution-sdk-js`](https://www.npmjs.com/package/@aws/durable-execution-sdk-js)
- [`@aws/durable-execution-sdk-js-otel`](https://www.npmjs.com/package/@aws/durable-execution-sdk-js-otel)

The project runs on Node.js 22 and bundles one CommonJS entry point per
scenario. The template covers 19 invocation-view and 19 execution-view
requirements; chained-invoke cases 11 and 18 also deploy durable targets for
both views. Execution-view functions reuse the scenario modules and select
`ExecutionOtelPlugin` through deployment configuration.

The OTel package's `InvocationOtelPlugin` API is newer than its latest npm
artifact, so `scripts/install-sdk-main.sh` builds and installs both SDK packages
from the repository's `main` branch before the examples are compiled.

## Scenarios

| Requirement | Handler | Behavior |
|---|---|---|
| `otel-invocation-1` | `otel_1_success.handler` | Successful step and attempt. |
| `otel-invocation-2` | `otel_2_wait_resume.handler` | Wait, resume, and post-resume step. |
| `otel-invocation-3` | `otel_3_retry.handler` | Failed and successful retry attempts. |
| `otel-invocation-4` | `otel_4_terminal_failure.handler` | Terminal step failure. |
| `otel-invocation-5` | `otel_5_child_context.handler` | Child context with a nested step. |
| `otel-invocation-6` | `otel_6_parallel.handler` | Parallel context, branches, and steps. |
| `otel-invocation-7` | `otel_7_map.handler` | Map context, iterations, and steps. |
| `otel-invocation-8` | `otel_8_handled_failure.handler` | Handled failed step and recovery step. |
| `otel-invocation-9` | `otel_9_wait_for_condition.handler` | Two condition polling attempts. |
| `otel-invocation-10` | `otel_10_wait_for_callback.handler` | Callback context, callback, and submitter. |
| `otel-invocation-11` | `otel_11_chained_invoke.handler` | Successful chained invoke. |
| `otel-invocation-12` | `otel_12_child_context_failure.handler` | Failed child context. |
| `otel-invocation-13` | `otel_13_parallel_failure.handler` | Failed parallel branch. |
| `otel-invocation-14` | `otel_14_map_failure.handler` | Failed map iteration. |
| `otel-invocation-15` | `otel_15_wait_interrupted.handler` | Wait interrupted by execution timeout. |
| `otel-invocation-16` | `otel_16_wait_for_condition_failure.handler` | Failed condition check. |
| `otel-invocation-17` | `otel_17_wait_for_callback_failure.handler` | External callback failure. |
| `otel-invocation-18` | `otel_18_chained_invoke_failure.handler` | Failed chained invoke. |
| `otel-invocation-19` | `otel_19_execution_failure.handler` | Direct handler failure. |
| `otel-execution-1` | `otel_1_success.handler` | Execution-view workflow, step, and attempt hierarchy. |
| `otel-execution-2` | `otel_2_wait_resume.handler` | Execution-view hierarchy across a resumed invocation. |
| `otel-execution-3` | `otel_3_retry.handler` | Execution-view hierarchy across retry attempts. |
| `otel-execution-4` | `otel_4_terminal_failure.handler` | Failed workflow, step, and attempt hierarchy. |
| `otel-execution-5` | `otel_5_child_context.handler` | Child-context and nested-step parentage. |
| `otel-execution-6` | `otel_6_parallel.handler` | Parallel context, branch, step, and attempt parentage. |
| `otel-execution-7` | `otel_7_map.handler` | Map context, iteration, step, and attempt parentage. |
| `otel-execution-8` | `otel_8_handled_failure.handler` | Failed and recovery operations under a successful workflow. |
| `otel-execution-9` | `otel_9_wait_for_condition.handler` | Condition polling attempts across invocations. |
| `otel-execution-10` | `otel_10_wait_for_callback.handler` | Callback, submitter, and attempt parentage. |
| `otel-execution-11` | `otel_11_chained_invoke.handler` | Source and target workflow roots for a chained invoke. |
| `otel-execution-12` | `otel_12_child_context_failure.handler` | Failed child context under a failed workflow. |
| `otel-execution-13` | `otel_13_parallel_failure.handler` | Failed parallel branch under its operation. |
| `otel-execution-14` | `otel_14_map_failure.handler` | Failed map iteration under its operation. |
| `otel-execution-15` | Not implemented | Requires a terminal plugin hook after a pending invocation times out externally. |
| `otel-execution-16` | `otel_16_wait_for_condition_failure.handler` | Failed condition operation and attempt. |
| `otel-execution-17` | `otel_17_wait_for_callback_failure.handler` | Failed callback telemetry under one workflow. |
| `otel-execution-18` | `otel_18_chained_invoke_failure.handler` | Source and target failed workflow roots. |
| `otel-execution-19` | Not implemented | Requires retaining the workflow after the handler invocation ends with `RETRY`. |

Execution cases 15 and 19 remain declared under
`TestingMetadata.NotImplemented` because the plugin does not receive a terminal
callback for those service-driven lifecycle transitions.

## Run Against the S3 Collector

The hosted workflow builds a custom OpenTelemetry Lambda collector extension
with `awss3exporter`, publishes it in the test account, and creates a
run-scoped S3 bucket. It then evaluates all 38 requirements with the community
JavaScript instrumentation layer and queries the exported OTLP objects through
the conformance package's `collector` backend.

After building the handlers and collector layer, the equivalent runner command
is:

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/typescript/template.yaml \
  --language javascript \
  --suite otel-invocation otel-execution \
  --parameter-overrides \
    LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
    OtelCollectorLayerArn="$COLLECTOR_LAYER_ARN" \
    OtelCollectorBucket="$OTEL_S3_BUCKET" \
    OtelCollectorPrefix=traces \
  --otel-exporter community \
  --otel-endpoint http://localhost:4318 \
  --otel-service-name invocation \
  --otel-backend collector \
  --otel-backend-endpoint "s3://$OTEL_S3_BUCKET/traces"
```

The template adds both the JavaScript instrumentation layer selected by the
runner and `COLLECTOR_LAYER_ARN`. The collector layer's
`/opt/collector-config/config-s3.yaml` listens on localhost, writes
gzip-compressed OTLP JSON to the run prefix, and uses the function's AWS
credentials for S3.

## Build Only

Node.js 22 or newer is required:

```bash
cd packages/aws-durable-execution-conformance-tests-otel/examples/typescript
npm ci
npm run install-sdk-main
npm run typecheck
npm run build
sam build --template-file template.yaml
```
