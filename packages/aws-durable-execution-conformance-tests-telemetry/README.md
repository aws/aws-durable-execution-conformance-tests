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
level, the record must carry exactly the listed keys, in the listed order, and
every value is checked recursively. So `operationsByName` must have exactly
the listed operation names, each summary must have exactly the listed keys,
and `input` and `output` must have exactly the listed payload keys. A key the
record carries but the entry does not list is reported as `unexpected keys`.
Key order is reported only when the key sets already match, so one defect
produces one error.

This check exists to detect a record that differs between SDKs. A key one SDK
emits and another omits is such a difference. So is a different key order, a
different number format, or a different timestamp format. The entry therefore
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
| mapping | `actual` must be a mapping with exactly the listed keys, in the listed order. Every key is checked recursively. |
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

## Reusable GitHub Actions workflow

SDK repositories can call
[`telemetry-suite.yml`](../../.github/workflows/telemetry-suite.yml#L1-L59).
The SDK repository owns its handlers, SAM template, and language build commands.
The shared workflow installs the runner and telemetry extension, deploys the
handlers, validates the telemetry suite, and uploads reports.

Pin the reusable workflow to a reviewed full commit SHA. The example below uses
`<workflow-commit-sha>` as a placeholder. That commit must contain both the
workflow and the telemetry package.

```yaml
permissions: {}

jobs:
  telemetry:
    if: >-
      github.event_name != 'pull_request' ||
      (github.actor != 'dependabot[bot]' &&
       github.event.pull_request.head.repo.full_name == github.repository)
    permissions:
      contents: read
      id-token: write
    uses: aws/aws-durable-execution-conformance-tests/.github/workflows/telemetry-suite.yml@<workflow-commit-sha>
    with:
      language: js
      resource_prefix: js
      sdk_repository: aws/aws-durable-execution-sdk-js
      sdk_ref: ${{ github.event.pull_request.head.sha || github.sha }}
      examples_dir: .build/durable-sdk/packages/aws-durable-execution-sdk-js-conformance-tests-telemetry
      setup_command: |
        source "$SDK_CHECKOUT/.github/workflows/scripts/setup-node22.sh"
      prepare_command: |
        export ELECTRON_SKIP_BINARY_DOWNLOAD=1
        cd "$SDK_CHECKOUT"
        npm ci
        npm run build -w @aws/durable-execution-sdk-js
        npm run build -w @aws/durable-execution-sdk-js-telemetry-exporter
        npm run test -w @aws/durable-execution-sdk-js-conformance-tests-telemetry
        npm run build -w @aws/durable-execution-sdk-js-conformance-tests-telemetry
    secrets:
      CONFORMANCE_TEST_ROLE_ARN: ${{ secrets.TEST_ROLE_ARN }}
      CONFORMANCE_TEST_ACCOUNT_ID: ${{ secrets.TEST_ACCOUNT_ID }}
      CONFORMANCE_TEST_LAMBDA_EXECUTION_ROLE_ARN: ${{ secrets.TEST_LAMBDA_EXECUTION_ROLE_ARN }}
```

### Inputs and execution

Required inputs are `language`, `resource_prefix`, `sdk_repository`, `sdk_ref`,
and `examples_dir`. Supply a full 40-character SDK commit SHA. The resource
prefix must start with a lowercase letter and contain 1-8 lowercase letters,
digits, or hyphens. Language accepts the same characters with a 20-character limit.

The SDK checkout is `.build/durable-sdk`. `examples_dir` is relative to the
conformance checkout and must contain `template.yaml`. Both optional command
hooks run in one Bash shell, from the conformance checkout, with `SDK_CHECKOUT`
set to the absolute SDK directory. A sourced toolchain setup therefore applies
to the preparation command. A failed command stops the job before deployment.
Only trusted caller workflows should supply these commands or choose revisions.
The workflow guard does not replace repository approval policies, runner
admission controls, or the deployment role's OIDC trust policy.

Optional inputs:

| Input | Default | Purpose |
| --- | --- | --- |
| `setup_command` | Empty | Select the SDK's language toolchain. |
| `prepare_command` | Empty | Build and test the SDK and handlers. |
| `conformance_test_ref` | Called workflow's commit | Override the runner and requirements revision for manual testing. |
| `aws_region` | `us-west-2` | Deployment and validation region. |
| `runs_on` | `ubuntu-latest` | Runner label; supports a CodeBuild label. |
| `timeout_minutes` | `30` | Job timeout. |

The conformance checkout always comes from the called workflow's repository.
An override changes the installed runner and requirements, not the workflow
steps. The job summary records the checked-out conformance and SDK commits.
The core and telemetry packages are installed together from that checkout.
Python 3.14 is installed through uv to support both Ubuntu and Amazon Linux.

### AWS permissions and resource lifecycle

The deployment role must support SAM build/deploy, Lambda invocation and durable
execution inspection, and CloudWatch Logs reads. It must allow
`logs:FilterLogEvents` on the telemetry group. CloudFormation needs permission to
create that group. The Lambda role needs `logs:CreateLogStream` and
`logs:PutLogEvents` on it. The template contract above still applies.

The worker checks the assumed account against `CONFORMANCE_TEST_ACCOUNT_ID`
before inspecting or changing a stack. Fork and Dependabot pull requests skip
cloud execution. Use `pull_request`, not `pull_request_target`, for SDK PR runs.

The persistent stack is `conformance-tests-<resource_prefix>-telemetry`.
Deployments are queued by prefix and region without cancelling an active run.
A stack in `ROLLBACK_COMPLETE` is deleted and deletion is awaited before retrying.
Successful and failed test runs otherwise keep the stack for reuse and diagnosis.
Remove test resources manually when retiring a caller.

Concurrency groups apply within one GitHub repository. Assign distinct resource
prefixes to callers in different repositories that share an AWS account and
region. For example, use `js` in the JS SDK and `ct-js` for a conformance-repository
self-test. Do not add a caller-level cancellation policy that interrupts deployment.

### Reports and failures

The worker runs `--suite telemetry --fail-on failed+uncovered`. Failed checks and
non-optional uncovered requirements fail the job. Declared `NOT_IMPLEMENTED`
requirements retain the core runner's non-blocking behavior.

Reports include console output, the GitHub job summary, JSON, and JUnit XML.
The artifact contains those machine reports and the history directory, including
`*-telemetry.json` diagnostics when retrieval produced them. Artifact upload runs
after success or failure. Configuration or deployment failures may occur before
reports exist. CloudFormation failure events are printed only after account
verification succeeds.
