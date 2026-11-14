# Reusing the LMI lifecycle harness

The core package contains the driver for all seven lifecycle requirements. It
runs on Python 3.14 or later, including when the SDK under test is Java or
JavaScript. It imports boto3 and the conformance package; it does not import any
language SDK or SDK testing package. The Lambda bundle contains the SDK fixture,
not this driver.

## SDK-owned bindings

| Shared API | SDK supplies |
| --- | --- |
| `runtime.RuntimeDefinition` | Runtime model (`python`, `java`, `javascript`), handler, architecture, memory |
| `deployment.DeploymentManager` | Runtime definition, stack owner tag, artifact directory, ZIP path, bucket prefix |
| `cloud.Cloud` | Deployment manifest, artifacts directory, `verify_profile=manager.verify` |
| `scenarios.ScenarioDriver` | Runtime model and fixture-name mapping |
| `source_evidence.build_evidence` | Runtime model, expected-outcome callback and observed worker-identity callback |
| `adapter.SharedAdapter` | Cloud, scenario-driver and evidence factories; configuration-identity callback |
| `runner.make_manifest` | Exact SDK artifact identity and conformance revision; runtime/configuration callback |
| `runner.run`, `runner.cleanup`, `summary.summarize` | Manifest/adapter factory or run artifacts |

Construct a deployment manager with explicit SDK choices, for example:

```python
from pathlib import Path
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.runtime import RuntimeDefinition

runtime = RuntimeDefinition("javascript", "index.handler", architecture="arm64")
manager = DeploymentManager(
    runtime=runtime,
    owner="javascript-sdk-lmi-e2e",
    artifacts=Path("lmi_tests/artifacts"),
    bundle=Path("lmi_tests/build/function.zip"),
)
```

`deploy(args)` accepts `run_id`, `stack_name`, and the Lambda `runtime` identifier.
It reads `AWS_REGION`, `TEST_ACCOUNT_ID`, `CAPACITY_PROVIDER_ARN`, and
`TEST_LAMBDA_EXECUTION_ROLE_ARN`. The build script must create the ZIP and
`artifacts/build.json` with the SDK `commit` and ZIP `sha256` (hex). Optional
`wheels` and `dependencies` metadata is retained for Python artifact auditing.
Other SDKs pass their own artifact hash to `make_manifest`. The controller keeps
the bucket private before upload, verifies the actual published configuration,
waits for earlier executions before updates, and retains persistent resources.

Map the seven scenario keys plus `peer`, `recovery`, and `warm` to your fixture
entry points. The shared driver passes the fixture name in `scenario`, the
catalog name in `sharedScenario`, and preserves `run`, `caseId`, `marker`, role,
control prefix, gates, and behavior budgets. Fixtures return their marker on
success; the normal-failure fixture throws a language-appropriate error.
`expected_outcome(item, scenario)` describes expected service outcomes, never
observed facts. `worker_identity(raw)` returns the actual worker identity from
the SDK's source record (a Python process, JVM worker, or Node.js worker).

## Observation protocol

The shared driver reads immutable JSON records from
`runs/<run>/events/<marker>/<request>/<sequence>.json` and controls from the
case's assigned `controlPrefix`. An SDK fixture must supply `run`, `commit`,
`caseId`, `marker`, `request`, `execution`, `environment`, `process`, `pid`,
`sequence`, `time` (epoch seconds), and `phase`. JavaScript additionally reports
its actual `worker`. `WRAPPER_ENTER` includes the observed native `deadline`.
`BLOCKED`, `ALIVE`, `EFFECT`, and `IO_EXIT` retain the control `gate`; positive
liveness records establish overlap, not absence of an exit record.

For detailed evidence, `CONFORMANCE` records contain an `observation` object
using the event kinds and identities in the [evidence contract](README.md).
The source assembler preserves actual timestamps, checks sequence continuity,
associates native retries with the original launch, and decodes Lambda history.
Replay fixtures use stable operation names `success`, `failure`, `retry`, and
callback `completion`; their body records retain operation IDs and attempts.
The shared runner compares actual checkpoints and outcomes. SDK hooks must
observe the real public API and must not emulate an unsupported operation.

Control records and service history remain separate from injected faults. The
case 4 driver admits the original peer halfway through the victim's timeout,
checks its remaining recovery window, and excludes expired originals or retries
from healthy-peer evidence. Retirement retains all observed user/probe tasks,
including those outside the scenario-owned task's assertion stream.

## Reusable GitHub Actions workflow

Use `.github/workflows/lmi-lifecycle.yml` from this repository. Pin both `uses`
and `conformance-ref` to the same full commit SHA. The workflow checks out the
calling SDK repository and this repository at `.conformance`.

```yaml
jobs:
  lifecycle:
    permissions:
      contents: read
      id-token: write
    uses: aws/aws-durable-execution-conformance-tests/.github/workflows/lmi-lifecycle.yml@<full-commit-sha>
    with:
      conformance-ref: <full-commit-sha>
      sdk-script: .github/scripts/lmi-ci.sh
      runtime-label: JavaScript
      lambda-runtime: nodejs24.x
      stack-name: javascript-lmi-e2e
      region: us-west-2
    secrets:
      TEST_ROLE_ARN: ${{ secrets.TEST_ROLE_ARN }}
      TEST_ACCOUNT_ID: ${{ secrets.TEST_ACCOUNT_ID }}
      TEST_LAMBDA_EXECUTION_ROLE_ARN: ${{ secrets.TEST_LAMBDA_EXECUTION_ROLE_ARN }}
      CAPACITY_PROVIDER_ARN: ${{ secrets.CAPACITY_PROVIDER_ARN }}
```

The caller owns triggers and a Bash stage script. The shared workflow invokes
`bash "$LMI_SDK_SCRIPT" <stage>` from the SDK checkout. Every stage must return
nonzero on failure. `setup` runs independently in the harness and cloud jobs.

| Stage | Contract |
| --- | --- |
| `setup` | Install the SDK toolchain and shared core package from `.conformance`; verify the pin |
| `harness` | Run binding tests and local SDK integration tests; write test diagnostics |
| `build` | Build the tested SDK fixture ZIP and build manifest |
| `deploy` | Use `LMI_SHARED_RUN_ID`, `LMI_RUNTIME`, `LMI_STACK_NAME` and AWS configuration |
| `run` | Execute all seven shared requirements and retain strict JSON/JUnit reports |
| `collect` | Collect available runtime records and service history after success or failure |
| `cleanup` | Release this run's controls and request stops for its identified executions |
| `summary` | Print the retained report summary to stdout |

`artifact-directory` defaults to `lmi_tests/artifacts`; it is exposed as
`LMI_ARTIFACT_DIRECTORY`. The shared workflow owns setup of driver Python,
OIDC credentials and refreshes, account verification, deployment/test budgets,
always-run collection/cleanup, artifact upload and the job summary. Cloud work
runs only for same-repository PRs and manual dispatch. Fork and Dependabot PRs
use the hosted unprivileged harness. Trusted jobs require the repository's
`codebuild-github-actions-runner-<run_id>-<run_attempt>` runner integration.

GitHub Actions concurrency is the sole cross-run guard. The fixed
`lmi-e2e-shared-capacity-provider` group covers deployment through cleanup and
uses `cancel-in-progress: false`. Concurrency groups are scoped to a calling
repository; SDK repositories must use separately provisioned test capacity if
they run at the same time. Direct CLI runs are not serialized with Actions.
There is no S3 shared lock. Within a run, the coordinator still verifies
case ownership, capacity, cancellation and retirement before reusing a target.

The Python adoption is [SDK PR #931](https://github.com/aws/aws-durable-execution-sdk-python/pull/931).
Merge the shared PR first, then the SDK caller pinned to it. Harness tests and
local SDK tests do not establish real LMI acceptance; retained cloud reports do.
