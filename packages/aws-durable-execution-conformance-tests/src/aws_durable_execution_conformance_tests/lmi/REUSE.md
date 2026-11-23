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
| `scenarios.ScenarioDriver` | Runtime model, fixture-name mapping, and optional callback encoder |
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

Each logical launch derives a stable, 64-character `DurableExecutionName` from
its run, configuration, case, and marker. The driver records this name before
Invoke and reuses it with the same payload across transport retries, so a lost
response cannot create a second execution. New logical launches use new markers.

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
using the event kinds and identities in the [evidence contract](evidence-schema.md).
The source assembler preserves actual timestamps, checks sequence continuity,
associates native retries with the original launch, and decodes Lambda history.

Each native request publishes a final ledger record with `phase: "SEALED"`,
`sourceProtocol: 2`, and a nonnegative integer `throughCapture`. Use the
same request and launch identity as its preceding records. There must be
exactly one seal at the highest raw sequence. Every `CONFORMANCE` observation
before the seal carries a unique positive `captureSequence`, with no gaps
through `throughCapture`. Register fixture background work before it can run.
Seal only after that work and all captures have finished publishing. A lost
publication or work that can still publish prevents sealing.

Lifecycle and SDK request coverage require a valid seal for every native
request discovered by either the fixture ledger or Lambda history. The
driver independently samples terminal execution state before fetching all
history pages. It requires consecutive service event IDs, a matching
terminal execution event, and completion records for the known native
requests. History for an execution still running at the cutoff is
provisional. Observations that occurred after the behavior cutoff remain in
raw artifacts and are checked during retirement. A later retry is omitted
from frozen behavior only when Lambda history proves its native start was
after the cutoff. The first attempt for each role and the full retirement
journal remain subject to the normal ownership checks. A request killed before its
seal can be written leaves the evidence incomplete and keeps the target
fenced even if Lambda reports native completion.

The only exception to the seal rule above is physical retirement of a request
the service proves it rejected before any handler ran: an allowlisted
`InvocationCompletedDetails.Error`, on an owned execution, with no ledger row
of any phase and no binding to a launch. Such a request settles as
`UNDISPATCHED` with its proof fields and is exempt only from the seal
requirement; behavior validation still reports it incomplete, so a retry can
never stand in for the original.

That exception depends on a binding requirement for the fixture wrapper: it
must durably publish `WRAPPER_ENTER` before any SDK or handler code runs, and
it must fail the invocation without running the handler if that publication
fails. "No ledger row at all" is what distinguishes a pre-dispatch platform
rejection from a handler that ran and let the same exception type escape; a
wrapper that can run handler code before (or despite a failure of) publishing
`WRAPPER_ENTER` breaks that distinction and makes the exception unsafe.

The deadline and peer-recovery scenarios freeze behavior at the measurement
cutoff. The driver records the victim's execution status and may stop that
execution after the cutoff to close its service history. It leaves fault
controls held during this read. A stop after the cutoff cannot supply a
terminal outcome to the frozen behavior snapshot. Service history proof
still requires the terminal event and matching native completions.

Replay fixtures use stable operation names `success`, `failure`, `retry`, and
callback `completion`; their body records retain operation IDs and attempts.
The callback payload defaults to UTF-8 marker bytes. A fixture using JSON or
another serializer can pass its encoder as `callback_encoder` to
`ScenarioDriver`. A fixture that suspends through a durable timer passes
`suspension_mode="wait"`. The driver awaits the execution result and requires
the same wait operation's service history to span a PENDING decision and a
later native invocation. Callback fixtures send the response only after the
native request that registered the outstanding callback has returned PENDING.
Record replay body effects in `CONFORMANCE` observations
with their durable operation IDs. When the fixture also publishes raw `BODY`
rows, the driver cross-checks them by native request, operation name, and
attempt. The standard fixture expects one effect each for `success` and
`failure` and two for `retry`. A binding with different fixture behavior
can use `expected_replay_effects` in `build_evidence` to set counts per
operation name. The count applies separately to each matching durable
operation ID. Operations without a configured name expect zero effects. The shared runner compares actual checkpoints
and outcomes. SDK hooks must
observe the real public API and must not emulate an unsupported operation.

Control records and service history remain separate from injected faults. The
case 4 driver admits the original peer halfway through the victim's timeout,
checks its remaining recovery window, and excludes expired originals or retries
from healthy-peer evidence. Retirement retains all observed user/probe tasks,
including those outside the scenario-owned task's assertion stream.

## Reusable GitHub Actions workflow

Use `.github/workflows/lmi-lifecycle.yml` from this repository, pinned in `uses`
to a full commit SHA. The workflow checks out the calling SDK repository and,
at `.conformance`, this repository at the commit of the reusable workflow itself
(`job.workflow_sha`), exposed as `CONFORMANCE_REVISION`. The optional
`conformance-ref` input must equal that commit when set. A mismatch fails the
harness job, so the cloud job is skipped.

```yaml
jobs:
  lifecycle:
    permissions:
      contents: read
      id-token: write
    uses: aws/aws-durable-execution-conformance-tests/.github/workflows/lmi-lifecycle.yml@<full-commit-sha>
    with:
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
| `deploy` | Use `LMI_SHARED_RUN_ID`, `LMI_RUNTIME`, `LMI_STACK_NAME` and AWS configuration; write `manifest.json` to `$LMI_ARTIFACT_DIRECTORY` by passing that directory as `DeploymentManager`'s `artifacts`, or the step fails |
| `run` | Execute all seven shared requirements and retain strict JSON/JUnit reports |
| `collect` | Collect available runtime records and service history after success or failure |
| `cleanup` | Release this run's controls and request stops for its identified executions |
| `summary` | Print the retained report summary to stdout |

`artifact-directory` defaults to `lmi_tests/artifacts`, also when the caller passes
an empty value; it is exposed as `LMI_ARTIFACT_DIRECTORY`. Use a plain workspace
path without glob characters, because the upload treats it as a pattern. The
workflow uploads only that directory and skips hidden files and directories, so
keep the `run` stage's case output, including its reports and
`collected-evidence.json`, in a visible path inside it. Leave the SDK journal root
(`sdk.artifacts_dir`) at its default `source/` under each coverage-profile output
directory, such as `standard/`, or another visible path inside that directory, so the journals are uploaded and cleanup can
locate them in a downloaded copy. For direct CLI use, give each run a separate artifact
directory and pass that same directory to build, deploy, run, collect, and cleanup.
Deployment archives existing output only after it holds the target fences.
Writers and cleanup use a nonblocking POSIX lock alongside each journal.
Automatic recovery of an uncertain write requires the original artifact root,
lock inode, and Linux boot identity. Keep the lock files with the journals.
The uploaded diagnostics include the plan and deployment journals, which name
the test account ID, the execution-role and capacity-provider ARNs, and the
bucket. They are identifiers, not credentials, but anyone who can read the
caller repository's Actions artifacts can read them. Recovery needs the
downloaded artifact, so they are not redacted.
The shared workflow owns setup of driver Python,
OIDC credentials and refreshes, account verification, deployment/test budgets,
always-run collection/cleanup, artifact upload and the job summary. The workflow
configures the test-role session only after `setup` and `build`. Those stages still
run in the cloud job, which can request the OIDC token, so they must run trusted
code only. Cleanup has 30 minutes, because it also
reconciles an interrupted deployment update. A `deploy` stage that succeeds without
writing `$LMI_ARTIFACT_DIRECTORY/manifest.json` fails the job. Collection and cleanup
run only when that manifest exists, and the cloud job fails if that directory is
empty when diagnostics are uploaded. Cloud work runs for trusted
`push`, `schedule` and `workflow_dispatch` events and same-repository
`pull_request` events. Fork PRs, Dependabot actors or PR authors, pushes to,
manual runs on, or same-repository PRs from `dependabot/` branches (even by a
maintainer) and every other event use only the hosted unprivileged harness, and
the cloud job is skipped.
Trusted jobs require the repository's
`codebuild-github-actions-runner-<run_id>-<run_attempt>` runner integration. The
cloud job needs that CodeBuild project to use EC2 compute, with a build timeout of
at least the job's 155 minutes. Lambda compute stops a build after 15 minutes, and
a runner that stops early skips collection and cleanup and leaves fences in place.

Every invocation in one caller run shares `LMI_SHARED_RUN_ID`. The deployment
bucket is scoped by region and stack name, so give each invocation in a run its
own `stack-name`; invocations sharing a stack would also share run evidence.

GitHub Actions concurrency serializes automation within the calling repository. The fixed
`lmi-e2e-shared-capacity-provider` group covers deployment through cleanup and
uses `cancel-in-progress: false`. Concurrency groups are scoped to a calling
repository; SDK repositories must use separately provisioned test capacity if
they run at the same time. Direct CLI runs are not serialized with Actions.

The driver also acquires persistent retirement fences on each qualified Lambda
target, twice per run. Deployment holds one while it updates the stack and
releases it once the update settles. Profile preparation (`prepare(profile)`, once
per target) then acquires a new one, verifies the deployed configuration before
admitting any case, and holds it through that target's cases. Its journal is
`<case output>/<coverage profile>/source/configuration/<first 20 hex characters of
the SHA-256 of the full qualified function ARN>-fence.json`; those 20 characters
begin the hash in the fence object's key below. Direct CLI runs can acquire
the target between the two; within a calling repository, the workflow concurrency
group serializes jobs across that gap. Each fence object is
`s3://<deployment bucket>/quarantine/<sha256 of full qualified function ARN>.json`
and records the owner `token`, `resourceId`, `runId`, `commit` and the owning run's
`evidencePrefix` (`runs/<runId>/`). The orchestration/deployment role needs `s3:GetObject`,
`s3:PutObject` and `s3:DeleteObject` on the deployment bucket's `quarantine/*`
prefix, plus `s3:ListBucket` on the bucket. Without it, S3 reports a missing fence
as `AccessDenied` rather than `NoSuchKey`, and acquisition fails. Fixture runtime
roles do not need fence access. Only the owning run clears
its fence, after it proves quiescence or that the case was never admitted. Failed
retirement, or an interruption whose outcome cleanup cannot prove from this run's
journals, leaves the fence in place. Later preparation and deployment reject the fenced
target and report its exact S3 URI, even if its service executions are terminal.

There is no recovery CLI for a retained fence. Reconcile it promptly: the
deployment bucket expires `runs/` objects after one day, function logs and durable
execution history are also kept for one day, and the workflow's diagnostics
artifact for seven days. Before manually removing any fence, confirm that the
owning controller and its workers can no longer resume, for example because its
job has ended and its runner is gone. A controller paused between cases can look
idle. Then inspect the fence's retained evidence prefix and reconcile all prior invocations, tasks, I/O, checkpoints, polls
and callbacks. Confirm that no runtime work remains and that the target
artifact/configuration is correct. If the fenced run left a `deployment-update.json`
journal, check its phase. A `PREPARED` journal never submitted an update. A
`SETTLED` journal has a resolved update. To release its remaining fences, run the
caller's `cleanup` stage against the downloaded artifact directory with the
original `LMI_SHARED_RUN_ID`; re-running the workflow uses a new run ID and cannot.
If any fence state in that journal is still `ACQUIRING`, treat the check as
inconclusive. For a `SUBMITTING` journal, the exact CloudFormation request token must
have reached `UPDATE_COMPLETE`, `UPDATE_ROLLBACK_COMPLETE` or a verified no-op;
`UPDATE_ROLLBACK_FAILED` keeps the fence. If the journal is unavailable, the
stack's latest status alone is not enough, because it can belong to an older
request. Keep the fence unless the stack events show that no update is in progress
and every update started after the fence's S3 `LastModified` time reached
`UPDATE_COMPLETE` or `UPDATE_ROLLBACK_COMPLETE`, or other independent evidence
shows that this run never submitted one. Delete the fence only with
a conditional request on the ETag you inspected, and keep it if any check is
inconclusive. A terminal Lambda execution status alone does not prove retirement. Within a run, the coordinator still
verifies case ownership, capacity, cancellation and retirement before reusing a
target.

Retirement inventories service callbacks and case controls. Callback completion
requires a terminal service-history event; controls are deleted after runtime
quiescence, with S3 absence verified and deletion receipts retained.

The Python adoption is [SDK PR #931](https://github.com/aws/aws-durable-execution-sdk-python/pull/931).
Merge the shared driver and workflow before the SDK caller pinned to them. Harness tests and
local SDK tests do not establish real LMI acceptance; retained cloud reports do.
