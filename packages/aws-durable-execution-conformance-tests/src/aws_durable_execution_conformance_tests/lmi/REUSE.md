# Reusing the LMI lifecycle harness

The core package contains the driver for all seven lifecycle requirements. It
runs on Python 3.14 or later, including when the SDK under test is Java or
JavaScript. It imports boto3 and the conformance package; it does not import any
language SDK or SDK testing package. The Lambda bundle contains the SDK fixture,
not this driver.

## SDK-owned bindings

`SharedAdapter`, `ScenarioDriver`, `build_evidence`, `configuration_identity` and
`ScenarioAdapter` are internal to this driver; they change freely without SDK edits.
An SDK's only contract with the shared collector is data, not code:

| Shared API | SDK supplies |
| --- | --- |
| `runtime.RuntimeDefinition` | Runtime model (`python`, `java`, `javascript`), handler, architecture, memory, `binding` |
| `runtime.FixtureBinding` | The data-only mapping of its deployed fixture onto the shared catalog (below) |
| `deployment.DeploymentManager` | Runtime definition, stack owner tag, artifact directory, ZIP path, bucket prefix |
| `runner.make_manifest` | Exact SDK artifact identity and conformance revision |
| `runner.run`, `runner.cleanup`, `summary.summarize` | Deployment manifest, manifest factory, or run artifacts |

Construct a deployment manager with explicit SDK choices, for example:

```python
from pathlib import Path
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.runtime import FixtureBinding, RuntimeDefinition

runtime = RuntimeDefinition(
    "javascript",
    "index.handler",
    architecture="arm64",
    binding=FixtureBinding.load(Path(__file__).parent / "binding.json"),
)
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
It records the runtime (including the binding) into the deployment manifest
under `fixtureBinding`, next to `runtimeModel`, `handler`, `architecture` and
`memorySize`; `make_manifest` copies that recorded deployment into
`plan["sdk"]["deployment"]` and includes the binding in each profile's
`configuration_id`.

### `FixtureBinding`

A checked-in JSON file (`FixtureBinding.load(path)`) data-only binding maps the
SDK's deployed fixture onto the shared catalog. Parsing fails closed: unknown
fields or values are rejected. The Python SDK's binding, for example:

```json
{
  "bindingVersion": 1,
  "fixtures": {
    "native-overlap": "barrier",
    "normal-return": "success",
    "deadline-during-step": "deadline",
    "peer-recovery": "deadline",
    "suspension-retry": "replay",
    "warm-reuse": "warm-owner",
    "normal-failure": "failure",
    "peer": "barrier",
    "recovery": "barrier",
    "warm": "warm-probe"
  },
  "failureErrorType": "ValueError",
  "workerIdentity": "process",
  "suspension": "callback",
  "callbackPayload": "marker",
  "recoveryPolicy": "reuse-worker",
  "replayEffects": [
    { "operation": "success", "count": 1 },
    { "operation": "failure", "count": 1 },
    { "operation": "retry", "count": 2 },
    { "operation": "item", "count": 1, "values": [1, 2, 3] },
    { "operation": "left", "count": 1 },
    { "operation": "right", "count": 1 }
  ]
}
```

- `fixtures` maps the seven catalog scenario keys plus `peer`, `recovery`, and
  `warm` to fixture entry points; a missing key yields `NOT_IMPLEMENTED` at run
  time, as today. The shared driver passes the mapped fixture name in
  `scenario`, the catalog name in `sharedScenario`, and preserves `run`,
  `caseId`, `marker`, role, control prefix, gates, and behavior budgets.
  Fixtures return their marker on success; the normal-failure fixture throws a
  language-appropriate error named by `failureErrorType`.
- `workerIdentity` (`process` or `worker`) selects the field of the SDK's
  source record holding its actual worker identity (a Python process, JVM
  worker, or Node.js worker).
- `suspension` (`callback`, default, or `wait`) and `callbackPayload`
  (`marker`, default UTF-8 marker bytes, or `json-marker`, the marker
  JSON-encoded) describe how the suspension-retry fixture suspends and how its
  callback result is encoded.
- `recoveryPolicy` (`reuse-worker`, default, or `replace-worker`) is recorded
  on every profile and carried through to evidence.
- `replayEffects` lists expected replay side-effect counts per durable
  operation name; an entry with `values` expects one count per listed value
  (e.g. the Python SDK's `item` fixture, keyed by map item), otherwise one
  count for every durable operation using that name. Expected service outcome,
  worker identity, and replay-effect ownership are all derived from this
  binding by the shared driver; they are never SDK code.

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
after the cutoff. The first attempt for each role is never omitted, and
physical retirement still settles every deferred request. A request killed
before its seal can be written leaves the evidence incomplete and keeps the
target fenced even if Lambda reports native completion.

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
The binding's `callbackPayload` selects the callback encoding (UTF-8 marker
bytes, or the marker JSON-encoded). A fixture that suspends through a durable
timer sets the binding's `suspension` to `wait`. The driver awaits the
execution result and requires the same wait operation's service history to
span a PENDING decision and a later native invocation. Callback fixtures send
the response only after the native request that registered the outstanding
callback has returned PENDING. Record replay body effects in `CONFORMANCE`
observations with their durable operation IDs. When the fixture also
publishes raw `BODY` rows, the driver cross-checks them by native request,
operation name, and attempt. The binding's `replayEffects` sets the expected
count per operation name (one entry each for `success`, `failure`, and `retry`
by default); an entry with `values` expects one count per listed value. For
each expected `(operation, value)` key, the first durable operation ID the
driver observes owns that count; any other ID sharing the key, or a body whose
key is not expected, gets zero. Operations without a configured name expect
zero effects. The shared runner compares actual checkpoints and outcomes. SDK
hooks must observe the real public API and must not emulate an unsupported
operation.

Control records and service history remain separate from injected faults. The
case 4 driver admits the original peer halfway through the victim's timeout,
checks its remaining recovery window, and excludes expired originals or retries
from healthy-peer evidence. Retirement retains all observed user/probe tasks,
including those outside the scenario-owned task's assertion stream.

## Reusable GitHub Actions workflow

The reusable workflow is a separate change from the driver. Once
`.github/workflows/lmi-lifecycle.yml` is available, pin both `uses` and
`conformance-ref` to the same full commit SHA containing the workflow and driver. The workflow checks out the
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
`LMI_ARTIFACT_DIRECTORY`. For direct CLI use, give each run a separate artifact
directory and pass that same directory to build, deploy, run, collect, and cleanup.
Deployment archives existing output only after it holds the target fences.
Writers and cleanup use a nonblocking POSIX lock alongside each journal.
Automatic recovery of an uncertain write requires the original artifact root,
lock inode, and Linux boot identity. Keep the lock files with the journals.
The shared workflow owns setup of driver Python,
OIDC credentials and refreshes, account verification, deployment/test budgets,
always-run collection/cleanup, artifact upload and the job summary. Cloud work
runs only for same-repository PRs and manual dispatch. Fork and Dependabot PRs
use the hosted unprivileged harness. Trusted jobs require the repository's
`codebuild-github-actions-runner-<run_id>-<run_attempt>` runner integration.

GitHub Actions concurrency serializes automation within the calling repository. The fixed
`lmi-e2e-shared-capacity-provider` group covers deployment through cleanup and
uses `cancel-in-progress: false`. Concurrency groups are scoped to a calling
repository; SDK repositories must use separately provisioned test capacity if
they run at the same time. Direct CLI runs are not serialized with Actions.
The driver also acquires a persistent per-target S3 retirement fence before
case admission and clears it only after quiescence is proven. Reconcile any
remaining runtime work before clearing a retained fence. Within a run, the
coordinator verifies case ownership, capacity, cancellation and retirement
before reusing a target.
Retirement inventories service callbacks and case controls. Callback completion
requires a terminal service-history event; controls are deleted after runtime
quiescence, with S3 absence verified and deletion receipts retained.

The Python adoption is [SDK PR #931](https://github.com/aws/aws-durable-execution-sdk-python/pull/931).
Merge the shared driver and workflow before the SDK caller pinned to them. Harness tests and
local SDK tests do not establish real LMI acceptance; retained cloud reports do.
