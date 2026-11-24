# LMI lifecycle conformance

This optional suite coordinates multiple invocations against persistent Lambda
Managed Instances targets. It uses the core JSON/JUnit/console report writers
for seven language-neutral lifecycle requirements. The shared package supplies
AWS transport, persistent deployment, scenario choreography and evidence
normalization. Each SDK supplies its public-API handlers, observation hooks,
artifact build and a data-only fixture binding. See [SDK reuse](REUSE.md) for
these APIs and the reusable GitHub Actions workflow.

Run it explicitly with `durable-execution-conformance lmi-lifecycle`. Ordinary
conformance, including `--suite all`, does not discover these multi-invocation
requirements or manage their persistent LMI deployment; it deploys only the
caller's own SAM template. The dedicated requirement catalog is
[`requirements/lmi-lifecycle`](requirements/lmi-lifecycle).

## Contracts and applicability

| ID | Scenario | Measurement ownership |
| --- | --- | --- |
| lmi-lifecycle-1 | `native-overlap` | Shared two-slot cohort reservation |
| lmi-lifecycle-2 | `normal-return` | Shared, bounded invocation-local faults |
| lmi-lifecycle-3 | `deadline-during-step` | Shared only for contained invocation-local admission checks |
| lmi-lifecycle-4 | `peer-recovery` | Exclusive original-environment capacity measurement |
| lmi-lifecycle-5 | `suspension-retry` | Shared, ownership retained through resume/retry |
| lmi-lifecycle-6 | `warm-reuse` | Identified sequential reuse; cohort ownership |
| lmi-lifecycle-7 | `normal-failure` | Shared, bounded invocation-local faults |

The `standard` coverage profile supports the `lifecycle` capability and requires
IDs 1–7. Each requirement file declares exactly one `scenario` string, yielding
seven independent test cases. Normal return is requirement 2 and normal failure
is requirement 7. OTel and plugin conformance are deferred to a later change.
Every ID and every listed scenario must be present and pass for every supported
runtime. Requirement files contain only the ID, description, contract, limits,
a scenario selector, roles, scheduling, minimum native concurrency and any required
behavior budget names. Mandatory coverage and runtime applicability are
suite-wide rules rather than repeated
`mandatory`, `capability`, `runtimes` or `coverageProfiles` fields. Missing SDK APIs
or fixtures are `NOT_IMPLEMENTED`, not exclusions. Language-specific
starvation and non-cooperative fault fixtures remain diagnostic unless a
supported behavior and test budget have been agreed.

| Runtime | Positive native overlap evidence | Warm reuse |
| --- | --- | --- |
| `javascript` | Same environment, process and Node.js worker; overlapping requests | Same identified worker |
| `java` | Same environment/JVM process; distinct overlapping requests | Same identified JVM/worker |
| `python` | Same environment, different worker processes | Sequential requests in the same identified process |

A Python worker is not required to run simultaneous native invocations. Scheduler
capacity reservations do not prove placement. An SDK handler must use its real
public API and preserve its suspension model. Do not artificially unwind a
suspended JavaScript stack to match Java cleanup, emulate missing APIs in a
handler, or manufacture PENDING/success for an unacknowledged checkpoint.

## Run manifests and retained evidence

Python 3.14 and the core package are required. From this repository,
`hatch env create` installs the workspace packages into Hatch's environment; run
the `durable-execution-conformance` commands below through `hatch run` or from
`hatch shell`. A minimal plan inventory looks like this; expand `scenarios` to
cover the selected catalog before expecting acceptance. `load_manifest` accepts it,
but `--collect` also needs the `sdk` block that `runner.make_manifest` adds; see
[Collection](#collection):

```json
{
  "schemaVersion": 1,
  "runId": "sdk-commit-ci-run",
  "runtime": "python",
  "capabilityProfile": "lifecycle",
  "region": "us-west-2",
  "scenarioConcurrency": 8,
  "profiles": [
    {
      "profile_id": "native-1",
      "resource_id": "arn:aws:lambda:us-west-2:123456789012:function:lmi-native-1:live",
      "capacity": 1,
      "native_concurrency": 1,
      "artifact_id": "exact-sdk-commit-or-digest",
      "configuration_id": "digest-of-deployed-configuration-1"
    },
    {
      "profile_id": "native-2",
      "resource_id": "arn:aws:lambda:us-west-2:123456789012:function:lmi-native-2:live",
      "capacity": 2,
      "native_concurrency": 2,
      "artifact_id": "exact-sdk-commit-or-digest",
      "configuration_id": "digest-of-deployed-configuration-2"
    }
  ],
  "budgets": {
    "provisioning": 300,
    "lease": 300,
    "placement": 120,
    "execution": 300,
    "collection": 120,
    "release": 30,
    "retirement": 120
  },
  "scenarios": [
    {
      "case_id": "overlap",
      "requirement_id": "lmi-lifecycle-1",
      "profile_id": "native-2",
      "scenario": "native-overlap",
      "roles": ["victim", "peer"],
      "slots": 2
    }
  ]
}
```

Re-adjudicate a collection run's saved bundle without loading SDK code or
contacting AWS. `lmi-output` is the collection run's output directory, as in the
[collection](#collection) example below. The shared runner writes one such directory per coverage
profile, such as `standard/`.

```bash
durable-execution-conformance lmi-lifecycle \
  --manifest lmi-output/manifest.json \
  --collected-evidence lmi-output/collected-evidence.json --output lmi-adjudication
```

Adjudication trusts an unmodified `collected-evidence.json`: re-adjudication is a
debugging tool, not verification. See [Adjudication](evidence-schema.md#adjudication).

Unknown schema versions, mismatched artifact/configuration/case identities and
absent observations fail acceptance. To record an intentional SDK gap, declare
`"not_implemented": "missing public deadline admission API"` on the case.
This remains blocking. Cases whose requirement declares `behaviorBudgets` must
also set every declared budget in the manifest, using the finite positive
millisecond limits agreed for that fixture (these example values are not product
SLAs). `deadline-during-step` cases must set every budget in, for example, `"behavior_budgets": {"admissionMs": 5000, "cleanupMs": 5000, "observationGraceMs": 1000}`,
and `peer-recovery` cases must set every budget in, for example, `"behavior_budgets": {"admissionMs": 5000, "cleanupMs": 5000, "observationGraceMs": 1000, "recoveryMs": 15000}`.
These are the shared runner's values. The matching evidence `budgets` must retain
exactly those values. Declare the limits before launching work; changes to the
manifest copy, scenario or evidence during collection cannot replace the admitted
limits. The run-level `budgets` above separately limits controller phases in
seconds.

A retry stays in the original evidence attempt list;
never replace the original case with a successful retry.

Use a fresh output directory for every run. The output contains the input manifest, complete case evidence/attempts/timings,
JSON report and JUnit report. Collection runs also flush `attempts.jsonl` before
every transport launch and append case rows to `results.jsonl`, preserving audits
if the runner is interrupted. A case can have several rows; read the last one. A
case whose collection succeeded is published as `COLLECTED`, which awaits
adjudication. After every target is released or quarantined, the runner writes the
frozen observations and cleanup evidence to `collected-evidence.json` and
publishes each adjudicated verdict. Retain the full journal and the bundle. See
[Collection](evidence-schema.md#collection) for the journal details. Report entries aggregate all cases for a requirement:
one successful case cannot hide a failed or missing sibling. `FAILED` preserves
SDK assertion, placement, infrastructure and collection errors in its details;
raw case status distinguishes `INFRASTRUCTURE_ERROR` and `INCOMPLETE_EVIDENCE`.
`NOT_IMPLEMENTED` and `UNCOVERED` are blocking under this command's `strict`
policy. Ordinary conformance keeps gaps nonblocking by default.

## Collection

The built-in shared AWS driver is the only collector for catalog runs. It loads no
SDK code:

```bash
durable-execution-conformance lmi-lifecycle --manifest manifest.json \
  --collect --output lmi-output
```

A collection plan is built from the manifest that `DeploymentManager.deploy()`
wrote, with `runner.make_manifest(deployment, coverage, directory, artifact_id=...,
conformance_revision=...)`. The caller's `run` stage normally uses
`runner.run(deployment, directory, manifest_factory=...)`. It calls
`manifest_factory(deployment, coverage, coverage_directory)` once per coverage
profile, with `make_manifest`'s positional arguments, so
`functools.partial(make_manifest, artifact_id=..., conformance_revision=...)` works.
The factory only returns the plan and must pass the given coverage directory
through unchanged; `runner.run` collects and reports each returned plan with the
same driver. `--collect` requires `sdk.deployment` with a valid runtime and data-only
`fixtureBinding`, a `runtimeModel` matching the plan's `runtime` and a
`conformanceRevision`, plus `sdk.artifacts_dir`, and rejects a plan that lacks any
of them before it claims the output directory. A direct `collect_and_adjudicate`
call checks the same before claiming its audit directory and also needs
`sdk.artifacts_dir` to be absolute; `--collect` resolves a relative value from the
current directory. `runner.run` differs: it creates each coverage directory
before it calls the factory and checks the plan, so a plan or binding error leaves
that directory behind and a retry needs a fresh run directory. With `make_manifest`,
binding and revision errors are raised inside the factory, before any
`manifest.json` is saved; other plan errors can surface after it is saved.
`runner.run` fills a missing `sdk.artifacts_dir` with `<coverage directory>/source`
and, like `--collect`, resolves a relative one from the current directory, which
can put the journals outside the uploaded artifact directory. Supply the checked-out conformance commit SHA as the revision
(`CONFORMANCE_REVISION` in the reusable workflow); the check rejects only a blank or
`unverified` value, so a wrong SHA would mislabel the evidence. See
[`FixtureBinding`](REUSE.md#fixturebinding) for the binding and the payload each
fixture receives, and [Collection](evidence-schema.md#collection) for the journals
and teardown.

Fixture handlers route through the real public handler API. Choose fault fixtures
through invocation-local configuration or immutable handler variants. Never mutate
environment variables, shared clients or process-global settings to switch
concurrent scenarios. Keep names and routing deterministic across replay;
checkpoint nondeterministic control reads in durable operations. Fixture output
stays untrusted evidence.

The catalog supplies default cohort roles, slot reservations and mandatory exclusive
measurement windows. Manifests cannot weaken them or substitute a success fixture
for the required failure fixture. Both disposal variants are required.

Missing placement, incomplete collection and cancelled collection cannot pass.
The coordinator freezes the driver's observations before fault release;
finalization may add retirement records but cannot rewrite failed recovery or
timing assertions, and adjudication evaluates behavior later from both. The evidence schema
and validation rules are in [`evidence-schema.md`](evidence-schema.md).

## Deployment, scheduling and ownership

Use two persistent functions per SDK/account with native environment concurrency
1 and 2. Sending one request to a concurrency-2 function is not a concurrency-1
deployment. Reuse each unchanged profile across all compatible scenarios. The
built-in collector runs cases only on those two deployed targets (`c1` and `c2`). A
third profile, for an SDK-owned collector, needs `incompatibilityReason` explaining
the actual runtime, architecture, layers, IAM, timeout or configuration
difference. Do not deploy once per scenario,
operation or case. Pin SDK, conformance, layer and handler artifacts;
retain the resolved deployment configuration with the evidence URI/digest.

`capacity` is the account/profile's configured active cohort reservation budget;
`scenarioConcurrency` bounds logical cases and can exceed native concurrency.
The shared driver keeps each catalog case's reservation through retirement,
including while it is suspended. An SDK-owned collector can release reserved
active capacity through `context.release_capacity()` after observing that its
requests are idle; it retains resource ownership and must reserve capacity before
controlling another launch. Service-initiated resumes still require conservative capacity budgeting;
this scheduling helper cannot control Lambda placement.

Shared cases use independent control/evidence namespaces. An exclusive waiter
stops new shared admission on its target and waits for all existing owners to
retire. PENDING, wrapper return or a completed primary result is insufficient.
Other profiles and SDK accounts continue. There is no cross-SDK lock. Capacity,
process-wide resource baselines and global fault controls require exclusive
windows; ordinary steps, waits, callbacks and invocation-local faults can overlap.

The in-process scheduler is not a distributed lock. The shared driver holds
external target ownership with a persistent per-target S3 retirement fence during
deployment updates, and acquires a new one before case admission. The owning run
clears each fence only after proving quiescence, or that the case was never
admitted; later runs refuse target preparation or deployment while a fence remains.
Failed retirement quarantines the affected profile. A case-phase driver operation
that a phase timeout or cancellation interrupts while still in flight keeps every
target prepared in that run fenced, even one whose cases passed. A case that only
timed out waiting for its lease does not, and an interrupted preparation or
`finish` fences only its own target. That operation's worker thread can still
complete after the interruption: an interrupted `finish` may complete its conditional S3 delete, and an
interrupted preparation may record `NOT_ADMITTED` and delete the fence. Inspect that
target's journal and S3 fence object to see whether a fence remains. Otherwise unaffected targets remain usable.

After a crash the fences stay. The caller's `cleanup` stage (`runner.cleanup`)
finds the run's case output through the pointer `runner.run` records; after a
direct `--collect` run, pass that run's output directory instead, for example
`runner.cleanup(deployment, directory=Path("lmi-output"))`. Cleanup reconciles an
interrupted deployment update and releases profile fences whose
journals record `NOT_ADMITTED` or `RETIRED`, or an `ACQUIRING` single-attempt
acquisition with its original owner lock and a matching S3 fence. Any other fence,
including a retried acquisition or one held by a live admission, needs explicit
recovery that proves residual work is settled and the artifact is correct; see the
[SDK reuse guide](REUSE.md#reusable-github-actions-workflow) for the fence location,
permissions and evidence. A CI workflow concurrency group may serialize full runs
using the same account/targets, but it must also account for residual executions
after cancellation. Do not release persistent ownership merely because the CI job
ended.

Never clear a shared bucket, remove another case's evidence, stop another case's
execution or reset shared runtime state. All controls, callbacks, logs,
requests and service histories must carry run/case ownership. Preserve
failed evidence after emergency release.

## Supported limits

No fixed five/eight-second recovery target is a product SLA. Each run records its
agreed harness budgets and supported cleanup/recovery policy. Arbitrary
non-cooperative I/O need not terminate. Already accepted side effects may finish;
retries remain at least once unless the SDK's supported policy says otherwise.

## Local checks, cloud prerequisites and adoption

```bash
hatch run test:all
hatch run types:check
hatch fmt --check packages scripts
hatch run yaml:lint
hatch run dist:build
hatch run dist:check
```

Local tests use controlled asyncio collectors and captured evidence for distinct
Node.js/Java/Python models. They cover positive controls plus retry/worker
substitution, missing observations, stale attribution, cleanup-driven false
passes, shared overlap, exclusive fairness, suspension ownership and quarantine.
They are not deployed LMI acceptance. Compare serial (`scenarioConcurrency: 1`)
and parallel runs of the same cases/assertions using retained timing records;
verify actual safe-case overlap and independent-profile progress without a fixed
speedup threshold.

Real runs need the SDK's public-API fixture bundle, fixture binding and stage
script, plus capacity and IAM in each SDK's isolated AWS test account;
`DeploymentManager` generates and submits the function stack, including the
stack's private control and evidence bucket, in which each case uses its own
namespace. The fixture wrapper publishes the runtime
identity observations. Start with one architecture/runtime
configuration and the two native-concurrency profiles per SDK. Keep LMI execution
opt-in and separately
scheduled; run accounts and compatible cases concurrently.

| SDK | Adoption tracker | Existing work and remaining evidence |
| --- | --- | --- |
| JavaScript | [#1017](https://github.com/aws/aws-durable-execution-sdk-js/issues/1017) | [#928](https://github.com/aws/aws-durable-execution-sdk-js/pull/928) closed without merging; map final fixtures to IDs 1–7 |
| Java | [#727](https://github.com/aws/aws-durable-execution-sdk-java/issues/727) | Base harness incorporated by [#782](https://github.com/aws/aws-durable-execution-sdk-java/pull/782); map its fixtures to a `FixtureBinding` and validate IDs 1–7 |
| Python | [#742](https://github.com/aws/aws-durable-execution-sdk-python/issues/742) | [#931](https://github.com/aws/aws-durable-execution-sdk-python/pull/931) adopts this suite through the reusable workflow; validate IDs 1–7 with retained cloud reports |

These are migration mappings, not accepted results for any requirement. At least
two distinct real runtime models with retained evidence and regression-sensitivity
controls remain a release/adoption gate tracked by
[#161](https://github.com/aws/aws-durable-execution-conformance-tests/issues/161)
and [Java #798](https://github.com/aws/aws-durable-execution-sdk-java/issues/798).
Review the initial contracts and supported policies with SDK owners; land binding,
handler and workflow adoption through coordinated SDK PRs. Existing
language-specific regressions remain useful while common coordination and
validation move here.

## Failed-case observations and retirement

A phase failure remains a failed result even when cleanup succeeds. When an
earlier phase failed without being interrupted, after the case started work, the
shared driver attempts one bounded, read-only snapshot before fault release. The
snapshot reads the available observations without waiting for runtime work to
become quiescent and uses the collection budget. A case that never started work,
such as one with a missing fixture, has nothing to snapshot. The coordinator skips
the snapshot when the case was cancelled or a case-phase operation was interrupted
by its phase timeout; such a case keeps only the observations it had already
recorded. A driver error inside a phase, such as a result poll reaching its own
deadline, still gets the snapshot.

For a failed or incomplete case that owned work, the driver's retirement
separately records a post-cleanup snapshot; if that read fails, only partial
diagnostics remain. The coordinator retains it as
`ScenarioResult.retirement_evidence`; it never replaces the original behavior
evidence or removes its errors. A successful retirement step alone is insufficient
to release a failed case's target.

The independent retirement audit requires the final snapshot to show terminal
service execution histories, retained resource inventories, completed
task/request intervals, source coverage, and actual invocation exit
observations, and to settle the native owner of every SDK record seen in any
snapshot. A journaled launch without an identified invocation request (unless
the driver recorded a definite Invoke rejection for it), missing completion
pairs, outstanding work, malformed evidence or interrupted controller work
retain quarantine. A case that held controls but never dispatched an Invoke is
retired by the separate controls-only audit instead, which requires empty runtime
histories; see the
[independent physical retirement audit](evidence-schema.md#independent-physical-retirement-audit).

The one exception is an invocation the service proves it rejected before any
handler ran (`UNDISPATCHED`): an allowlisted `InvocationCompletedDetails.Error` on
an owned execution, with no ledger row of any phase and no launch binding. It needs
no seal or exit observation and can settle physical retirement, but behavior
validation still reports it INCOMPLETE. The exception covers only that request:
every accepted launch still needs a bound native request, so a launch whose only
request was rejected keeps its target fenced. The exception depends on the fixture
wrapper durably publishing `WRAPPER_ENTER` before any SDK or handler code runs; see
the [observation protocol](REUSE.md#observation-protocol) and the
[evidence contract](evidence-schema.md).

A role that was required by the scenario but never actually launched still makes
the scenario incomplete. It does not represent an invented outstanding request:
retirement can succeed when every actual launch is independently proven settled.
This distinction lets later cases use a safely retired target while preserving
all earlier test failures. A failed case that owned work but has no independent
retirement evidence keeps its target conservatively quarantined. A case does not
when the driver established, without an interruption, that it never started work,
such as one with a missing fixture; an empty attempt journal alone, for example
after a placement timeout, is not enough.

## SDK-owned suites

The shared catalog contains only `standard`; all packaged requirements apply to
JavaScript, Java and Python. Language-specific operation,
cleanup, thread and exception assertions belong in the SDK repository.
They must not be added as runtime-exclusive requirement files here.

An SDK-owned controller can pass a `SuiteDefinition` to the programmatic
`load_manifest`, `collect_and_adjudicate`, `adjudicate_collected_evidence`,
`coverage_inventory` and `build_report` functions. The definition supplies its own requirement metadata,
case matrix, validator and report suite name. The common engine retains bounded
scheduling, launch journals, immutable manifest snapshots, evidence identity,
strict coverage and independent retirement auditing. An optional evidence policy
key binds the caller's declared behavior budgets; the caller explicitly declares
any budget names that permit zero. SDK-specific applicability is checked by that
SDK's loader and validator.

`collect_and_adjudicate` takes `suite` and `adapter_factory` together: both or
neither. Passing only one raises `TypeError` before anything is claimed. With
neither, the built-in shared driver collects the packaged catalog. An SDK-owned
collector builds on internal, unversioned interfaces, so pin the conformance
commit; the shared-plan pre-claim checks do not run on this path. See
[Collection](evidence-schema.md#collection).

Definitions are not loaded from evidence and are not passed to collectors. The
shared CLI continues to recognize only its packaged catalog. A local suite uses
its own requirement/report namespace and cannot fill missing shared conformance
coverage. Selecting a local SDK suite does not establish standard-profile
acceptance.
