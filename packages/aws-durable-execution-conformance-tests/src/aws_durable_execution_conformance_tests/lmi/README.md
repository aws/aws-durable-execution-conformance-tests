# LMI lifecycle conformance

This optional suite coordinates multiple invocations against persistent Lambda
Managed Instances targets. It uses the core JSON/JUnit/console report writers
and the existing OTel span model, backend adapters and assertion engine. SDK
handlers, public plugin registration, AWS deployment templates and control-plane
adapters stay in each SDK repository.

Run it explicitly with `durable-execution-conformance lmi-lifecycle`. Ordinary
conformance, including `--suite all`, does not discover these multi-invocation
requirements or deploy LMI functions. The dedicated requirement catalog is
[`requirements/lmi-lifecycle`](requirements/lmi-lifecycle).

## Contracts and applicability

| ID | Mandatory scenario selectors | Measurement ownership |
| --- | --- | --- |
| lmi-lifecycle-1 | `native-overlap` | Cohort capacity; exclusive if all capacity is needed |
| lmi-lifecycle-2 | `normal-return`, `normal-failure` | Shared, bounded invocation-local faults |
| lmi-lifecycle-3 | `deadline-during-step` | Shared only for contained invocation-local admission checks |
| lmi-lifecycle-4 | `peer-recovery` | Exclusive original-environment capacity measurement |
| lmi-lifecycle-5 | `suspension-retry` | Shared, ownership retained through resume/retry |
| lmi-lifecycle-6 | `warm-reuse` | Identified sequential reuse; cohort ownership |
| lmi-lifecycle-7 | `otel-attribution` | Shared for invocation-local fixtures |
| lmi-lifecycle-8 | `otel-provider` | Exclusive provider/resource fault measurement |

The `lifecycle` capability profile requires IDs 1–6. `lifecycle-otel` requires all
eight, including both Invocation and Execution views. OTel IDs are genuinely not
applicable to a declared lifecycle-only run; this does not accept OTel support.
Every applicable ID and every listed scenario must be present and pass. Missing
SDK APIs or fixtures are `NOT_IMPLEMENTED`, not exclusions. Language-specific
starvation and non-cooperative fault fixtures remain diagnostic unless a
supported behavior and test budget have been agreed.

| Runtime | Positive native overlap evidence | Warm reuse |
| --- | --- | --- |
| `javascript` | Same environment, process and Node.js worker; overlapping requests | Same identified worker |
| `java` | Same environment/JVM and handler owner; distinct caller threads | Same identified JVM/worker |
| `python` | Same environment, different worker processes | Sequential requests in the same identified process |

A Python worker is not required to run simultaneous native invocations. Scheduler
capacity reservations do not prove placement. An SDK handler must use its real
public API and preserve its suspension model. Do not artificially unwind a
suspended JavaScript stack to match Java cleanup, emulate missing APIs in a
handler, or manufacture PENDING/success for an unacknowledged checkpoint.

## Run manifests and retained evidence

Python 3.14 and the core package are required. Install the OTel package for the
`lifecycle-otel` capability. From this repository, `hatch env create` installs the
workspace packages. A minimal inventory looks like this; expand `scenarios` to
cover the selected catalog before expecting acceptance:

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
      "slots": 2,
      "evidence_file": "evidence/overlap.json"
    }
  ]
}
```

Validate captured evidence without contacting AWS:

```bash
durable-execution-conformance lmi-lifecycle \
  --manifest manifest.json --output retained-report
```

An `evidence_file` is resolved relative to the manifest. Missing files, unknown
schema versions, mismatched artifact/configuration/case identities and absent
observations fail acceptance. To record an intentional SDK gap, replace
`evidence_file` with `"not_implemented": "missing public deadline admission API"`.
This remains blocking. A retry stays in the original evidence attempt list;
never replace the original case with a successful retry.

Use a fresh output directory for every run. The output contains the input manifest, complete case evidence/attempts/timings,
JSON report and JUnit report. Live runs also flush `attempts.jsonl` before every
transport launch and append finalized cases to `results.jsonl`, preserving audits
if the runner is interrupted. Later result records for a case include profile
finalization; retain the full journal. Report entries aggregate all cases for a requirement:
one successful case cannot hide a failed or missing sibling. `FAILED` preserves
SDK assertion, placement, infrastructure and collection errors in its details;
raw case status distinguishes `INFRASTRUCTURE_ERROR` and `INCOMPLETE_EVIDENCE`.
`NOT_IMPLEMENTED` and `UNCOVERED` are blocking under this command's `strict`
policy. Ordinary conformance retains its existing nonblocking gap policy.

## SDK adapter contract

To run a deployment and fixtures, supply a trusted local `module:factory`:

```bash
durable-execution-conformance lmi-lifecycle --manifest manifest.json \
  --adapter sdk_lmi_adapter:create_adapter --output lmi-output
```

`create_adapter(manifest)` returns the async `ScenarioAdapter` defined in
[`coordinator.py`](coordinator.py). The same dispatcher sends `scenario`,
`otelView` when selected, `runId`, `caseId`, `cohortId` and `role`. Route through
real public handler/plugin registration. Choose views and fault fixtures through
invocation-local configuration or immutable handler variants. Never mutate
environment variables, shared clients or process-global plugin settings to
switch concurrent scenarios. Keep names and routing deterministic across replay;
checkpoint nondeterministic control reads in durable operations.

The catalog supplies default cohort roles, slot reservations and mandatory exclusive
measurement windows. Manifests cannot weaken them or substitute a success fixture
for the required failure fixture. Both disposal variants are required.

The adapter methods have distinct responsibilities:

1. `prepare(profile)` acquires persistent cross-run target ownership, reconciles
   residual work and prepares the exact artifact/configuration once. It must
   refuse deployment while another owner has resumable work.
2. `start(context)` journals every launch with
   `record_attempt(attempt_id, role=role)` before dispatch, then updates it with
   the actual request identity. The journal derives immutable retry ordinals
   per role and binds each selected role to its first launched request. `place(context)` verifies
   runtime identity and overlap, coordinating only this cohort's bounded barriers.
   All attempts share the placement budget.
3. `execute(context)` runs the fixture. `collect(context)` returns version 1
   observation evidence, with independent stream completion watermarks. Assertions
   run before fault release.
4. `release(context)` releases only this case's controls. `retire(context)` stops
   or settles this case's executions, retries, callbacks and late work, updates
   `context.evidence["retirement"]` and returns literal `True` only after positive
   quiescence evidence. Assertions run again before the lease is released.
5. `finish(profile, quiescent=...)` releases cross-run ownership only if quiescence
   is established. Otherwise preserve external quarantine for explicit recovery.

Every method must be cancellation-cooperative and use bounded service I/O. The
coordinator does not attempt to kill arbitrary threads or user I/O. Missing
placement, incomplete collection and adapter cancellation cannot pass.
Observation evidence is frozen before cleanup; finalization may add retirement
records but cannot rewrite failed recovery or timing assertions. Evidence schema
and validation details live beside [`evidence.py`](evidence.py).

## Deployment, scheduling and ownership

Use two persistent functions per SDK/account with native environment concurrency
1 and 2. Sending one request to a concurrency-2 function is not a concurrency-1
deployment. Reuse each unchanged profile across all compatible scenarios. A third
profile needs `incompatibilityReason` explaining the actual runtime, architecture,
layers, IAM, timeout or configuration difference. Do not deploy once per scenario,
operation, view or case. Pin SDK, plugin, conformance, layer and handler artifacts;
retain the resolved deployment configuration with the evidence URI/digest.

`capacity` is the account/profile's configured active cohort reservation budget;
`scenarioConcurrency` bounds logical cases and can exceed native concurrency.
Suspended cases can release reserved active capacity through
`context.release_capacity()` after observing that their requests are idle. They
retain resource ownership and must reserve capacity before controlling another
launch. Service-initiated resumes still require conservative capacity budgeting;
this scheduling helper cannot control Lambda placement.

Shared cases use independent control/evidence namespaces. An exclusive waiter
stops new shared admission on its target and waits for all existing owners to
retire. PENDING, wrapper return or a completed primary result is insufficient.
Other profiles and SDK accounts continue. There is no cross-SDK lock. Capacity,
provider stalls, process-wide resource baselines and global fault controls require
exclusive windows; ordinary steps, waits, callbacks and invocation-local OTel
fixtures can overlap.

The in-process scheduler is not a distributed lock. SDK adapters must implement
external target ownership in `prepare`/`finish`, including reconciliation after a
runner crash. A CI workflow concurrency group may serialize full runs using the
same account/targets, but it must also account for residual executions after
cancellation. Do not release persistent ownership merely because the CI job
ended. Failed retirement quarantines the affected profile. Explicit external
recovery must prove residual work is settled and the artifact is correct before
clearing quarantine. Unaffected targets remain usable.

Never clear a shared bucket, remove another case's evidence, stop another case's
execution or reset a shared provider. All controls, callbacks, logs, traces,
requests and service histories must carry run/case/cohort ownership. Preserve
failed evidence after emergency release.

## OTel and supported limits

The OTel extension's [LMI guidance](../../../../aws-durable-execution-conformance-tests-otel/README.md)
describes canonical span evidence and cohort collection. Capture Java's runtime
carrier before worker dispatch, JavaScript's invocation-context carrier and
Python's process-local environment carrier. Input-injected trace headers do not
prove runtime propagation. Check independent request provenance before filtering
spans by execution, then reuse ordinary view assertions. Collect both views,
explicit sampling, warm reuse, late activity and controlled exporter outcomes.

No fixed five/eight-second recovery target is a product SLA. Each run records its
agreed harness budgets and supported cleanup/recovery policy. Arbitrary
non-cooperative I/O need not terminate. Already accepted side effects may finish;
retries remain at least once unless the SDK's supported policy says otherwise.
Timeouts and export errors can lose telemetry, so do not require unconditional
export or describe export/execution as exactly once. Expected unsampled output,
exporter failure and missing collection are different observations.

## Local checks, cloud prerequisites and adoption

```bash
hatch run test:all
hatch run types:check
hatch fmt --check packages scripts
hatch run yaml:lint
hatch run dist:build
hatch run dist:check
```

Local tests use controlled asyncio adapters and retained evidence for distinct
Node.js/Java/Python models. They cover positive controls plus retry/worker
substitution, missing observations, stale attribution, cleanup-driven false
passes, shared overlap, exclusive fairness, suspension ownership and quarantine.
They are not deployed LMI acceptance. Compare serial (`scenarioConcurrency: 1`)
and parallel runs of the same cases/assertions using retained timing records;
verify actual safe-case overlap and independent-profile progress without a fixed
speedup threshold.

Real runs need SDK-owned public-API handlers, LMI deployment templates, capacity
and IAM in each SDK's isolated AWS test account, a case-owned control/evidence
store, runtime identity instrumentation, genuine X-Ray propagation and the
selected OTel backend. Start with one architecture/runtime configuration and the
two native-concurrency profiles per SDK. Keep LMI execution opt-in and separately
scheduled; run accounts and compatible cases concurrently.

| SDK | Adoption tracker | Existing work and remaining evidence |
| --- | --- | --- |
| JavaScript | [#1017](https://github.com/aws/aws-durable-execution-sdk-js/issues/1017) | [#928](https://github.com/aws/aws-durable-execution-sdk-js/pull/928) closed without merging; map final fixtures to IDs 1–8 |
| Java | [#727](https://github.com/aws/aws-durable-execution-sdk-java/issues/727) | Base harness incorporated by [#782](https://github.com/aws/aws-durable-execution-sdk-java/pull/782); adapt its driver/evidence and validate IDs 1–8 |
| Python | [#742](https://github.com/aws/aws-durable-execution-sdk-python/issues/742) | [#743](https://github.com/aws/aws-durable-execution-sdk-python/pull/743) is proposed; adapt process-specific fixtures and validate IDs 1–8 |

These are migration mappings, not accepted results for any requirement. At least
two distinct real runtime models with retained evidence and regression-sensitivity
controls remain a release/adoption gate tracked by
[#161](https://github.com/aws/aws-durable-execution-conformance-tests/issues/161)
and [Java #798](https://github.com/aws/aws-durable-execution-sdk-java/issues/798).
Review the initial contracts and supported policies with SDK owners; land adapter,
handler/template and workflow adoption through coordinated SDK PRs. Existing
language-specific regressions remain useful while common coordination and
validation move here.
