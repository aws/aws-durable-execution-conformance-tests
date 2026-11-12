# Python lifecycle regression coverage

Select `"coverageProfile": "python-lifecycle"` with `"runtime": "python"`
to require the lifecycle and observability behaviors mapped from
[Python SDK PR #743](https://github.com/aws/aws-durable-execution-sdk-python/pull/743),
at revision `2179e011cc6a5c0d3620b11cbe8eec77ed1ff24f`.
The original `standard` coverage profile remains available for its portable
six/eight-contract suite. The new profile explicitly preserves Python's settled
scope, synchronous checkpoint, process ownership and resource measurement rules;
it does not impose Python's `finally` or executor behavior on other runtimes.

The coverage unit is **requirement + scenario + native concurrency + OTel view**.
Declaring c1 and c2 functions does not establish coverage: each required slot must
have a matching admitted case and passing evidence. Repeated c2 cases cannot
satisfy a missing c1 slot, instrumented cases cannot replace the uninstrumented
baseline, and successful exports cannot substitute for failure or unsampled
fixtures. All failed attempts and missing capability declarations remain blocking.

- `capabilityProfile: lifecycle` requires **28 slots**: 14 lifecycle variants on
  native concurrency 1 and 2.
- `capabilityProfile: lifecycle-otel` requires **76 slots**: those 28 baseline
  slots plus 12 variants in each OTel view on both native configurations.

## Lifecycle mapping

Every row runs on both c1 and c2. The three early-completion variants and the two
in-flight variants are separate slots. Every request also receives the profile's
cross-cutting checks: actual wrapper/user/task ordering, completed request
settlement, invocation-owned worker cleanup, and complete observation of late
activity. A platform-retired timed-out worker requires explicit runtime evidence
and the selected recovery policy.

| Requirement | Scenario selector | Source test in `lmi_tests/tests/e2e/test_lifecycle.py` |
| --- | --- | --- |
| 1 | `native-concurrency` | `test_actual_environment_process_concurrency` |
| 5 | `replay-callback-retry` | `test_checkpoint_replay_callback_and_retry` |
| 6 | `warm-lifecycle` | `test_warm_success_failure_and_suspension_cleanup` |
| 9 | `early-completion-parallel` | `test_early_completion_reclaims_losing_branches[parallel]` |
| 9 | `early-completion-map` | `test_early_completion_reclaims_losing_branches[map]` |
| 9 | `early-completion-nested` | `test_early_completion_reclaims_losing_branches[nested]` |
| 10 | `checkpoint-settlement` | `test_real_synchronous_checkpoint_settles_before_branch_join` |
| 4 | `deadline-recovery` | `test_invocation_deadline_isolation_and_capacity_recovery` |
| 11 | `timeout-retry` | `test_service_timeout_retry_does_not_repeat_completed_step` |
| 14 | `pending-cleanup` | `test_pending_waits_for_root_finally_and_then_replays` |
| 15 | `nested-progress` | `test_nested_single_lane_pools_progress_for_all_runtime_workers` |
| 13 | `return-inflight` | `test_root_return_or_failure_settles_inflight_work[return-inflight]` |
| 13 | `failure-inflight` | `test_root_return_or_failure_settles_inflight_work[failure-inflight]` |
| 12 | `late-operation` | `test_abandoned_child_rejects_late_durable_operation` |

Requirement IDs use the full `lmi-lifecycle-` prefix. This profile selects the
Python regression matrix; IDs 2 and 3 of the standard profile are not selected.
Related Python behavior is checked through the explicit in-flight and deadline
variants. Run the standard profile separately when acceptance against its
additional admission probes and portable contracts is also required.

## OTel mapping

These twelve variants run with **each** `otel_view` (`invocation`, `execution`)
and **each** c1/c2 target. Each case carries only its declared view in evidence;
coverage aggregation proves that both views ran. Every instrumented case validates
its lifecycle observations as well as its OTel observations.

| Requirement | Scenario selectors | Source test in `lmi_tests/tests/e2e/test_observability.py` |
| --- | --- | --- |
| 7 | `runtime-carrier-sampled`, `runtime-carrier-unsampled` | `test_native_runtime_carrier_and_sampling` |
| 7 | `otel-warm-lifecycle` | `test_warm_process_cleanup_and_replay` |
| 7 | `otel-concurrent` | `test_concurrent_worker_attribution_and_peer_end` |
| 8 | `export-error-recovery` | `test_export_failure_then_healthy_invocation` |
| 8 | `held-export` | `test_held_export_must_settle_before_successful_cleanup` |
| 4, 11, 14, 12, 13 | `deadline-recovery`, `timeout-retry`, `pending-cleanup`, `late-operation`, `return-inflight`, `failure-inflight` | `test_plugin_enabled_lifecycle_boundaries` |

The OTel validator requires the selected sampling outcome, actual context
restoration, process-owned provider/exporter reuse, bounded exports, observed
per-invocation plugin ownership, and logical Invocation/terminal Workflow span
counts and status across replay. Repeated transport delivery of the same span
identity does not imply an additional logical span. Sampled and unsampled native
carrier cases compare the real runtime carrier to the caller's actual request
carrier; injected compatibility headers do not establish native propagation.

Export failure must precede a healthy export in the original reused process.
A held-export fixture records an actual exporter start, a held gate, the bounded
observation before release, and normal case-owned release before OTel/wrapper
exit. That deliberate measurement release is part of the fixture. The
coordinator's later emergency release cannot satisfy those observations.

## Manifest and immutable budgets

Start with the persistent c1/c2 profile inventory described in [README.md](README.md)
and set these run fields:

```json
{
  "schemaVersion": 1,
  "runtime": "python",
  "coverageProfile": "python-lifecycle",
  "capabilityProfile": "lifecycle-otel"
}
```

Use the packaged matrix to construct all scenario entries. For example, the
following expands a base manifest containing `profiles`, `runId` and the fields
above. Each profile also declares `recovery_policy` as `reuse-worker` or
`replace-worker`, matching the supported runtime policy; collected
`profile.recoveryPolicy` must match it. `approved-budgets.json` contains the fixture budgets agreed for that SDK
revision and environment; they are not supplied by collected test results.

```python
import json
from pathlib import Path

from aws_durable_execution_conformance_tests.lmi.matrix import required_cases

plan = json.loads(Path("base-manifest.json").read_text())
budgets = json.loads(Path("approved-budgets.json").read_text())
profiles = {p["native_concurrency"]: p["profile_id"] for p in plan["profiles"]}
plan["scenarios"] = []
for case in required_cases("python", plan["capabilityProfile"], "python-lifecycle"):
    case_id = f"{case.scenario}-c{case.native_concurrency}-{case.otel_view or 'baseline'}"
    plan["scenarios"].append({
        "case_id": case_id,
        "requirement_id": case.requirement_id,
        "scenario": case.scenario,
        "profile_id": profiles[case.native_concurrency],
        "otel_view": case.otel_view,
        "behavior_budgets": {name: budgets[name] for name in case.budget_names},
        "evidence_file": f"evidence/{case_id}.json",
    })
Path("manifest.json").write_text(json.dumps(plan, indent=2) + "\n")
```

Timeout fixtures capture the runtime's observed remaining-time deadline, including
time already elapsed before wrapper entry. Cleanup, recovery and service-retry
bounds use that deadline. They do not restart the configured platform timeout at
the SDK wrapper boundary.

All slots require `observationGraceMs`; instrumented slots also require
`exportMs`. Applicable lifecycle fixtures add `cleanupMs`, `recoveryMs`,
`retryMs`, or `progressMs`. Warm resource measurements declare
`maxThreadGrowth`, `maxFdGrowth`, and `maxRssGrowthBytes`. Time budgets must be
finite and positive; resource-growth allowances may be zero. The runner keeps an independent validation snapshot and gives the adapter copies
of the manifest and scenario budgets. It binds
`evidence.regression.budgets` to each case's declared `behavior_budgets` and passes
them as invocation-local `behaviorBudgets` in the dispatcher payload.

The scheduler supplies the required roles, cohort reservations and exclusive
windows from the matrix. Configuring a shared window for an exclusive resource
measurement is rejected. The native concurrency key is exact; a higher-capacity
profile does not stand in for c1/c2.

Run with the existing opt-in command:

```bash
durable-execution-conformance lmi-lifecycle --manifest manifest.json \
  --adapter sdk_lmi_adapter:create_adapter --output fresh-lmi-output
```

Omit `--adapter` to validate retained `evidence_file` documents. The output adds
`coverage.json`, listing required slots and omissions from the manifest, alongside
case evidence, strict JSON/JUnit reports and the live attempt/result journals.
`coverage.json` describes inventory; acceptance still requires every case result
to pass. `NOT_IMPLEMENTED`, missing results, incomplete evidence and unsuccessful
variants cannot be replaced by another passing case.

## Evidence and remaining SDK work

The schemaVersion 1 envelope adds `coverageProfile`, `scenario`, `otelView` and
`regression` for this opt-in profile. See [lifecycle-regressions-schema.md](lifecycle-regressions-schema.md)
for actual observed events and the OTel package's regression evidence guide for
export/context records. The reader still validates common artifact/configuration
identity, case ownership, histories, collection watermarks and final retirement.

Python #743's S3 ledger is not already this schema. Its SDK adapter must preserve
all source observations, implement the new event/ownership mapping and supply
missing evidence through the real public handlers. It must journal native
service-generated resumes/retries as well as runner-initiated Invoke attempts.
For a direct Invoke, record the attempt before transport and update the observed
request ID. For a service-generated attempt, retain its actual request identity,
service provenance and timing when observed; never issue a replacement Invoke
and label it as a service retry.

The Python deployment code remains responsible for real AWS configuration
readback, qualified invocation targeting, artifact hashes, provider capacity and
external target ownership. This PR supplies test contracts, coordination,
validators and regression-sensitivity controls; it does not claim an SDK adapter
or a real-LMI acceptance run has been completed.

Keep SDK-local public-wrapper tests for already-expired entry, injected
missing/empty/malformed carriers, Python decorator/executor implementation and
transport-client isolation. Keep their lower-level harness tests too. The mapped
cloud behavior profile does not replace those source-level tests. Missing APIs or
unsupported behavior must remain visible; proposed test budgets are not product
SLAs and no hard termination or lossless post-timeout export is assumed.
