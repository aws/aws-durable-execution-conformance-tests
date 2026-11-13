# Export lifecycle modes

The `export-lifecycle` coverage profile tests exporter behavior independently of
SDK language. Select `runtime: javascript`, `java`, or `python`, and
`capabilityProfile: lifecycle-otel`. Every selected runtime must cover both export
modes in both OTel views on native-concurrency 1 and 2 deployments.

The profile has 24 mandatory cells:

- Three scenarios: `export-success`, `export-failure-recovery`, `held-export`.
- Two modes: `sync`, `async-bounded-flush`.
- Two views: `invocation`, `execution`.
- Two native configurations: c1 and c2.

All cells belong to requirement `lmi-lifecycle-16`. Missing mode, view, native
configuration, or scenario remains uncovered. Repeating a synchronous case cannot
satisfy an asynchronous cell. Unsupported exporter configurations remain visible
as missing capabilities; they are not silently skipped based on language.

## Two distinct completion models

| Observation | `sync` | `async-bounded-flush` |
| --- | --- | --- |
| Export call | Completes in the synchronous span-finalization path | May remain pending after the SDK stops waiting |
| SDK return | Follows actual export completion | Follows bounded flush waiting; a wait timeout is not export completion |
| Held-export fixture | Gate release and export completion precede wrapper return | Flush wait times out and wrapper returns while export is still held; release and actual completion occur later |
| Resource retirement | Requires retained completion evidence | Requires later export and underlying flush-work completion, even after wrapper return |

The Java and Python SDK fixtures demonstrate these two configurations; neither
configuration is intrinsically tied to its language. A synchronous custom
exporter can block before a later flush timeout is reached. The synchronous test
uses a controlled release and a declared observation budget; it does not promise
that the SDK can forcibly interrupt arbitrary blocking exporter code.

For asynchronous export, distinguish the actual flush request, completion of the
SDK's bounded wait, and completion of the underlying export/flush work. A
`timeout` result ends the wait only. It cannot be relabeled as export success or
used to release a case's resource ownership. The controlled held-export scenario
must retain positive evidence that the export was still pending when the wrapper
returned, then release only its own gate and observe completion before retirement.

The failure scenario records a real failed export without corrupting the
handler's result, then observes successful export from a healthy invocation using
the original provider/process. Trace attribution, original request/execution
identity, and provider ownership apply to both modes. These assertions do not
promise unconditional telemetry delivery after an actual platform timeout or
exactly-once transport delivery.

## Selecting and expanding the matrix

The manifest uses an explicit run profile and a per-case mode:

```json
{
  "schemaVersion": 1,
  "runtime": "java",
  "coverageProfile": "export-lifecycle",
  "capabilityProfile": "lifecycle-otel",
  "runId": "artifact-commit-ci-run"
}
```

Add the persistent c1/c2 `profiles` inventory described in [README.md](README.md).
Use the matrix API to generate scenario entries:

```python
from aws_durable_execution_conformance_tests.lmi.matrix import required_cases

for case in required_cases(runtime, "lifecycle-otel", "export-lifecycle"):
    entry = {
        "case_id": f"{case.scenario}-c{case.native_concurrency}-{case.otel_view}-{case.export_mode}",
        "requirement_id": case.requirement_id,
        "profile_id": profiles_by_native_concurrency[case.native_concurrency],
        "scenario": case.scenario,
        "otel_view": case.otel_view,
        "export_mode": case.export_mode,
        "behavior_budgets": {name: approved_budgets[name] for name in case.budget_names},
    }
```

Both modes declare positive `observationGraceMs` and `exportCompletionMs`.
`async-bounded-flush` also declares `flushWaitMs`. These are explicit fixture
settings agreed before execution, not cross-SDK product SLAs. The runner compares
collected `exportLifecycle.budgets` against an independent snapshot of the
manifest; adapters cannot relax a budget or switch modes after seeing results.

The dispatcher passes `exportMode`, `otelView`, `coverageProfile`, and
`behaviorBudgets` along with case/cohort ownership. SDK handlers select
invocation-local configuration or immutable, pre-registered provider variants.
Do not mutate a shared processor, provider, environment variable or client to
switch modes while other invocations use it. Reuse the two persistent functions
when their unchanged artifacts can host both modes. Add deployment profiles only
when an incompatible SDK deployment setting requires them, and record the reason.

Run the existing command with the SDK-owned adapter or retained evidence files:

```bash
durable-execution-conformance lmi-lifecycle --manifest manifest.json \
  --adapter sdk_lmi_adapter:create_adapter --output fresh-export-results
```

The [export evidence schema](../../../../aws-durable-execution-conformance-tests-otel/src/aws_durable_execution_conformance_tests_otel/lmi-export-schema.md)
defines export acceptance, completion, flush waiting and final settlement records.

`coverage.json` and strict JSON/JUnit reports retain the mode dimension. The
coordinator holds ownership through export/flush retirement, rather than granting
an exclusive measurement or deployment lease at wrapper return. An incomplete
export or uncertain retirement quarantines only the affected target. Other
profiles can continue. The standalone profile does not claim native overlap just
because it exercises a native-concurrency-2 deployment.

## Shared and SDK-owned checks

Durable checkpoint/poll dispatch remains in `sdkRequests`. Export I/O uses its
own exporter events and owned resources; asynchronous export after wrapper return
is not a new durable operation. Its attribution remains with the original request.

The common suite validates observable request/execution attribution, declared
runtime placement, export/flush timing, provider ownership and retirement.
Java-specific handler-owner identities, worker-thread internals, `HandlerScoped`
openers, same-thread End and MDC tests remain in the Java SDK repository. Optional
owner/thread diagnostics are retained as evidence but do not affect common
acceptance. No Java-specific regression profile is introduced here.

The `standard` profile remains separate. SDK-specific regression suites live in
their SDK repositories and do not add requirements to this shared catalog. The
complete ordinary SDK/OTel catalogs still use their existing runner path. SDK
adapters/handlers and real-LMI validation remain necessary; local positive
controls validate the harness, not a deployed SDK implementation.
