# Lambda Managed Instances lifecycle conformance

**Design reconstructed from the PR stack at the #199 head as of November 19, 2026 ([`584254d`](https://github.com/aws/aws-durable-execution-conformance-tests/commit/584254d97d1016218bed433f70c6d4a5d6851e76)) · updated November 21, 2026 with the #195 collection and adjudication decisions, and November 23, 2026 with the code layout**

Statements about #195 follow its November 21 decisions. Other statements about the state of the stack, including "the #199 snapshot", "At #199", work that later stack branches still need, and the #197–#199 drafts, describe the stack as of November 19, when `584254d` was the #199 head, unless they say *current* (as of November 21). The current branches have since resolved some of them. The code layout section, and statements that name #209 or #210, describe the stack as of November 23.

Lambda Managed Instances (LMI) can run overlapping invocations in one execution environment and reuse workers across requests. A platform timeout can end an invocation while user code is still running. Durable execution history can show the persisted outcome, but proving invocation isolation also requires knowing which request owned the work, when the SDK dispatched it, and what happened to the original worker and its peers. JavaScript, Java, and Python need the same acceptance rules across different runtime models. [Tracking issue #161](https://github.com/aws/aws-durable-execution-conformance-tests/issues/161)

## Scope

Add an opt-in, language-neutral `lmi-lifecycle` suite with seven required scenarios per supported runtime and a shared harness for live AWS runs. Preserve the existing ordinary-conformance entry points, including `--suite all`. Keep language-specific fixtures and regressions in each SDK. LMI OpenTelemetry and plugin conformance are deferred. The lifecycle contract requires observable isolation and bounded recovery. It leaves cancellation mechanisms to SDKs and makes no hard-termination guarantee for arbitrary user I/O. [Contract #193](https://github.com/aws/aws-durable-execution-conformance-tests/pull/193) · [Tracking issue #161](https://github.com/aws/aws-durable-execution-conformance-tests/issues/161)

## Required behavior

The table summarizes the contract merged in #193. See its evidence guide and requirement YAML for the full assertions. Later stack branches still need to incorporate that merged contract. [Contract #193](https://github.com/aws/aws-durable-execution-conformance-tests/pull/193)

| ID | Scenario | What the evidence must establish |
| --- | --- | --- |
| 1 | Native overlap | The original victim and peer belong to distinct durable executions and reach the barrier during observed overlap at the runtime’s sharing boundary. Each dispatches its own successful `record-marker` step checkpoint during overlap. The stored results match distinct fixture expectations for their respective invocations. |
| 2 | Normal return | The root succeeds and the wrapper returns. A victim-owned checkpoint probe at or after actual wrapper return is rejected, and the SDK dispatches no new request for the closed invocation. |
| 3 | Deadline during step | Controlled work is in flight at the platform timeout. Admission closes within `admissionMs` of the timeout, both probes are rejected at or after closure, and the task exits within `cleanupMs` of the timeout. |
| 4 | Peer recovery | Establish the overlap and deadline observations. The original peers occupy every other native slot through recovery and show progress before timeout and again by recovery. An independent recovery request records `worker_recovered` in the original environment under the selected worker policy within `recoveryMs` of timeout, and succeeds. |
| 5 | Suspension and retry | The service resumes the same execution after `PENDING`. Replay preserves operation identity, including unnamed operations, and reuses stored successes and failures without rerunning completed bodies. An incomplete operation has a failed attempt followed by success, with the expected side-effect counts. |
| 6 | Warm reuse | The warm request runs in the same environment, worker and process after the victim invocation. Victim-owned late activity occurs during the warm request and precedes the victim task’s exit. |
| 7 | Normal failure | The root fails and the wrapper returns the failure. Preserve the outcome and apply the same post-return checkpoint rules as in case 2. |

The merged #193 contract makes the selected victim’s operation probe optional at or after actual wrapper return. A supplied outcome is retained. If admission had closed before wrapper return, an observed `ACCEPTED` probe fails the case. An omitted or rejected operation probe is still allowed. The rejected checkpoint probe at or after wrapper return remains required. The #199 snapshot still requires both rejected probes and needs the merged contract integrated. A separate late SDK dispatch fails even when the checkpoint probe reports rejection or service history lacks the request. An earlier dispatch may settle after closure. Cases 2 and 7 also require a separate victim task exit at or after root completion. A raised wrapper and a returned failure are distinct boundaries. [Contract #193](https://github.com/aws/aws-durable-execution-conformance-tests/pull/193) · [Closed disposal draft #200](https://github.com/aws/aws-durable-execution-conformance-tests/pull/200)

## Architecture

| Layer | Responsibility |
| --- | --- |
| Contract and validator | The catalog defines roles, minimum native concurrency and scheduling constraints. The version 1 JSON Schema describes final evidence. The executable Python validator checks structure and semantics for observation and final validation, including ownership, causal order, replay, coverage and retirement. Verdicts for collected runs come only from adjudication, which applies these rules to the collected evidence. |
| Coordinator | Runs collection. It admits the manifest, budgets and schedules cases, journals dispatch, freezes and retains evidence, and audits physical retirement to decide whether a target can be reused. It never decides conformance, so its results carry collection statuses and never `PASSED`. It can run against fake or SDK adapters without embedding AWS transport logic. |
| Shared AWS driver | Deploys persistent concurrency-1 and concurrency-2 targets, drives barriers and faults, invokes fixtures, and normalizes service history and S3 ledger records. It manages run-scoped cleanup and persistent retirement fences. |
| SDK adapter | Binds the deployed SDK fixture to the scenarios and supplies public-API fixtures, runtime configuration, lifecycle hooks and expected-outcome policy where needed. The shared driver can be packaged without importing a language SDK. |
| Reusable workflow | Runs the harness and trusted cloud jobs, owns OIDC and account verification, refreshes credentials, and collects cleanup results, artifacts and summaries. SDK callers supply a stage script that builds and deploys the tested SDK artifact, and pin the workflow and package to the same revision. |

The runtime profile describes real placement. Node.js overlap occurs within a worker, Java overlap within a JVM, and Python overlap across worker processes in one environment. Python warm reuse establishes sequential reuse of a specific process. The scheduler reserves capacity, while placement assertions use observed environment and worker identities. Use a new deployment profile when the scenario needs incompatible runtime or function settings. [Contract #193](https://github.com/aws/aws-durable-execution-conformance-tests/pull/193) · [Coordinator #195](https://github.com/aws/aws-durable-execution-conformance-tests/pull/195) · [Driver #189](https://github.com/aws/aws-durable-execution-conformance-tests/pull/189) · [Workflow #190](https://github.com/aws/aws-durable-execution-conformance-tests/pull/190)

## Code layout

The contract, collection, adjudication and rules rows follow the module layout that #209, #195 and #210 introduce. The other rows follow the #202 tree. That tree and the #189, #190 and #197–#199 branches keep the earlier layout until they adopt the new one. In the earlier layout, the rules live in `evidence.py`, adjudication lives in `offline.py`, and some cleanup re-checks and retention helpers also remain in the coordinator. Package paths are relative to `packages/aws-durable-execution-conformance-tests/src/aws_durable_execution_conformance_tests/lmi/`. Test paths are relative to `packages/aws-durable-execution-conformance-tests/`. [Adjudication #209](https://github.com/aws/aws-durable-execution-conformance-tests/pull/209) · [Coordinator #195](https://github.com/aws/aws-durable-execution-conformance-tests/pull/195) · [CLI #210](https://github.com/aws/aws-durable-execution-conformance-tests/pull/210)

**Contract, collection and adjudication**

| Path | Responsibility |
| --- | --- |
| `__init__.py` | Package for the opt-in multi-invocation LMI suite. |
| `app.py` | CLI and programmatic entry point. It admits the manifest and claims the output directory. With `--adapter` it runs collection followed by adjudication, and with `--collected-evidence` it adjudicates a saved bundle again. With neither, it validates the manifest's standalone evidence files, a mode that a separate PR will remove. Every mode writes the results and reports. |
| `coordinator.py` | Collection. It schedules cases on shared targets, journals launches, freezes observations and audits physical retirement to decide reuse or quarantine. It never decides conformance. |
| `adjudication.py` | Adjudication. It derives each case's final status from a collected capture alone: its snapshots, launch journal and collection status. |
| `evidence.py` | Public facade for validating a single evidence snapshot: `validate_evidence`, the retirement audits and the `EvidenceValidator` protocol. |
| `catalog.py` | Explicit catalog of LMI requirements, capability profiles and runtimes, separate from ordinary suite discovery. |
| `matrix.py` | Scenario and configuration slots that each coverage profile requires, used to report missing coverage. |
| `requirements/lmi-lifecycle/` | Requirement YAML for the seven lifecycle scenarios. |
| `evidence-schema.json` | Version 1 JSON Schema for evidence. |
| `evidence-schema.md` | Evidence contract, the collection and adjudication flow, the trust boundary and the manifest format. |

**Rules (`rules/`)**

| Path | Responsibility |
| --- | --- |
| `rules/engine.py` | Rule entry points. `establish` runs the envelope and scenario rules, and `retire` audits case-owned retirement. |
| `rules/view.py` | Per-case evaluation state: the evidence frame, derived indexes and findings. |
| `rules/frames.py` | The evidence frames, or snapshots, that a case is judged from. |
| `rules/values.py` | Contract constants, the validator protocol and exact JSON comparison helpers. |
| `rules/common.py` | Rules shared by every scenario: envelope, ownership, histories and admission. |
| `rules/overlap.py` | Lifecycle 1 and the lifecycle 4 cohort: native overlap and execution isolation. |
| `rules/disposal.py` | Lifecycle 2 and 7: normal return or failure, and admission after return. |
| `rules/deadline.py` | Lifecycle 3 and the lifecycle 4 victim: crossing a real invocation deadline. |
| `rules/recovery.py` | Lifecycle 4: healthy peer progress and worker recovery. |
| `rules/replay.py` | Lifecycle 5: suspension, replay and retry. |
| `rules/warm_reuse.py` | Lifecycle 6: warm reuse and late activity. |
| `rules/retirement.py` | Settlement of case-owned work and the independent retirement audit. |
| `rules/reconcile.py` | Shared cleanup re-check state, which pairs the frozen evidence with later snapshots, for each scenario's `reconcile_*` checks. |
| `rules/retention.py` | Merging one run's snapshots and reconciling their records. Collection and adjudication share it. |

**Shared AWS driver (#189), workflow (#190) and guides**

| Path | Responsibility |
| --- | --- |
| `adapter.py` | `SharedAdapter`, which an SDK instantiates to implement the coordinator's adapter protocol over the shared driver. |
| `scenarios.py` | Drives the seven lifecycle scenarios through real SDK handlers. |
| `cloud.py` | Bounded real-service driver built on the repository's cloud runner. |
| `service.py` | Bounded AWS transport, independent of SDK language and testing libraries. |
| `deployment.py` | Deploys and reuses the persistent LMI stacks. Runtime and build choices stay with the caller. |
| `configuration.py` | Identity of the deployed runtime artifact and its configuration. |
| `runtime.py` | Explicit runtime and artifact configuration for an SDK-owned fixture. |
| `observations.py` | Language-neutral source selection and runtime-specific placement models. |
| `source_evidence.py` | Normalizes observed SDK and service records into the evidence contract. |
| `artifacts.py` | Run artifacts, redaction and deterministic case namespaces. |
| `quarantine.py` | Persistent retirement fences that keep a target out of use across runs until it is recovered. |
| `runner.py` | Builds the standard manifest from a deployment (`make_manifest`), runs it with strict reporting (`run`) and performs run-scoped cleanup (`cleanup`). |
| `summary.py` | Summarizes the lifecycle report without claiming cloud acceptance. |
| `errors.py` | Shared deployment, placement and source-collection failures. |
| `README.md` | User guide for running the suite and writing its manifest. |
| `REUSE.md` | Guide for SDKs that reuse the harness: the bindings they supply and the reusable workflow. |
| `DESIGN.md` | This design document. |
| `.github/workflows/lmi-lifecycle.yml` (repository root) | Reusable workflow that runs the harness and the trusted cloud jobs. |

**Tests**

| Path | Responsibility |
| --- | --- |
| `tests/test_lmi_*.py` | Tests of the contract, rules, adjudication, coordinator and CLI, and of the user guide's manifest examples. They use hand-built evidence and fake adapters, and need no AWS access. |
| `tests/lmi_driver/` | Tests of the driver, deployment, quarantine fences and evidence normalization against mocked or stubbed AWS clients, and of the reusable workflow file. |

## Execution and evidence

A run has two halves. Collection runs the cases on AWS and records what happened. Adjudication derives the verdict from that record alone. Collection never decides conformance, and adjudication never contacts AWS. [Coordinator #195](https://github.com/aws/aws-durable-execution-conformance-tests/pull/195)

1. Admit the manifest and retain a copy. Every CLI mode claims an empty output directory with an exclusive `manifest.json` write, so simultaneous runs cannot share it. Validate catalog membership and bind each case to its declared profile, roles and budgets. Report declared `NOT_IMPLEMENTED` cases without dispatching them.
2. Schedule compatible cases against an unchanged target within its capacity. Exclusive cases require other owners to retire. The coordinator lets an adapter release its native capacity reservation after its requests are idle and reacquire it before controlled dispatch, while retaining target ownership. The #199 snapshot releases the built-in callback case’s reservation after `PENDING` and reacquires it after callback lookup. The agreed #189 change keeps built-in cases’ reservations through callback lookup, automatic retries and retirement. Record the attempt in `attempts.jsonl` before dispatch and abort the launch if the write fails.
3. Run the selected invocations under bounded provisioning, placement, execution and collection phases. Correlate run and case IDs, request IDs, execution ARNs and worker identities. The shared driver journals a stable durable execution name before invoking and uses it for transport retries, including ambiguous responses. Service invocation timestamps and SDK lifecycle events establish different parts of the timeline.
4. Freeze each case's observations before releasing its faults. SDK request records can show attempts that service history cannot see, so retain them with lifecycle events and persisted history. Preserve prior observations, failures and identity across snapshots. Host-monotonic coordination deadlines never become evidence. At #199, individual driver calls still use their own timeouts. The agreed #189 design propagates the remaining phase budget to each wait, including callback lookup.
5. Release case-owned faults and audit outstanding executions, callbacks, requests and tasks. A cleanup request is evidence of an attempt to retire work. Positive settlement and ownership evidence are required before reuse. Preserve both behavioral and retirement failures. Profiles finish only after every case in the run has finished retirement and registered its ownership claims, so a conflicting claim quarantines the affected targets before any lock is released. A profile is reported quiescent, which lets `finish` release its external lock, only if its target is not quarantined and no interrupted adapter task from the run is still running, because such a task could still claim a target.
6. After every target is released or quarantined, write the frozen observations and separate cleanup evidence to `collected-evidence.json`, read the file back and adjudicate it against the original admitted cases. Adjudication compares SDK request records with lifecycle events and persisted history and applies the declared behavior budgets. `attempts.jsonl` and `results.jsonl` are the run journals: a `COLLECTED` row in `results.jsonl` awaits the verdict row that adjudication appends. Every CLI mode writes final per-case results to `evidence.json`, coverage to `coverage.json`, and JSON and JUnit reports. `evidence.json` holds the final result even if a final journal append fails. Strict acceptance exits nonzero for failed, missing or unimplemented required cases. [Contract #193](https://github.com/aws/aws-durable-execution-conformance-tests/pull/193) · [Coordinator #195](https://github.com/aws/aws-durable-execution-conformance-tests/pull/195) · [Driver #189](https://github.com/aws/aws-durable-execution-conformance-tests/pull/189)

```sh
# Collect through an SDK adapter, then adjudicate the collected evidence.
durable-execution-conformance lmi-lifecycle --manifest manifest.json \
  --adapter sdk_harness:factory --output lmi-collection

# Adjudicate a saved bundle again, without an adapter or AWS credentials.
durable-execution-conformance lmi-lifecycle --manifest lmi-collection/manifest.json \
  --collected-evidence lmi-collection/collected-evidence.json --output lmi-adjudication
```

Adjudicating a saved bundle again is a tool for debugging, investigating a reported verdict and developing rules. It is not a verification step, and CI does not run it. Adjudication has a trust boundary: it assumes that the coordinator wrote `collected-evidence.json` and that the file has not been modified. It judges behavior again with the current rules, but trusts the recorded collection status and the facts only collection can observe. It does not re-verify collection integrity, such as journal binding or the independent physical retirement audit, because the coordinator checks those before it reuses a target. [Coordinator #195](https://github.com/aws/aws-durable-execution-conformance-tests/pull/195)

## Target ownership and failure recovery

In-process quarantine prevents the coordinator from dispatching to a target whose retirement is unproven. A scheduler serves one run; sharing it across runs is not supported, so it has no in-process recovery and a quarantined target stays quarantined for the rest of the run. When a profile is not quiescent, the adapter's `finish` must keep its external run ownership in place until recovery. The driver's S3 fence persists across runs and protects a target if cleanup fails or the process is interrupted. At #199 the driver conditionally creates fences, then checks run and commit before deletion. The agreed #189 changes add a unique acquisition identity, exact-record readback after an ambiguous acquisition, and ETag-conditional deletion to protect against replacement between verification and deletion. Unknown ownership or incomplete retirement requires explicit recovery outside the run before reuse. Other targets can continue. [Coordinator #195](https://github.com/aws/aws-durable-execution-conformance-tests/pull/195) · [Driver #189](https://github.com/aws/aws-durable-execution-conformance-tests/pull/189)

The workflow's fixed concurrency group serializes cloud jobs within each caller repository with cancellation disabled. Separate SDK callers need isolated capacity because GitHub concurrency groups are repository-scoped. Fork and Dependabot changes remain on the unprivileged harness path. The workflow collects artifacts and attempts cleanup on failure so operators can distinguish assertion failures, evidence gaps and unresolved retirement. [Workflow #190](https://github.com/aws/aws-durable-execution-conformance-tests/pull/190)

## Tradeoffs

- SDK-only suites can exercise language-specific races directly, but duplicate placement and evidence checks. Shared validation gives these runtimes one acceptance contract while preserving SDK-specific fixtures.
- Per-case deployment simplifies isolation but repeats provisioning and makes reuse across scenarios harder to measure. Persistent profiles reduce deploys, with leases, ownership and retirement fences needed to keep reuse safe.
- Service-history-only checks are simpler to collect but miss attempted transport sends and worker-local behavior. A normalized evidence ledger lets adjudication run the same assertions without AWS access at the cost of requiring trustworthy instrumentation and complete collection. [Tracking issue #161](https://github.com/aws/aws-durable-execution-conformance-tests/issues/161) · [Driver #189](https://github.com/aws/aws-durable-execution-conformance-tests/pull/189)
- Re-running collection integrity checks during adjudication could detect inconsistent edits to a bundle, but it cannot prove the bundle is unmodified and it duplicates checks the coordinator already makes before reuse. Trusting coordinator-written bundles keeps adjudication simpler. Either way, a saved bundle supports debugging, not verification. [Coordinator #195](https://github.com/aws/aws-durable-execution-conformance-tests/pull/195)

## Follow-ups and rollout

- **Warm reuse ordering.** The stack follow-up uses distinct per-request sequence numbers when victim late activity and task exit share a millisecond timestamp. Missing or ambiguous ordering makes the evidence incomplete. [#197](https://github.com/aws/aws-durable-execution-conformance-tests/pull/197)
- **Missing peer wrapper return.** A regression draft shows that a late peer dispatch can pass when the wrapper-return record is omitted. The cutoff and classification are still open in the draft. [#198](https://github.com/aws/aws-durable-execution-conformance-tests/pull/198)
- **Held-fault recovery.** The draft reproduces false passes for early release and an unrelated gate. It also reports a false pass with an empty fault inventory, with regression coverage pending. The proposed rule requires a recorded fault tied to the victim’s actual gate and held until the selected recovery request succeeds. The draft also proposes manifest and evidence agreement on `reuse-worker` or `replace-worker`. A fixture's own deadline signal cannot substitute for platform timeout or task exit. [#199](https://github.com/aws/aws-durable-execution-conformance-tests/pull/199)
- **Standalone evidence files.** #195 still validates `evidence_file` entries from the manifest when neither `--adapter` nor `--collected-evidence` is given. A pass in that mode proves less than a collected run, because there is no journal, cleanup or independent physical retirement audit. A separate PR will remove the mode, its manifest field and its documentation, keeping the library `validate_evidence`.
- **Retirement audit during adjudication.** Adjudication could re-run the independent physical retirement audit from a saved bundle instead of trusting the recorded collection status. Under the trust boundary above, this has lower priority and can follow #189's adoption of the latest #195 changes.

The core merge order is **#193 → #195 → #189 → #190**, followed by Python caller **#931** pinned to the final shared workflow revision. #195 is now split into #209 → #195 → #210, which merge in that order ahead of #189. The core PRs report validation without live AWS runs: retained evidence, fake transport and fixtures. The rollout gate requires retained evidence and regression-sensitivity controls on at least two distinct real runtime models. Each supported runtime still needs to pass all seven cases for conformance. [Tracking issue #161](https://github.com/aws/aws-durable-execution-conformance-tests/issues/161) · [Python #931](https://github.com/aws/aws-durable-execution-sdk-python/pull/931)

GitHub shows #197, #198 and #199 above the core changes. The November 19 #199 snapshot contains the held-fault regressions. The warm-reuse fix and missing-return regressions live on separate commits outside that snapshot. That snapshot predates the #195 collection and adjudication split: its coordinator still reports verdicts, and its CLI neither offers `--collected-evidence` nor claims the output directory exclusively. The current #189 and #190 branches have the split. The current #197, #198 and #199 branches have the saved-bundle CLI, but their coordinator can still adjudicate cases itself. All five current branches use the earlier #195 names (`live_results`, `collected_evidence_results` and `lmi/offline.py`) and predate the latest #195 changes described here, such as the trust boundary, the run-wide finish barrier and one scheduler per run. The current branches need to adopt those changes, including the renames to `collect_and_adjudicate`, `adjudicate_collected_evidence` and `lmi/adjudication.py`. [Stack #196](https://github.com/aws/aws-durable-execution-conformance-tests/stack/196)
