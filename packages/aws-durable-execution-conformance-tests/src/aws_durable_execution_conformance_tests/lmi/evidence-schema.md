# LMI lifecycle evidence, version 1

`validate_evidence(document, phase="observations" | "final")` is the executable
schema and semantic validator. It returns findings prefixed `INCOMPLETE:`,
`PLACEMENT:`, or `ASSERTION:`. Unknown versions fail closed. Producers may add
fields; they cannot omit required version 1 observations. `evidence-schema.json`
describes the structural wire format; the Python validator also checks ownership,
ordering, placement, outcomes, and coverage.

All times are finite, nonnegative milliseconds on a single observation clock.
Adapters must reconcile clocks before producing this document and preserve raw
source times and correlation records in `collection.evidenceUris`. An arbitrary
client timestamp is not proof of a runtime boundary. Collect wrapper return and
platform timeout through the actual wrapper/platform observation path. A root
handler return, a PENDING response, and task exit are separate events.

The envelope identifies `schemaVersion: 1`, `runId`, `caseId`, `cohortId`,
`requirementId` (`lmi-lifecycle-1` through `lmi-lifecycle-7`), and `runtime`
(`javascript`, `java`, or `python`). `artifact.sdk` and `artifact.conformance`
identify exact immutable tested artifacts. `profile` includes `functionArn`,
`configurationDigest`, `nativeConcurrency`, and the deployed `timeoutMs`.
`configurationDigest` covers runtime, architecture, layers, environment, IAM,
timeout, and concurrency. Native concurrency describes deployment configuration,
not the number of requests sent.

`selectedRequests` maps fixture roles to original invocation request IDs. The
coordinator must bind this map to its launch journal, before interpreting results.
A later service retry cannot replace a selected original request. Every launched
attempt remains in `attempts`, including failed placement and unrelated retries.
Each attempt records:

- `requestId`, `executionArn`, `environmentId`, `workerId`, `processId`, `role`;
- `attempt` (positive ordinal within the fixture role; selected requests use 1);
- `startedAtMs`, `endedAtMs`, and independently observed invocation `outcome`
  (`SUCCEEDED`, `FAILED`, `PENDING`, or `TIMED_OUT`).

`events` is an array of observations. Every event records `kind`, `requestId`,
`ownerRequestId`, `executionArn`, and `atMs`. The invocation and execution owner
must agree with the launch record. Late work keeps its original `requestId` and
`ownerRequestId`; `observedDuringRequestId` identifies a later invocation during
which it happened. Reassigning late work to the current invocation is a failure.
Events may outlive wrapper return and the native invocation interval, but must
remain inside `observationEndedAtMs`.

`sdkRequests` retains every attempted SDK request, including rejected requests.
Each has the same ownership/time fields plus `method` and `outcome` (`DISPATCHED`
or `REJECTED`). No dispatch is allowed after that invocation's `wrapper_return`
or `admission_closed`, whichever came first. Admission probes are separate
`admission_probe` events with `probe: operation | checkpoint` and `outcome:
REJECTED`; both probes must be observed after closure.

`histories` contains every launched execution's service history, with
`executionArn`, `ownerCaseId`, `status`, `result` or `error`, and `operations`.
Optional `startedAtMs` is the actual service execution start from
`GetDurableExecution.StartTimestamp` or `ExecutionStarted.EventTimestamp`,
normalized to UTC Unix epoch milliseconds. When supplied, it must be finite,
nonnegative, belong to that history's execution ARN, and precede or equal that
execution's first native admission.
Each operation retains `id`, static `name`, `type`, `parentId` (null at root),
`status`, and its stored result/error. `name` is explicitly null for a genuinely
unnamed operation, including SDK-supported unnamed child contexts; missing names
are incomplete evidence. Preserve its actual ID and parent instead of inventing
a name. Execution history status and its fixture
expectation must be recognized `SUCCEEDED`, `FAILED`, `TIMED_OUT`, `STOPPED`,
`RUNNING`, or `PENDING` values; nonterminal observations cannot establish final
retirement. `expectedHistories` is keyed by execution
ARN and declares the fixture's expected `status` and `result`/`error` independently
of the fetched service output. A fixture whose execution can remain active while
its fault is held may instead declare `allowedStatuses`, a nonempty list of
unique recognized statuses. Exactly one of `status` or `allowedStatuses` is
required. Only a list containing `RUNNING` permits omission of `result`/`error`;
any declared output expectation is still compared. For example,
`{"allowedStatuses": ["RUNNING", "FAILED", "TIMED_OUT", "STOPPED"], "result": null}`
permits an active timeout victim without accepting a successful outcome. This
observation allowance does not establish retirement: every owned execution still
needs a terminal retirement record. All declared expected fields are compared;
`allowedStatuses` itself is expectation metadata, not a service-history field.
Never generate expectations by copying the observed status or output.

`collection.watermarks` contains independently confirmed `lifecycle`,
`sdkRequests`, and `history` coverage through `observationEndedAtMs`.
`collection.evidenceUris` references retained raw records under this case's
namespace. An empty query without a collection watermark cannot prove absence.
Source collection failures and incomplete observations must remain visible.

## Scenario observations

| ID | Required observations |
| --- | --- |
| `lmi-lifecycle-1` | Original `victim` and `peer`, distinct executions, overlapping intervals, and `barrier_arrived` for each inside the overlap. Same environment always; same Node.js worker/process, same JVM process, or distinct Python processes. Histories and fixture expectations preserve attribution. |
| `lmi-lifecycle-2` | Normal return: `victim` and `root_completed` both have outcome `SUCCEEDED`, followed by `wrapper_return`, both rejected admission probes, and separate `task_exit`. |
| `lmi-lifecycle-3` | `victim` with `work_in_flight`, actual `platform_timeout` (`source: platform`) at or beyond the configured timeout, `admission_closed`, both rejected probes, and cooperative `task_exit` after timeout. `budgets.admissionMs` and `budgets.cleanupMs` are agreed fixture limits, not product SLAs. |
| `lmi-lifecycle-4` | Deadline and overlap observations plus original `recovery` request and `worker_recovered`. `profile.recoveryPolicy` explicitly selects `reuse-worker` (same process/worker) or `replace-worker` (different worker, with `replacesWorkerId` naming the original). Both stay in the original environment. All other native slots are occupied by selected `peer*` requests until recovery, each with `peer_progress` before and after timeout. The recovery request must succeed. |
| `lmi-lifecycle-5` | Original `victim` returns PENDING with `pending` and `wrapper_return`; selected `resume` starts later against the same execution. `replay.before` stores completed success and failure operations. `replay.after` retains that exact prefix and matches collected final history. `replay.operationExecutions` records actual user-code entries by request/operation (and `attemptId` for retries), proving completed code did not run again. `replay.retry` identifies an incomplete operation and at least two distinct observed attempts, first failed and last successful. `replay.sideEffects` records `operationId`, actual `count`, and fixture-defined `expectedCount` for completed operations and retries. |
| `lmi-lifecycle-6` | Original `victim` and sequential `warm` request reuse the same environment, worker, and process. `late_activity` retains victim ownership and identifies the warm request in `observedDuringRequestId`. Separate `task_exit` follows the late activity; no stale SDK dispatch is permitted. |
| `lmi-lifecycle-7` | Normal failure: `victim` and `root_completed` both have outcome `FAILED`, followed by `wrapper_return`, both rejected admission probes, and separate `task_exit`. |

Use static durable operation names. Routing uses fixture/scenario and immutable
handler inputs. Run, case, and cohort identifiers isolate evidence and controls;
they do not change durable names, branching, or concurrency topology on replay.

## Retirement

`resourcesCreated` inventories case-owned objects with `id`, `kind`, and
`ownerCaseId`. It must include every invocation (`id = requestId`, `kind =
invocation`) and durable execution (`id = executionArn`, `kind = execution`), plus
callbacks, control objects, and other retained work. `faultsCreated` separately
inventories fault IDs and owners. If a fault was already released during
observation, record `releasedAtMs` there; release cannot establish the earlier
cleanup/recovery assertion.

In `final` validation, `retirement.retiredAtMs` follows observation completion.
`retirement.resources` matches every created resource exactly once, preserving
kind and owner, with `settledAtMs`, an `evidenceUri`, and outcome. Invocations must
be `QUIESCENT`; executions must have terminal `SUCCEEDED`, `FAILED`, `TIMED_OUT`,
or `STOPPED` outcomes. PENDING and wrapper return alone cannot settle ownership.
`retirement.faults` likewise matches every created fault and records `releasedAtMs`
and `mode: normal | emergency`. Releasing a fault before the asserted cleanup or
recovery is a failure regardless of release mode.

The coordinator first freezes collected observations and validates with
`phase="observations"`, before release. It preserves those findings while adding
retirement observations for `phase="final"`. Final retirement validation runs
even when earlier observation records were malformed or incomplete. Successful emergency release cannot
rewrite failed behavior. Missing or uncertain retirement quarantines the affected
profile; other profiles remain independent. A blanket `passed` or `cleanedUp`
boolean is not evidence for any requirement.

The SDK adapter is responsible for faithful instrumentation through the real
public handler registration paths. This schema detects inconsistent or
incomplete evidence; it cannot authenticate observations supplied by a dishonest
adapter or substitute local synthetic controls for real LMI validation.
