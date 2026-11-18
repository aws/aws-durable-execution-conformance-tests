# LMI lifecycle evidence, version 1

`validate_evidence(document, phase="observations" | "final")` is the executable
schema and semantic validator. It returns findings prefixed `INCOMPLETE:`,
`PLACEMENT:`, or `ASSERTION:`. Unknown versions fail closed. Producers may add
fields; they cannot omit required version 1 observations. `evidence-schema.json`
describes the structural wire format; the Python validator also checks ownership,
ordering, placement, outcomes, and coverage.

Integer-valued wire fields accept finite, mathematically integral JSON numbers,
including decimal and exponent spellings such as `2.0` and `2e0`. Booleans remain
distinct from numbers. Manifest loading normalizes these quantities to Python
integers before passing them to typed configuration and coverage APIs.

Observation validation checks the structure of supplied, recognized optional
fields in every scenario. Presence and behavioral requirements remain scoped to
the applicable scenario; unknown extension fields stay permitted. Caller-owned
common validation preserves custom coverage-profile names. The separate physical
retirement audit may retain partial behavior diagnostics while proving ownership
and quiescence, and does not require those diagnostics to form complete replay
or budget observations.

All times are finite, nonnegative milliseconds on a single observation clock.
Adapters must reconcile clocks before producing this document and preserve raw
source times and correlation records in `collection.evidenceUris`. An arbitrary
client timestamp is not proof of a runtime boundary. Collect wrapper return and
platform timeout through the actual wrapper/platform observation path. A root
handler return, a PENDING response, and task exit are separate events.

The envelope identifies `schemaVersion: 1`, `runId`, `caseId`,
`requirementId` (`lmi-lifecycle-1` through `lmi-lifecycle-7`), and `runtime`
(`javascript`, `java`, or `python`). `artifact.sdk` and `artifact.conformance`
identify exact immutable tested artifacts. `profile` includes `functionArn`,
`configurationDigest`, `nativeConcurrency`, and the deployed `timeoutMs`.
`configurationDigest` covers runtime, architecture, layers, environment, IAM,
timeout, and concurrency. Native concurrency describes deployment configuration,
not the number of requests sent.

`runId` and the run-unique `caseId` identify the scenario and its evidence.
There is no additional cohort identifier; roles bind its original invocations.

`selectedRequests` maps fixture roles to original invocation request IDs. The
coordinator must bind this map to its launch journal, before interpreting results.
A later service retry cannot replace a selected original request. Every launched
attempt remains in `attempts`, including failed placement and unrelated retries.
Each attempt records:

- `requestId`, `executionArn`, `environmentId`, `workerId`, `processId`, `role`;
- `attempt` (positive ordinal within the fixture role; selected requests use 1);
- `startedAtMs`, `endedAtMs`, and independently observed invocation `outcome`
  (`SUCCEEDED`, `FAILED`, `PENDING`, or `TIMED_OUT`).

Each `(role, attempt)` pair is unique within the case, across all recorded
requests and executions. A different execution does not reset a role's ordinal;
different roles may use the same ordinal, including when they share an execution.

The source normalizer uses the matching request's
`InvocationCompletedDetails.StartTimestamp` for a completed native invocation's
start. It must be finite, nonnegative and no later than that request's first
fixture observation. A missing or invalid start on a collected completion record
makes the evidence incomplete. Before a platform completion record is available,
the normalizer retains the narrower fixture-observed interval; that alone cannot
establish a full native timeout. Never substitute the durable execution start or
infer native admission by subtracting the configured timeout from a deadline.

`events` is an array of observations. Every event records `kind`, `requestId`,
`ownerRequestId`, `executionArn`, and `atMs`. The invocation and execution owner
must agree with the launch record. Late work keeps its original `requestId` and
`ownerRequestId`; `observedDuringRequestId` identifies a later invocation during
which it happened. Reassigning late work to the current invocation is a failure.
Events may outlive wrapper return and the native invocation interval, but must
remain inside `observationEndedAtMs`.

`sdkRequests` retains every attempted SDK request, including rejected requests.
Each has the same ownership/time fields plus `method` and `outcome` (`DISPATCHED`
or `REJECTED`). No dispatch is allowed at or after that invocation's first
`wrapper_return`, `wrapper_raise`, or `admission_closed`. A raised wrapper exit
closes admission for peers, resumes, retries, and warm requests too. The disposal
fixtures specifically require a `wrapper_return`, including their normal root
failure case; a wrapper that itself throws is a different boundary. Admission
probes are separate `admission_probe` events with `probe: operation | checkpoint`
and a recorded `outcome`. For the selected `victim` in `lmi-lifecycle-2` or
`lmi-lifecycle-7`, a `checkpoint` probe with outcome `REJECTED` is required at
or after actual wrapper return. An `operation` probe at that boundary is
optional and diagnostic; its outcome may be `ACCEPTED` or `REJECTED`, and it
must meet the same ownership and observation rules as other events. The victim
must have one wrapper exit: a return. If a victim probe and a closure share a
timestamp, use their per-request `sequence` values to order them when both
are present; these must be distinct nonnegative integers. With either
sequence absent, the timestamp boundary remains inclusive. This exception
does not permit late SDK dispatch or require forced cancellation of previously
dispatched work.

All other probes at or after their invocation's earliest recorded closure must
be rejected. In particular, an earlier `admission_closed` or `wrapper_raise`
does not gain an exception from a later boundary. Deadline and recovery
fixtures still require both rejected probes at or after `admission_closed`.

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
a name. Optional operation `startedAtMs` and `endedAtMs` are finite, nonnegative
timestamps; when both are supplied, the end must be at or after the start.
Execution history status and its fixture
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
The optional `collectionErrors` array retains non-empty source error messages;
omit it or use an empty array when collection succeeded. Owned lifecycle events
with kind `observation_error` or `control_error` retain failed observation or
control publication. Any reported source or publication failure makes the
evidence incomplete in both observation and final validation, even when source
watermarks cover the full window. Independent retirement applies the same rule.

## Scenario observations

| ID | Required observations |
| --- | --- |
| `lmi-lifecycle-1` | Original `victim` and `peer`, distinct executions, overlapping intervals, and `barrier_arrived` for each inside the overlap. Same environment always; same Node.js worker/process, same JVM process, or distinct Python processes. Histories and fixture expectations preserve attribution. |
| `lmi-lifecycle-2` | Normal return: `victim` and `root_completed` both have outcome `SUCCEEDED`, followed by `wrapper_return`, a rejected checkpoint probe, and separate `task_exit`; operation admission is optional diagnostic evidence as described above. |
| `lmi-lifecycle-3` | `victim` with `work_in_flight`, actual `platform_timeout` (`source: platform`) satisfying `startedAtMs + profile.timeoutMs <= platform_timeout.atMs <= endedAtMs`, `admission_closed`, both rejected probes, and cooperative `task_exit` after timeout. Closure, probes and cleanup may follow native completion. `budgets.admissionMs` and `budgets.cleanupMs` are agreed fixture limits, not product SLAs. |
| `lmi-lifecycle-4` | Deadline and overlap observations plus original `recovery` request and `worker_recovered`. `profile.recoveryPolicy` explicitly selects `reuse-worker` (same process/worker) or `replace-worker` (different worker, with `replacesWorkerId` naming the original). Both stay in the original environment. All other native slots are occupied by selected `peer*` requests until recovery, each with `peer_progress` before and after timeout. Recovery must use an execution distinct from the victim and every selected peer. Python recovery must also use a process distinct from every held peer; reuse of the victim's process requires recovery admission at or after the victim's native invocation `endedAtMs`, regardless of worker naming or policy. Background task exit remains a separate boundary. The recovery request must succeed. |
| `lmi-lifecycle-5` | Original `victim` returns PENDING with `pending` and `wrapper_return`; selected `resume` starts later against the same execution. `replay.before` stores completed success and failure operations. `replay.after` preserves each completed operation unchanged by operation ID and matches collected final history; completed operations need not form a positional prefix. `replay.operationExecutions` records actual user-code entries by request/operation (and `attemptId` for retries), proving completed code did not run again. `replay.retry` identifies an incomplete operation and at least two distinct observed attempts, first failed and last successful. `replay.sideEffects` records `operationId`, actual `count`, and fixture-defined `expectedCount` for completed operations and retries. |
| `lmi-lifecycle-6` | Original `victim` and sequential `warm` request reuse the same environment, worker, and process. The warm request starts after the victim's `wrapper_return` and at or after its native invocation `endedAtMs`. Victim-owned activity may outlive that native interval: `late_activity` retains victim ownership and identifies the warm request in `observedDuringRequestId`. Separate `task_exit` follows the late activity; no stale SDK dispatch is permitted. |
| `lmi-lifecycle-7` | Normal failure: `victim` and `root_completed` both have outcome `FAILED`, followed by `wrapper_return`, a rejected checkpoint probe, and separate `task_exit`; operation admission is optional diagnostic evidence as described above. |

Use static durable operation names. Routing uses fixture/scenario and immutable
handler inputs. Run and case identifiers isolate evidence and controls;
they do not change durable names, branching, or concurrency topology on replay.

For deadline and recovery cases, `budgets.observationGraceMs` is the minimum
retained observation tail after the asserted scenario boundary: the victim's
actual `platform_timeout` for `lmi-lifecycle-3`, and the selected recovery's
`worker_recovered` for `lmi-lifecycle-4`. `observationEndedAtMs` must reach that
boundary plus the declared grace, with collection watermarks covering the full
window. A missing or too-short tail is incomplete measurement evidence; it does
not by itself establish an SDK behavior failure. Physical retirement audits
remain independent of this behavior-observation requirement.

Every `replay.operationExecutions` record requires nonempty string `requestId`
and `operationId` fields. The request must identify a recorded invocation. An
operation may be incomplete or not yet checkpointed, so its ID need not appear
in the final history, but an entry without an ID cannot establish skipped work.
The operation identified by `replay.retry.operationId` must be `SUCCEEDED` in
`replay.after` and the matching final history, with its stored `result` key.
Explicit JSON null is a valid result; an absent result is incomplete evidence.
Unrelated pending operations need no stored result. This cross-record identity
constraint is enforced by the semantic validator, since the structural JSON
schema cannot compare an array entry's ID with the retry object's ID.
Invocation intervals can be provisional, and owned activity may outlive them;
their endpoints alone do not establish retry user-code entry order.

## Retirement

`resourcesCreated` inventories case-owned objects with `id`, `kind`, and
`ownerCaseId`. It must include every invocation (`id = requestId`, `kind =
invocation`) and durable execution (`id = executionArn`, `kind = execution`), plus
callbacks, control objects, and other retained work. `faultsCreated` separately
inventories fault IDs and owners. When a release has been observed, record its
`releasedAtMs` there as a finite, nonnegative timestamp on the same normalized
clock. A final snapshot may add a post-assertion cleanup release after
`observationEndedAtMs`; the frozen observation phase precedes that cleanup. Both
observation and final validation check every supplied release timestamp before
comparing it to cleanup or recovery; release cannot establish the earlier
cleanup/recovery assertion.

In `final` validation, `retirement.retiredAtMs` follows observation completion.
Inventoried invocation and execution IDs must match the observed requests and
their service histories in both directions. The independent retirement audit
enforces the same correspondence; a settlement entry alone cannot account for
an unobserved invocation or execution.
`retirement.resources` matches every created resource exactly once, preserving
kind and owner, with `settledAtMs`, an `evidenceUri`, and outcome. Invocations must
be `QUIESCENT`; executions must have terminal `SUCCEEDED`, `FAILED`, `TIMED_OUT`,
or `STOPPED` outcomes. PENDING and wrapper return alone cannot settle ownership.
An execution's settlement must be at or after its service history's `startedAtMs`
when supplied. Native admission is a separate boundary and does not substitute
for missing service-start metadata.
Durable callbacks use `kind: callback` and require an observed `SUCCEEDED`,
`FAILED`, or `TIMED_OUT` outcome; their `evidenceUri` retains the service completion
record. Case-owned control objects use `kind: control` and require `DELETED`,
with `evidenceUri` retaining the provider's deletion acknowledgment or a read
confirming absence. A requested cleanup or a locally declared success is not
completion evidence. Both final validation and independent failed-case audits
enforce these outcomes. Other retained resource kinds need an explicit completion
contract before the independent audit can permit target reuse.
`retirement.faults` likewise matches every created fault and records `releasedAtMs`
and `mode: normal | emergency`. Its timestamp records the provider release
acknowledgment or retained release confirmation. The fixture's release receipt
and the provider's acknowledgment are observations from independent sources;
either may arrive first, so their timestamps need not equal or order each other.
For example, a release request at 205, provider acknowledgment at 210, and fixture
polling receipt at 215 are consistent with retirement at 220. Both supplied
timestamps must be at or before `retirement.retiredAtMs`. An absent original release time
remains valid when the final acknowledgment proves release. Releasing a fault
before the asserted cleanup or recovery is a failure regardless of release mode.

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


## Independent physical retirement audit

`validate_retirement_evidence` audits a separate post-cleanup snapshot, including
failed cases. It needs the actual completion stream for every launched request,
with the ownership and normalized timestamp fields above. It requires:

- Exactly one `wrapper_enter`, one wrapper exit (`wrapper_return` or
  `wrapper_raise`), and one `task_exit` with `taskId: root` per request. Entry must
  precede or equal both exits.
- A nonnegative integer `sequence` on every event, unique within each request.
  Sequence order must agree with nondecreasing `atMs`. Equal timestamps retain
  their semantic order through sequence; interval and entry/exit comparisons use
  `(atMs, sequence)`.
- Exactly one start and exit for every work identity in the following pairs.
  Neither an unmatched start nor an unmatched exit establishes completion, and
  each start must precede or equal its exit.

| Start | Exit | Identity within a request |
| --- | --- | --- |
| `task_start` | `task_exit` | Nonempty `taskId`, with `root` reserved for the root task |
| `io_start` | `io_exit` | Nonempty `taskId` identifying this observed I/O interval |
| `checkpoint_call` | `checkpoint_exit` | Nonempty `callId` identifying this call |
| `poll_call` | `poll_exit` | Nonempty `callId` identifying this call |

Each I/O interval needs a distinct ID in the `taskId` field; reusing the owning
task's ID for multiple I/O intervals cannot establish unique completion pairs.
Identities are scoped to the request and event pair. Multiple child tasks and
calls are permitted in this audit, with all their start and exit facts retained.

Behavior assertions use one scenario-owned `task_exit` for the selected request.
Other task facts may be retained as `observed_task_start`/`observed_task_exit`
with their original `sourceKind`. The physical audit locally restores these
known aliases to `task_start`/`task_exit`, requiring a matching `sourceKind` for
each. It rejects missing or mismatched source kinds and unknown names in the
reserved `observed_task_*` alias family. Every restored child start and exit
receives the same completion checks as a native task event. The audit leaves the
caller's behavior evidence unchanged; already restored snapshots remain valid.
The root exit alone does not prove child-task or I/O completion. The wire schema
covers the shared envelope; these additional physical completion rules apply to
the independent audit rather than every behavior observation document.

Every invocation's settlement must cover its last actual activity. Every
collected execution history must be terminal and agree with its retirement
outcome. Only invocation, execution, callback, and control resource kinds have
completion contracts in this audit; an unknown retained resource kind cannot
permit target reuse. Successful retirement does not clear failed behavior
assertions, and unresolved completion keeps the target quarantined.
