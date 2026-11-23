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
Adapters call `record_attempt` before dispatch and `record_service_attempt` when
discovering a service-initiated attempt against an already journaled execution.
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
remain inside `observationEndedAtMs`. The optional event field `sequence` must be a
nonnegative integer when present. It is required wherever this document says
tied timestamps need sequences to establish order, and on every event in the
independent physical retirement snapshot.

`sdkRequests` retains every attempted SDK request, including rejected requests.
Lifecycle 1 requires a checkpoint dispatch from each successful selected original
invocation during their native overlap and
exactly one successful STEP named `record-marker` in each selected service
history. The step result must match that execution's fixture-declared expected
result. Use distinct expected results for the two selected executions. Each SDK request has the same
ownership/time fields plus `method` and `outcome` (`DISPATCHED`
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
must have one wrapper exit: a return. If a dispatch or a disposal probe and a closure share a
timestamp, use their per-request `sequence` values to order them when both
are present; these must be distinct nonnegative integers. With either
sequence absent, only the dispatch or probe comparison against a closure
keeps the inclusive timestamp boundary. This exception
does not permit late SDK dispatch or require forced cancellation of previously
dispatched work.

All other probes at or after their invocation's earliest recorded closure must
be rejected. In particular, an earlier `admission_closed` or `wrapper_raise`
does not gain an exception from a later boundary. Deadline and recovery
fixtures still require both rejected probes at or after `admission_closed`.

`histories` contains every launched execution's service history, with
`executionArn`, `ownerCaseId`, `status`, and `operations`. Retain the stored
`result` or `error` when the service reports it; a held `RUNNING` history can have neither.
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
`RUNNING`, `PAUSING`, `PAUSED`, or `PENDING` values; nonterminal observations
cannot establish final retirement. `expectedHistories` is keyed by execution
ARN and declares the fixture's expected `status` and `result`/`error` independently
of the fetched service output. A fixture whose execution can remain active while
its fault is held may instead declare `allowedStatuses`, a nonempty list of
unique recognized statuses. Exactly one of `status` or `allowedStatuses` is
required. Only a list containing `RUNNING`, `PAUSING`, or `PAUSED` permits
omission of `result`/`error`; any declared output expectation is still compared. For example,
`{"allowedStatuses": ["RUNNING", "FAILED", "TIMED_OUT", "STOPPED"]}`
permits an active timeout victim without accepting a successful outcome. This
observation allowance does not establish retirement: every owned execution still
needs a terminal retirement record. When adjudication re-checks a collected capture,
an execution that settles inside the observation window must have its declared
`result`/`error` supplied by a terminal history observation with the settled
status, in any snapshot. A frozen nonterminal view does not supply it. All
declared expected fields are compared;
`allowedStatuses` itself is expectation metadata, not a service-history field.
Never generate expectations by copying the observed status or output.

`collection.watermarks` contains independently confirmed `lifecycle`,
`sdkRequests`, and `history` coverage through `observationEndedAtMs`.
`collection.evidenceUris` references retained raw records under this case's
namespace. An empty query without a collection watermark cannot prove absence.
Source collection failures and incomplete observations must remain visible.
An observation that excludes an owned service-only retry retains its request
and execution identity in `collection.deferredServiceRequests`. Adjudication
requires the capture's independent retirement evidence to contain every
deferred request and its actual completion.
A service retry first discovered during retirement also joins that inventory.
Collection audits its quiescence, and adjudication checks late SDK dispatches
against the retirement evidence. A single document cannot prove these
obligations, so `validate_evidence` reports such a case incomplete;
`validate_common_evidence` leaves them to adjudication.

Collected replay observations compare `replay.before` with
`replay.expectedBefore`, the complete set of stored operation outcomes in
service history strictly before the selected native resume. When an actual
serialized value provides encoding evidence, `resultEncoding` distinguishes
JSON text from raw strings without requiring identical JSON formatting.
For a serialized JSON execution result, the builder records a
`resultFingerprint` on the service history before rounding the display. The
validator derives the comparison fingerprint from the declared fixture result
and checks both the fingerprint and the displayed structure.
The optional `resultFingerprint` preserves exact numeric semantics of
serialized JSON even when the displayed result is rounded. Matching digests
reconcile corresponding numeric values only when one display is the
binary64 rounding of the other. They do not override structure or
nonnumeric values. A finite number outside the display's numeric range is
rendered as canonical decimal text while remaining JSON in the replay
comparison. A saved replay
can report `projectionErrors` when service chronology is incomplete.
Those errors invalidate behavior evidence without erasing the physical audit.
Timer fixtures report `replay.durableWaitObserved` after complete service
collection. Callback and timer fixtures also verify the native request
resumed by the public operation and report `replay.suspensionCheckpointMatches`.
A wakeup tied with the resume timestamp counts only for the independently
identified waking operation. A false value is a behavior failure.

A complete replay observation may report `replay.missingExpectedEffects` when
a fixture's named effect has no durable operation identity or when no retry
attempts were observed. When `missingExpectedEffects` includes `retry`, the
retry record may be absent or retain one observed attempt. This fails the
behavior check. It does not invalidate independent retirement evidence.
The optional `collectionErrors` array retains non-empty source error messages;
omit it or use an empty array when collection succeeded. Owned lifecycle events
with kind `observation_error` or `control_error` retain failed observation or
control publication. Any reported source or publication failure makes the
evidence incomplete in both observation and final validation, even when source
watermarks cover the full window. Independent retirement applies the same rule.

## Scenario observations

Shared driver runs require a conformance revision in the generated plan.
Historical evidence without known provenance cannot pass behavior validation.

A lifecycle-3 or lifecycle-4 observation may mark a nonselected victim retry
with `activeAtObservationEnd: true`. Its sealed ledger and matching service
completion must prove that it started by the cutoff and ended afterwards.
The observation retains its in-window events, SDK requests and
`startedAtMs`, and omits `endedAtMs` and `outcome`. Independent retirement
must retain the same native start, the actual post-cutoff end and the
observed outcome. Earlier V1 documents are unchanged. Adjudication proves
this marker against the capture's independent retirement evidence, and
`validate_evidence` reports a single document with the marker incomplete.

| ID | Required observations |
| --- | --- |
| `lmi-lifecycle-1` | Original `victim` and `peer`, distinct executions, overlapping intervals, and `barrier_arrived` for each inside the overlap. Same environment always; same Node.js worker/process, same JVM process, or distinct Python processes. Histories and fixture expectations preserve attribution. |
| `lmi-lifecycle-2` | Normal return: `victim` and `root_completed` both have outcome `SUCCEEDED`, followed by `wrapper_return`, a rejected checkpoint probe, and separate `task_exit`; operation admission is optional diagnostic evidence as described above. |
| `lmi-lifecycle-3` | `victim` with `work_in_flight`, actual `platform_timeout` (`source: platform`) satisfying `startedAtMs + profile.timeoutMs <= platform_timeout.atMs <= endedAtMs`, `admission_closed`, both rejected probes, and cooperative `task_exit` after timeout. Closure, probes and cleanup may follow native completion. `budgets.admissionMs` and `budgets.cleanupMs` are agreed fixture limits, not product SLAs. |
| `lmi-lifecycle-4` | Deadline and overlap observations plus original `recovery` request and `worker_recovered`. `profile.recoveryPolicy` explicitly selects `reuse-worker` (same process/worker) or `replace-worker` (different worker, with `replacesWorkerId` naming the original). Both stay in the original environment. All other native slots are occupied by selected `peer*` requests until recovery, each with `peer_progress` before and after timeout. Recovery must use an execution distinct from the victim and every selected peer. Python recovery must also use a process distinct from every held peer; reuse of the victim's process requires platform-sourced recovery admission at or after the victim's platform-sourced native invocation `endedAtMs`, regardless of worker naming or policy. Background task exit remains a separate boundary. The recovery request must succeed. |
| `lmi-lifecycle-5` | Original `victim` returns PENDING with `pending` and `wrapper_return`; selected `resume` starts later against the same execution. `replay.before` stores completed success and failure operations. `replay.after` preserves each completed operation unchanged by operation ID and matches collected final history; completed operations need not form a positional prefix. `replay.operationExecutions` records actual user-code entries by request/operation (and `attemptId` for retries), proving completed code did not run again. `replay.retry` identifies an incomplete operation and at least two distinct observed attempts, first failed and last successful. `replay.sideEffects` records `operationId`, actual `count`, and fixture-defined `expectedCount` for completed operations and retries. |
| `lmi-lifecycle-6` | Original `victim` and sequential `warm` request reuse the same environment, worker, and process. The warm request starts after the victim's `wrapper_return` and at or after its native invocation `endedAtMs`. Victim-owned activity may outlive that native interval: `late_activity` retains victim ownership and identifies the warm request in `observedDuringRequestId`. Separate `task_exit` follows the late activity. When both events have the same `atMs`, each needs a distinct nonnegative integer `sequence`; the earlier event has the lower sequence. Otherwise, timestamps determine their order. Cleanup-discovered victim `late_activity` follows the same order against the frozen `task_exit`, compared within one snapshot. No stale SDK dispatch is permitted. |
| `lmi-lifecycle-7` | Normal failure: `victim` and `root_completed` both have outcome `FAILED`, followed by `wrapper_return`, a rejected checkpoint probe, and separate `task_exit`; operation admission is optional diagnostic evidence as described above. |

For lifecycle 3 and 4, `work_in_flight` must precede the platform timeout.
`admission_closed` and `task_exit` must follow it. The `work_in_flight`
and `task_exit` observations must carry the same `taskId`. Any recorded start
for that task must precede the in-flight observation. If they share a
timestamp, distinct per-request sequences must establish that order. When one of these events shares the timeout timestamp, distinct per-request
`sequence` values for that event and the timeout must prove the order.
A missing or ambiguous sequence leaves the order incomplete.

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
be `QUIESCENT` or `UNDISPATCHED`; executions must have terminal `SUCCEEDED`, `FAILED`,
`TIMED_OUT`, or `STOPPED` outcomes. PENDING and wrapper return alone cannot settle
ownership. An execution's settlement must be at or after its service history's
`startedAtMs` when supplied. Native admission is a separate boundary and does
not substitute for missing service-start metadata.

An invocation the service rejected before any handler ran may settle as
`UNDISPATCHED`, in physical retirement only. `executionArn` is the owned
execution whose history holds the request's only `InvocationCompleted` event.
`sourceEventId` is that event's `EventId`. `serviceErrorType` is its
`Error.Payload.ErrorType` and must exactly match the validator's allowlist.
`startedAtMs` and `settledAtMs` are its normalized `StartTimestamp` and
`EndTimestamp`, with `settledAtMs <= retiredAtMs`. `evidenceUri` is the
retained raw history, listed in `collection.evidenceUris`. The request has no
attempt, event, SDK request or completion. Behavior validation reports every
`UNDISPATCHED` invocation as INCOMPLETE; it never satisfies a role, attempt or
deferred-request obligation.
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

The coordinator freezes collected observations before release and verifies
physical retirement before allowing target reuse. Missing or uncertain
retirement quarantines the affected profile. Adjudication checks the frozen
observations once and audits retirement once, against a copy combined with the
retirement records; a custom validator receives `phase="observations"` and then
`phase="final"`. Emergency release cannot
erase a behavior failure. A blanket `passed` or `cleanedUp` boolean is
insufficient evidence.

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

- Every dispatched `CheckpointDurableExecution` or `GetDurableExecutionState`
  request carries its `callId` and matches the corresponding checkpoint or poll
  start with the same request, call ID, and dispatch timestamp. The start must
  have a matching exit. In behavior validation a recorded checkpoint or poll
  start matches exactly one SDK request with the same method, `requestId`,
  `callId`, `atMs`, and outcome (`DISPATCHED` by default or `REJECTED`).
  A rejected start can omit its exit when corroborated by a matching rejected
  request. Other dispatched methods require a completion contract before
  retirement can be established.

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

A proven `UNDISPATCHED` invocation is exempt from the entry/exit/sequence
completion contract above: the service's own rejection record, not a wrapper
stream, is its proof. The audit still requires its `executionArn`,
`serviceErrorType`, `sourceEventId`, `startedAtMs`, and `settledAtMs` to be
internally consistent and that no event, SDK request, `observedDuringRequestId`,
or retirement completion names the request. This settles physical retirement
only; behavior validation always reports it INCOMPLETE.

An independent retirement snapshot may mark `retirement.mode` as
`controls-only` after a case fails before any Invoke could succeed. This
audit requires a reconciled launch journal with only local PREPARED intents
or definite REJECTED attempts, empty runtime inventories and no replay
assertions or collection watermarks. Every possibly held control must
have one acknowledged release and subsequent deletion receipt under the
admitted case's S3 control prefix. Each settlement references the retained
case control journal and uses its acknowledged deletion timestamp. The
coordinator retains the original case failure.

When a work start is present in the behavior stream, final validation requires
its completion before its invocation settles. A completion collected after
the observation window can be placed in `retirement.completions`, with the
same request and work identities as the start. Tied timestamps require distinct
per-request sequences to establish interval order. Behavior validation reads only
the frozen observation stream.

For lifecycle 6, the selected victim's `endedAtMs` must come from the platform
native invocation completion record and the warm attempt's `startedAtMs` from
its platform start record. Set their corresponding `endedAtMsSource` and
`startedAtMsSource` to `platform`. A provisional fixture or wrapper timestamp
cannot prove sequential reuse.

Checkpoint `updates` entries use lowercase `id`, `type`, and `action` keys.
For lifecycle 1 the selected request's overlap checkpoint contains a
`STEP` update with action `SUCCEED` for its marker operation ID. Lifecycle 5
requires a `WAIT` or `CALLBACK` update with action `START` and the same
operation ID and type as a successful operation in the collected service
history. That operation's recorded completion must follow the pending wrapper
return and precede or coincide with a later invocation's admission. The request must belong to a PENDING
invocation of the selected execution, precede its wrapper return, and be
followed by another invocation of that execution. This establishes an observed
wait or callback suspension even when an earlier invocation paused for retry. Replay also needs at least one completed STEP in
`replay.before`. Every completed STEP in that snapshot needs an original-request
entry in `operationExecutions` and a victim-owned `body` event whose
`operationId` matches the step ID. The body must precede the victim's `pending`
event. If they share a timestamp, distinct sequences must establish that order.
Any supplied step completion time must be at or before that suspension. For lifecycle 2 and 7, when the selected execution
has only one invocation, both its terminal service history and final retirement
outcome must agree with the required invocation outcome.

A retry `body` event for the retried operation identifies a declared attempt by
`attemptId`, or by `requestId:attempt` when the producer records a numeric
`attempt`. There is at most one timed body per attempt. Its timing must agree
with retry order and precede or coincide with the recorded operation completion.
Completed step bodies likewise cannot follow their recorded completion.

Adjudication preserves precise fractional values in checkpoint payloads
and extension fields. Python callers may receive a `Decimal` for fractions that
cannot round-trip as floats. Timings and budgets still use the runner's finite
clock precision.

## Collection

Collection runs the cases on AWS and records what happened. Adjudication then
derives the verdict from that record alone. Collection never decides
conformance; adjudication never contacts AWS.

Write a manifest as described under [Manifest](#manifest), and use it with an
SDK-owned adapter to run cases and collect evidence. The adapter module must be importable
in the runner's Python environment. Its factory receives the admitted manifest
and returns an adapter implementing `lmi.coordinator.ScenarioAdapter`: `prepare`
and `finish` for each profile that an active case uses, and `start`, `place`,
`execute`, `collect`, `release`, and `retire` for each case. When every case is
`not_implemented`, the runner does not load the adapter.

```sh
durable-execution-conformance lmi-lifecycle --manifest manifest.json --adapter my_adapter:create --output lmi-collection
```

Choose an empty output directory, and use a separate one for every run. The
runner journals attempts to `attempts.jsonl` before dispatch and publishes case
outcomes to `results.jsonl`, including predeclared skips. Both JSONL journals
may have multiple records for a case. The last record for the case contains its
latest published attempts or outcome. A row with status `COLLECTED` awaits the
verdict. After all targets are released or quarantined, the runner writes frozen
observations and separate cleanup evidence to `collected-evidence.json`, reads
the file back, and adjudicates it to produce the final report.

A collection manifest may also set `scenarioConcurrency`, the most cases the run
executes at once (default 8), and `budgets`, harness time limits in seconds:
`provisioning` for each target's `prepare` (default 300), `lease` for waiting on
target capacity (300), `placement` for `start` and `place` (120), `execution`
(300), `collection` (120), `release` (30), and `retirement` for each case's
`retire` and each target's `finish` (120). These bound the harness, not SDK
cleanup or delivery.

Collection holds each target through physical retirement of its own cases.
It finishes profiles only after every case in the run has ended its retirement
attempt and registered its ownership claims, so a conflicting claim quarantines
the affected targets before any release. Quiescence then decides which targets
an adapter may unlock. If any case phase in the
run was interrupted, no profile is reported quiescent, because the interrupted
task could have claimed, or could still claim, any target. A failed or
interrupted preparation or `finish` quarantines its own target. Each run uses
its own scheduler. Because profiles finish together, an adapter's `prepare` must
not wait on a lock that another profile's `finish` releases, in this run or a
concurrent one: such a wait can reach the provisioning deadline and leave the
waiting target quarantined. Acquire a lock that several of this run's targets
share once for the run. Release it after the last `finish` only if every target
sharing it finished with `quiescent=True`; otherwise keep it. For a lock another
run holds, fail fast or wait well under the provisioning budget.

The coordinator only collects evidence and decides whether a target can be
reused. Its `run_scenarios` API returns collection statuses and never reports
`PASSED`. A collection run with `--adapter` and a later `--collected-evidence`
run judge the coordinator's captures with the same adjudication.

## Adjudication

A collection run with `--adapter` adjudicates its evidence as soon as it is
collected. To adjudicate a saved bundle again, without loading an adapter, pass
`--collected-evidence`. This is a tool for debugging, for investigating a
reported verdict, and for developing rules. It is not a verification step:

```sh
durable-execution-conformance lmi-lifecycle --manifest lmi-collection/manifest.json --collected-evidence lmi-collection/collected-evidence.json --output lmi-adjudication
```

Adjudication trusts the bundle: it assumes that the coordinator wrote it and
that it was not modified. Before recording `COLLECTED`, the coordinator bound
every observation and retirement record to the launch journal and checked that
the case's work settled. Adjudication judges behavior again with the current
rules, but it keeps the recorded collection status and the facts only collection
observed. Pass the collection's own `manifest.json`. Adjudication re-checks only
the collection facts its verdict rules use: each case's identity against that
manifest, ownership claims across cases, launch coverage, and the retirement
proof behind the lifecycle 2/7 allowance. The frozen observations must include
every client launch the journal records (`launchedAtMs`): cleanup can journal a
new client launch after observations froze, and the coordinator still records
`COLLECTED`, so adjudication reports that case incomplete. A service-initiated
attempt (`discoveredAtMs`) may be absent. The lifecycle 2/7 allowance requires a
retirement snapshot that passes `validate_retirement_evidence`. Adjudication
does not verify collection integrity otherwise, so a hand-edited bundle can
receive a wrong verdict.

## Manifest

A manifest admits the run's target profiles and cases. Under strict coverage,
failed, missing, uncovered, and `not_implemented` cases all produce a nonzero
exit status.

An inventory with two default profiles and no cases looks like this. The runner
reports all mandatory cases as uncovered until cases are added.

```json
{
  "schemaVersion": 1,
  "runId": "sample-run",
  "runtime": "python",
  "capabilityProfile": "lifecycle",
  "profiles": [
    {
      "profile_id": "native-1",
      "resource_id": "arn:example:function:native-1",
      "native_concurrency": 1,
      "capacity": 1,
      "artifact_id": "sdk-revision",
      "configuration_id": "config-1"
    },
    {
      "profile_id": "native-2",
      "resource_id": "arn:example:function:native-2",
      "native_concurrency": 2,
      "capacity": 2,
      "artifact_id": "sdk-revision",
      "configuration_id": "config-2"
    }
  ],
  "scenarios": []
}
```

Each scenario declares `case_id`, `requirement_id`, `scenario`, and
`profile_id`. Use the catalog's scenario identifier: `native-overlap` (1),
`normal-return` (2), `deadline-during-step` (3), `peer-recovery` (4),
`suspension-retry` (5), `warm-reuse` (6), or `normal-failure` (7).
Additional target profiles need a nonempty
`incompatibilityReason`. Each case can declare `slots` up to the profile's
capacity. For an exclusive requirement, `exclusive` may be omitted or set to
`true`. Case roles
default to the requirement's roles and must match evidence `selectedRequests`.
List additional roles explicitly when the fixture uses them. Lifecycle 3 and 4
cases must declare positive `behavior_budgets` for `admissionMs` (timeout to
admission close), `cleanupMs` (timeout to task exit), and
`observationGraceMs` (continued observation after the scenario boundary).
Lifecycle 4 also requires `recoveryMs`, the budget from the victim's timeout
to `worker_recovered`. The evidence must
report each declared budget with the same value. If the profile declares
`recovery_policy`, it must match evidence `profile.recoveryPolicy`.

For an unavailable fixture, declare `not_implemented` with an optional reason.
The runner reports the case without dispatching it.
