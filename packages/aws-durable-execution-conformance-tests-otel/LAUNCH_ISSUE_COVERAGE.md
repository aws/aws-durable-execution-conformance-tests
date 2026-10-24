# OTel launch regression coverage

The context requirements add normal user spans through each SDK's public APIs. The
handlers read the active span context and create a span without an explicit
parent. They never attach a replacement context, fabricate SDK spans, or invoke
instrumentation hooks themselves.

| Launch work item | Requirements | Regression detected |
| --- | --- | --- |
| Chained-invoke propagation | `otel-invocation-11`, `otel-execution-11` | The child service carrier must retain the first invoke span's Root/Parent and sampled-positive decision. X-Ray also requires the real child ancestry to reach that span; completed replay must not start another child. |
| 4 — Python status mapping | `otel-invocation-24`, `otel-execution-24` | A genuine invocation retry must report `RETRYING` and `UNSET`, followed by successful recovery. An ordinary step retry reports `PENDING` and does not test this path. |
| 4 — Errorless operation failure | `otel-invocation-25`, `otel-execution-25` | A handler catches the real failed child, checkpoints an observation, replays through two barriers and returns successfully while the FAILED callback leaf remains ERROR. Exception events are optional. Service history retains its untruncated, exactly empty error payload. |
| External-completion replay | `otel-invocation-26`, `otel-execution-26` | A root callback first completes before a checkpointed observation step, then is visited again on two later resumes without another terminal export. |
| 5 — User-function context | `otel-invocation-22..23`, `otel-execution-22..23` | User code must receive valid context on the execution's trace, with the appropriate operation/attempt parent and restoration after nested work and resume. |
| 6 — Completed-operation replay exports | `otel-execution-21` | A completed step before a normal wait/resume must not export its operation or user attempt again. The paired invocation-view case permits that view's distinct replay segments. |

The status case deliberately does not reject duplicate operation delivery after
an invocation failure. That is expected recovery behavior, separate from a normal
successful suspension and resume.

Case 11 keeps the existing public invoke and echo target, then pauses the caller
at `otel-invoke-replay-barrier`. After the runner completes that callback with
`resume`, the caller replays the successful invoke and returns its stored result.
The exact history and SDK span counts require one child execution and three caller
invocations: PENDING, PENDING, SUCCEEDED. The terminal invoke belongs to caller
invocation 2; invocation 3 must not start another child or export another terminal
invoke. Five seconds of unchanged telemetry bound the duplicate observation.

The target ARN comes from actual `ChainedInvokeStarted` history. A read-only
`GetDurableExecution` call supplies the child's service-reported trace header. Its Root
must equal the caller invoke's trace ID, Parent must identify the first invoke
span, and Sampled must be 1. Target Workflow and Invocation spans must be on that
trace. S3 proves this carrier boundary and the normal SDK span structure; it does
not supply missing Lambda/service parent spans. X-Ray must additionally expose a
complete parent path from both target roots to the same first invoke. A complete
path ending elsewhere is a failure, even if every span shares a Root. No
cross-execution clock-ordering assertion is added by this propagation check.
Case 11 retains its caller replay barrier; case 27 separately exercises child suspension and resume.

Use the S3 collector to verify duplicate export counts and `UNSET` status. It
retains repeated OTLP span records, including repeated trace/span IDs. X-Ray can
merge updates to the same ID and normalizes unset status, so it cannot by itself
prove those two properties.

Case 21 checks the deployed SDK's step replay exports and the single export of
the externally completed timer wait, `otel-replay-wait`. It does not exercise
the JS local runner's invocation-event wrapper. The `UpdatedOperationIds`
copying regression fixed by
[JS PR #954](https://github.com/aws/aws-durable-execution-sdk-js/pull/954)
is covered by the SDK's
[local runner integration test](https://github.com/aws/aws-durable-execution-sdk-js/blob/19ee19fa0619093943f079b69943f7b5cdee7ac7/packages/aws-durable-execution-sdk-js-testing/src/test-runner/local/__tests__/integration/plugin-external-completion.integration.test.ts),
not by this cloud suite, and is not a prerequisite for these cloud cases.
The existing core plugin requirements
[10-9](../aws-durable-execution-conformance-tests/test-requirements/plugin/10-9.yaml)
and [10-19](../aws-durable-execution-conformance-tests/test-requirements/plugin/10-19.yaml)
separately check service-supplied externally updated operations on resume.

Case 21's service history requires `WaitSucceeded` before the following
`StepStarted`, with the same operation ID as `WaitStarted`. Execution-view timer
timestamps are supplied by the service, so their ordering against SDK step or
Workflow timestamps is not required. The timer must still have its Workflow
parent and link to the resumed Invocation. Case 27 applies the same rule to its
child timer's Workflow containment; parent identity and invocation phase remain
strict even when service and SDK clocks differ.

The same clock rule applies to case 26's root callback and the chained invokes
in cases 11 and 27. Initial invocation-view waits and callback scopes in cases
21, 25 and 26 explicitly name Invocation 1; their start segments cannot move
into a resumed invocation while leaving an empty initial invocation behind.

## User-function coverage and limits

Cases 22 and 23 cover handler entry and resume, successful and retried step
bodies, nested steps, child contexts, concurrent parallel branches and map
iterations, condition-check callbacks, callback submitters, the body and strategy
of a wrapped retry helper, and virtual child-context bodies. Probes after nested
work check that the caller's context is restored. Valid non-recording context
placeholders are allowed; `isRecording()` is not the context-validity contract.

Handler-root assertions preserve the configured view and runtime integration:
execution-view code may use Workflow context, invocation-view code may use
Invocation context, and a same-trace non-SDK Lambda parent is also valid. Every
probe must use the Workflow's trace. Operation callbacks require their own named
context or attempt, so a valid span from another branch does not pass.

This does **not** claim that every callable accepted anywhere in an SDK has an
operation-span contract. Step retry policies and condition wait policies run
after the wrapped user body; serialization, deserialization, result summaries,
batch completion policies and item naming also have different execution phases
across SDKs. Some run on coordinator/worker threads outside the current
user-function hooks. A new expected parent for those phases requires a defined
SDK contract; requiring the already-ended body attempt would be incorrect.

Input deserialization can precede invocation-start instrumentation. Plugin
factories, context extractors and samplers can run before the span they create
exists. Arbitrary threads or tasks launched by application code are outside the
SDK-owned callback boundary. These phases cannot universally be required to have
an active SDK span under the existing lifecycle. The callback audit must remain
explicit about these limits rather than accepting fabricated test context.

Case 25 sends the public callback-failure API only the callback ID, after the
callback's creating invocation has completed. The service represents this as a
present `Error` wrapper with `Payload: {}` and `Truncated: false`. The requirement
checks that exact empty payload and its object type; it does not treat every
empty error object as `None`, and it does not require the wrapper to be absent.
The callback leaf must be `FAILED/ERROR`: explicit terminal failure status is
independent of whether error details are available. The handler catches the public
failed-child/callback error, checkpoints `caught` in
`otel-failed-callback-observed`, then crosses
`otel-failed-callback-barrier-one` and `otel-failed-callback-barrier-two`. Their
success results are `one` and `two`; the final execution returns `caught/one/two`
and is `SUCCEEDED/OK`. The four Invocations are PENDING/OK, PENDING/OK, PENDING/OK
and SUCCEEDED/OK. Case 17 retains the separate unhandled, supplied-error failure
path. SDKs must not fabricate exception details to obtain ERROR status.

The first callback failure is consumed and caught in invocation 2. Invocations 3 and 4
replay the persisted failed child into the catch and reuse the completed marker;
the marker body and failed submitter must not run again. The failed leaf remains
one terminal record associated with the first resumed invocation, with strict
identity, parent and link checks. Invocation-view local segments precede the
observation attempt. Execution-view backend end timestamps are not calibrated to
the SDK clock, so that view does not compare the failed operation's end with the
observation attempt's start; service-history phases and invocation links establish
the association. These are cached failed-child
replays: a completed child can restore its failure without revisiting the nested
callback body. This does not claim a callback failure while invocation 1 remains
active, or completion during End/forceFlush. SDK public tests separately establish
any additional live-delivery or direct-callback replay coverage.

Invocation-view completed steps may still emit replay segments without running
their user bodies. Case 25 therefore permits one observation-step segment in
Invocation 2 plus at most one linked replay segment in each of Invocations 3 and
4. Its single user attempt must belong to the original segment. Callback
submitters in cases 11 and 25 may likewise emit a second segment when their
pending callback scope resumes; their attempts remain single. Generic `STEP`
and `CALLBACK` names are disambiguated by checkpointed operation IDs. These
allowances do not relax terminal callback export counts or execution-view
completed-operation counts. Case 26 already leaves step segments outside its
complete-coverage scope while checking the observation attempt separately.

The corrected operation-end contract maps explicit `FAILED`, `TIMED_OUT`, and
`STOPPED` to `ERROR`, even without error metadata. An empty service error payload
does not imply that the SDK has no exception to record. OpenTelemetry
[permits an exception event when an exception escapes the span and causes ERROR status](https://opentelemetry.io/docs/specs/otel/trace/exceptions/),
even if an enclosing workflow later catches it. Case 25 accepts telemetry with or
without that event and does not require a backend to expose its count.
Successful operations remain `OK`; pending operations and invocation
`RETRYING/UNSET` retain their existing behavior.
Unrelated cancellation is not assigned a new status by this change. SDK unit
controls cover the terminal status mapping, details, success and retry behavior.
Case 25 exercises the actual cloud `FAILED` path; it does not claim cloud coverage
of `TIMED_OUT` or `STOPPED`. An execution timeout leaving a wait `STARTED` (case
15) is not evidence of an operation ending `TIMED_OUT` without an error.

Case 25's complete matching trace must remain unchanged for five seconds, so an
early passing observation cannot hide a later duplicate terminal export within
that window. A change in the observed exception-event count restarts the
stability window, but does not invalidate an otherwise conformant trace.
This bounds the observation period; it does not prove that no record can arrive
after the window. S3 retains repeated OTLP records and is the reference for the
terminal export count; X-Ray can merge updates to the same span ID. Every backend
must still enforce the required ERROR status.

Case 26 uses a direct root `createCallback` operation, not a callback hidden in
a completed child context. Every replay visits that same operation. After its
first result, a durable step records the value once; two separate callback
barriers then force two later resumes. Driver actions wait for a newer
`InvocationCompleted` event before delivering each response. The four-invocation
history, stable operation IDs, root parentage and first-completion invocation are
checked. Invocation-view spans also check timing against the observation attempt.
Execution-view callback timestamps come from the service, so that view uses
history and invocation links to associate completions with their phases without
comparing those timestamps to the observation attempt's SDK clock.
Raw S3 terminal records must occur exactly once, including when two
records have identical span IDs. Five seconds of unchanged telemetry are
required. X-Ray merging cannot establish this raw export-count property.
This cloud scenario does not exercise the JS local invocation-event wrapper.

New requirements and SDK handler updates should be reviewed together. Configure
`conformance_test_ref` to the new requirement commit when validating the handler
PRs; an older pinned requirement revision filters out the new handlers and is not
coverage evidence.


Case 21 now requires five seconds of valid, unchanged normalized telemetry before
passing. Late duplicate records reset or invalidate that candidate. Handler-root
fallbacks reject probe spans tagged with `conformance.callback`, and invocation
replay checks the required link shape for each allowed segment count.

The self-test workflow pins the Python companion fixture from SDK PR #796,
including the dedicated caught-failure/replay case25 handler and the correction
for explicit terminal failure without error details. A fixture still routing25
to the unhandled case17 handler, or exporting the failed leaf as UNSET, is
incompatible with this successful-execution requirement. The fixture retains the #752/#756 runtime prerequisites and
all 27 handlers. Manual dispatch can override that revision; the
`failed+uncovered` coverage threshold is unchanged.

Case 24 covers every execution-correlated Invocation span and requires five
seconds of stable telemetry. The normal lifecycle is RETRYING/UNSET followed by
SUCCEEDED/OK. Recovering an interrupted at-most-once step can additionally
checkpoint one retry timer, yielding a non-first PENDING/OK invocation between
those phases. Both two- and three-invocation sequences are checked in full,
including order and status mappings. Legacy RETRY labels, failed or repeated
phases, and incorrect sampling-independent status values still fail. Operation
spans remain outside complete-coverage scope so expected redelivery after
invocation failure is allowed.

## Short suspended-child coverage

Case 27 replaces the short mode of `otel-long-running-4`. Its child performs
a one-second durable wait, resumes, and returns its input. Exactly one invoke
completes in the caller, while both executions produce PENDING then SUCCEEDED
invocations. Invocation view requires both child wait segments and the link
back to the initial segment. Execution view requires one logical child wait
linked to the resumed child invocation. Both views check the actual child
service carrier against the first invoke context, and require five seconds of
stable telemetry. Child ordering is checked within its own execution; no
comparison between caller and child clocks is needed.
