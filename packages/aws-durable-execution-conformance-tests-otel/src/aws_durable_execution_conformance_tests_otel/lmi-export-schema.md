# Export lifecycle evidence

`validate_export_lifecycle(evidence, phase="observations" | "final")` validates
requirement `lmi-lifecycle-16` under `coverageProfile: export-lifecycle`. It uses
the existing canonical span model and OTel assertions. Call the core common
ownership validator as well. Runtime can be `javascript`, `java`, or `python`;
export mode is an independent choice of processor/exporter configuration.

The matrix has three selectors (`export-success`, `export-failure-recovery`,
`held-export`), two `exportMode` values (`sync`, `async-bounded-flush`), both
`otelView` values, and deployed native concurrency 1 and 2: 24 cells per runtime.
These are controlled, bounded fixture contracts, not product timeout or delivery
SLAs. They do not impose Java owner-thread, HandlerScoped or MDC behavior.

## Immutable configuration and ownership

The normal envelope identifies the run, case, cohort, runtime, tested artifacts,
deployment, every native request and execution, their original selected roles,
and actual service histories. Every handler in these export fixtures succeeds;
the controlled exporter failure must not change its handler/execution outcome.
`selectedRequests.victim` identifies the original request; failure recovery also
selects `warm`, a distinct execution in the same process-owned provider.

`exportLifecycle.budgets` matches the admitted manifest's `behavior_budgets`:

| Budget | Applies to | Meaning |
| --- | --- | --- |
| `exportCompletionMs` | Both modes | Maximum elapsed time from accepting owned export work to its actual completion, including a controlled hold. |
| `observationGraceMs` | Both modes | Collection extends this far beyond every actual wrapper return. |
| `flushWaitMs` | Async mode | Maximum observed duration of the SDK's wait for flush work. It does not bound, complete, or cancel that underlying work. |

All budgets are finite and positive, chosen before measurement. Timestamps use
the envelope's normalized UTC epoch milliseconds. Preserve raw clocks and
normalization evidence. `collection.watermarks.exportLifecycle` must cover
`observationEndedAtMs`; an empty query or missing completion is not quiescence.

Each event retains the common `requestId`, `ownerRequestId`, `executionArn`,
`atMs`, and unique per-request `sequence`. Sequence numbers order actual
semantic boundaries, including boundaries sharing one millisecond; they do not
represent delayed log-emission order. Their order must agree with timestamp
progress. Preserve the original emission order separately in raw evidence. Local
sequence numbers cannot order different requests: an equal-time cross-request
causal dependency needs finer normalized timestamps and otherwise remains
incomplete evidence. Export, flush and gate events also
retain `providerId` and `exporterId`, which must match `telemetry.providerBindings`
and `telemetry.providers`. Providers record their process/environment identity,
`createdAtMs`, and any `shutdownAtMs`. Lazy first-use initialization is allowed;
the provider must exist before its export work and survive invocation completion
and identified reuse. No invocation may shut it down during the measurement.

## Observed boundaries

| Event | Additional fields and meaning |
| --- | --- |
| `wrapper_return` | Actual SDK wrapper boundary and `outcome: SUCCEEDED`; not a root-handler return or a later diagnostic timestamp. |
| `export_started` | `exportId`: the configured processor accepts owned export work, including queuing in async mode. Observe acceptance rather than deriving it from a later successful span. |
| `export_completed` | Same `exportId` and immutable owner, `outcome: succeeded | failed`; failures include the observed `error`. This is actual work completion, not a timeout signal. |
| `flush_started` | `flushId`, and `pendingExportIds` identifying known case-owned work pending when actual flush work starts. |
| `flush_wait_completed` | Same `flushId`, `outcome: completed | timeout | error`, and observed `pendingExportIds` when the SDK's bounded wait returns. An error includes observed `error` details. |
| `flush_completed` | Same `flushId`, `outcome: succeeded | failed`, and `pendingExportIds` when the underlying flush work actually settles. This may occur after a timed-out wait and wrapper return. |
| `export_held` | `exportId` and case-owned `gateId` observed while that work is held. |
| `export_released` | Same IDs and `mode: normal`; the time must match `faultsCreated[].releasedAtMs`. Emergency release cannot satisfy measurement assertions. |

Flush completion is an observable operation result, not a prescribed future,
promise or Java object type. Its pending IDs describe known case-owned exports;
they do not assert that an entire shared provider is idle. Every invocation's
owned export work is accepted before its final flush starts. Acceptance is
distinct from the time a background exporter begins its I/O.

Sync mode requires every export to complete before actual wrapper return. Flush
records are optional and cannot substitute for completion of a synchronous
export blocked earlier in `span.end()`. A later flush timeout cannot bound or
interrupt that earlier call.

Async mode records both the bounded wait and actual flush completion. A
successful wait cannot report completion while its known work remains pending.
A timed-out wait leaves ownership intact until actual completion is observed.
For `held-export`, the SDK wait must time out and the wrapper return while the
original export remains held. Only then does the case release its gate, observe
successful export completion, and observe the underlying flush work settle.
This same policy applies to asynchronous configurations in every runtime.

For `export-failure-recovery`, the victim's exports actually fail, while its
handler still succeeds. A later distinct invocation in the original process and
provider must export successfully. A replacement process, service retry or
earlier healthy invocation cannot establish recovery. Unexpected failure or
missing eventual collection remains a failure; delivery after timeout is not
unconditionally promised.

## Canonical telemetry and retirement

`telemetry.views[otelView]` contains the ordinary cohort `collection`, genuine
runtime `carriers` with explicit sampled expectations, and canonical `spans`.
Each span record includes its independently observed `requestId` and `exportId`
plus the existing `span_to_dict` payload. Keep actual failed export attempts;
their captured spans are evidence of what the exporter received, not claims of
backend delivery. Invocation/Workflow identity, successful execution status and
cohort attribution are mandatory, independent of adapter-supplied assertions.
Every canonical span must end at or before the actual completion of its
identified export work. Use fractional milliseconds or normalized nanoseconds
when needed; no implicit tolerance conceals a span ending after export completed.
Identical logical-span re-exports remain visible without promising exactly-once
transport delivery.

Every export and async flush is included in `resourcesCreated`, with kind
`export` or `flush`, its ID and `ownerCaseId`. Final `retirement.resources` retains
the same resource, actual `outcome: succeeded | failed`, `evidenceUri`, and a
`settledAtMs` at or after actual completion. Wrapper return and a flush wait
timeout never settle export ownership. Both phases require complete controlled
fixture observations; only final validation requires retirement. Final export
retirement is audited independently even if earlier observations are malformed,
so an incomplete collection cannot hide residual work from quarantine handling.
