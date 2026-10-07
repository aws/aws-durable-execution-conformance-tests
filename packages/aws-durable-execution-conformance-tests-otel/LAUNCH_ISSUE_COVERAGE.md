# OTel launch regression coverage

These requirements add normal user spans through each SDK's public APIs. The
handlers read the active span context and create a span without an explicit
parent. They never attach a replacement context, fabricate SDK spans, or invoke
instrumentation hooks themselves.

| Launch work item | Requirements | Regression detected |
| --- | --- | --- |
| 4 — Python status mapping | `otel-invocation-24`, `otel-execution-24` | A genuine invocation retry must report `RETRYING` and `UNSET`, followed by successful recovery. An ordinary step retry reports `PENDING` and does not test this path. |
| 5 — User-function context | `otel-invocation-22..23`, `otel-execution-22..23` | User code must receive valid context on the execution's trace, with the appropriate operation/attempt parent and restoration after nested work and resume. |
| 6 — Completed-operation replay exports | `otel-execution-21` | A completed step before a normal wait/resume must not export its operation or user attempt again. The paired invocation-view case permits that view's distinct replay segments. |

The status case deliberately does not reject duplicate operation delivery after
an invocation failure. That is expected recovery behavior, separate from a normal
successful suspension and resume.

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

The Python status fix also covers non-success operation-end records without an
error object. Public callback failure with omitted error details is a candidate
cloud scenario, but the current local testing service does not faithfully expose
that null-error operation-end path. That branch keeps focused plugin contract
coverage; case 24 is the verified public invocation-retry conformance path.

New requirements and SDK handler updates should be reviewed together. Configure
`conformance_test_ref` to the new requirement commit when validating the handler
PRs; an older pinned requirement revision filters out the new handlers and is not
coverage evidence.


Case 21 now requires five seconds of valid, unchanged normalized telemetry before
passing. Late duplicate records reset or invalidate that candidate. Handler-root
fallbacks reject probe spans tagged with `conformance.callback`, and invocation
replay checks the required link shape for each allowed segment count.

The self-test workflow pins the Python companion fixture from SDK PR #758 so all
24 cases are exercised before the new handlers reach SDK main. Runtime
prerequisites are the focused fixes #752 and #756. Manual dispatch can override
that revision; the `failed+uncovered` coverage threshold is unchanged.

Case 24 covers every execution-correlated Invocation span and requires five
seconds of stable telemetry. The normal lifecycle is RETRYING/UNSET followed by
SUCCEEDED/OK. Recovering an interrupted at-most-once step can additionally
checkpoint one retry timer, yielding a non-first PENDING/OK invocation between
those phases. Both two- and three-invocation sequences are checked in full,
including order and status mappings. Legacy RETRY labels, failed or repeated
phases, and incorrect sampling-independent status values still fail. Operation
spans remain outside complete-coverage scope so expected redelivery after
invocation failure is allowed.
