# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Scheduling and fault isolation tests, without an AWS deployment."""

import asyncio
import contextlib
import copy
import inspect
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from runpy import run_path
from typing import Any, Literal

import pytest

from aws_durable_execution_conformance_tests.lmi.adjudication import adjudicate_evidence
from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Budgets,
    Profile,
    ResourceScheduler,
    Scenario,
    ScenarioContext,
    ScenarioResult,
    run_scenarios,
)
from aws_durable_execution_conformance_tests.lmi.evidence import EvidenceValidator


async def run_and_adjudicate(
    scenarios: Sequence[Scenario],
    profiles: Sequence[Profile],
    adapter: Any,
    *,
    validator: EvidenceValidator | None = None,
    **options: Any,
) -> list[ScenarioResult]:
    """Collect with the coordinator, then adjudicate each capture.

    The coordinator reports only collection outcomes. Tests that assert a
    verdict adjudicate each capture with ``lmi.adjudication.adjudicate_evidence``;
    ``validator=None`` selects the shared standard rules.
    """
    outcomes = await run_scenarios(scenarios, profiles, adapter, **options)
    standard = {case.case_id: case.coverage_profile == "standard" for case in scenarios}
    for result in outcomes:
        if result.evidence:
            result.status, result.errors, result.evidence = adjudicate_evidence(
                result.status,
                result.errors,
                result.evidence,
                retirement_evidence=result.retirement_evidence,
                cleanup_evidence=result.cleanup_evidence,
                observation_evidence=result.observation_evidence,
                attempts=result.attempts,
                validator=validator,
                standard=standard[result.case_id],
                trusted_observations=result.trusted_observations,
            )
    return outcomes


PROFILE = Profile("native-2", "arn:aws:lambda:us-west-2:123:function:lmi:1", 2, 2, "sdk-sha", "config-sha")


def case(name, *, profile=PROFILE, **kwargs):
    return Scenario(name, "lmi-lifecycle-1", profile.profile_id, "overlap", **kwargs)


def validator(evidence, *, phase="final"):
    errors = list(evidence.get("assertionErrors", []))
    if phase == "final" and "retirement" not in evidence:
        errors.append("Missing retirement evidence")
    return errors


class Adapter:
    def __init__(self):
        self.preparations = []
        self.finished = []
        self.started = []
        self.released = []
        self.retired = []
        self.live = set()
        self.max_live = 0

    async def prepare(self, profile):
        self.preparations.append(profile.profile_id)

    async def finish(self, profile, *, quiescent):
        self.finished.append((profile.profile_id, quiescent))

    async def start(self, context):
        self.started.append(context.scenario.case_id)
        for role in context.scenario.roles:
            request_id = f"{context.scenario.case_id}:{role}"
            context.record_attempt(request_id, role=role, requestId=request_id)
        self.live.add(context.scenario.case_id)
        self.max_live = max(self.max_live, len(self.live))

    async def place(self, context):
        await asyncio.sleep(0)

    async def execute(self, context):
        await asyncio.sleep(0.005)

    async def collect(self, context):
        name = context.scenario.case_id
        attempts = []
        events = []
        histories = []
        resources: list[dict[str, Any]] = []
        for journal in context.attempts:
            request = journal["requestId"]
            arn = f"arn:execution:{request}"
            attempts.append(
                {
                    **{key: journal[key] for key in ("requestId", "role", "attempt")},
                    "executionArn": arn,
                    "environmentId": "env",
                    "workerId": "worker",
                    "processId": "process",
                    "startedAtMs": 10,
                    "endedAtMs": 50,
                    "outcome": "SUCCEEDED",
                }
            )
            histories.append({"executionArn": arn, "ownerCaseId": name, "status": "SUCCEEDED", "operations": []})
            resources.extend(
                (
                    {"kind": "invocation", "id": request, "ownerCaseId": name},
                    {"kind": "execution", "id": arn, "ownerCaseId": name},
                )
            )
            for sequence, (kind, time, details) in enumerate(
                (
                    ("wrapper_enter", 10, {}),
                    ("task_start", 11, {"taskId": "root"}),
                    ("task_exit", 49, {"taskId": "root"}),
                    ("wrapper_return", 50, {"outcome": "SUCCEEDED"}),
                )
            ):
                events.append(
                    {
                        "requestId": request,
                        "ownerRequestId": request,
                        "executionArn": arn,
                        "kind": kind,
                        "atMs": time,
                        "sequence": sequence,
                        **details,
                    }
                )
        return {
            "schemaVersion": 1,
            "runtime": "python",
            "coverageProfile": context.scenario.coverage_profile,
            "runId": context.run_id,
            "caseId": context.scenario.case_id,
            "requirementId": context.scenario.requirement_id,
            "profile": {
                "functionArn": context.profile.resource_id,
                "configurationDigest": context.profile.configuration_id,
                "nativeConcurrency": context.profile.native_concurrency,
                "timeoutMs": 100,
            },
            "artifact": {"sdk": context.profile.artifact_id, "conformance": "fixture"},
            "selectedRequests": {item["role"]: item["requestId"] for item in context.attempts if item["attempt"] == 1},
            "attempts": attempts,
            "events": events,
            "histories": histories,
            "resourcesCreated": resources,
            "faultsCreated": [],
            "sdkRequests": [],
            "observationEndedAtMs": 70,
            "collection": {
                "evidenceUris": ["memory://observations"],
                "watermarks": dict.fromkeys(("lifecycle", "sdkRequests", "history"), 70),
            },
        }

    async def release(self, context):
        self.released.append(context.scenario.case_id)

    async def retire(self, context):
        self.retired.append(context.scenario.case_id)
        self.live.discard(context.scenario.case_id)
        context.evidence["retirement"] = {
            "ownerCaseId": context.scenario.case_id,
            "retiredAtMs": 80,
            "faults": [],
            "resources": [
                {
                    **resource,
                    "outcome": "QUIESCENT" if resource["kind"] == "invocation" else "SUCCEEDED",
                    "settledAtMs": 75,
                    "evidenceUri": "memory://retirement",
                }
                for resource in context.evidence.get("resourcesCreated", [])
            ],
        }
        return True


def run(cases, adapter, *, profiles=(PROFILE,), **kwargs):
    return asyncio.run(run_and_adjudicate(cases, profiles, adapter, run_id="run-1", validator=validator, **kwargs))


def test_shared_cases_overlap_prepare_once_and_keep_case_owned_cleanup():
    adapter = Adapter()
    results = run([case("one"), case("two"), case("three")], adapter)
    assert [result.status for result in results] == ["PASSED"] * 3
    assert adapter.max_live == 2
    assert adapter.preparations == [PROFILE.profile_id]
    assert sorted(adapter.released) == ["one", "three", "two"]
    assert sorted(adapter.retired) == ["one", "three", "two"]
    assert adapter.finished == [(PROFILE.profile_id, True)]
    assert all(len(result.attempts) == 1 for result in results)
    for result in results:
        assert set(result.timings) == {
            "admission",
            "provisioning",
            "lease",
            "placement",
            "execution",
            "collection",
            "release",
            "retirement",
            "profileRetirement",
        }
        assert result.timings["release"]["startedAtMs"] >= result.timings["collection"]["finishedAtMs"]


def test_exclusive_waiter_blocks_later_shared_work_and_updates():
    async def exercise():
        scheduler = ResourceScheduler()
        scheduler.register("function", 2)
        first = await scheduler.acquire("function", "first")
        order = []
        release_exclusive = asyncio.Event()

        async def exclusive():
            lease = await scheduler.acquire("function", "exclusive", exclusive=True)
            order.append("exclusive")
            await release_exclusive.wait()
            await lease.release(quiescent=True)

        async def later():
            lease = await scheduler.acquire("function", "later")
            order.append("later")
            await lease.release(quiescent=True)

        waiting_exclusive = asyncio.create_task(exclusive())
        await asyncio.sleep(0)
        waiting_shared = asyncio.create_task(later())
        await asyncio.sleep(0)
        assert order == []
        await first.release(quiescent=True)
        await asyncio.sleep(0)
        assert order == ["exclusive"]
        release_exclusive.set()
        await asyncio.gather(waiting_exclusive, waiting_shared)
        assert order == ["exclusive", "later"]

    asyncio.run(exercise())


def test_suspended_owner_keeps_deployment_excluded_but_releases_capacity():
    async def exercise():
        scheduler = ResourceScheduler()
        scheduler.register("function", 1)
        suspended = await scheduler.acquire("function", "suspended")
        await suspended.release_capacity()
        peer = await scheduler.acquire("function", "peer")
        update = asyncio.create_task(scheduler.acquire("function", "update", exclusive=True))
        await asyncio.sleep(0)
        assert not update.done()
        await peer.release(quiescent=True)
        # An existing owner may resume despite the queued update; the update
        # must continue waiting until that owner's durable work is retired.
        await suspended.reserve_capacity(timeout=0.1)
        assert not update.done()
        await suspended.release(quiescent=True)
        lease = await update
        await lease.release(quiescent=True)

    asyncio.run(exercise())


def test_scenario_concurrency_exceeds_native_capacity_during_suspension():
    profile = replace(PROFILE, capacity=1, native_concurrency=1)

    class SuspendedAdapter(Adapter):
        async def execute(self, context):
            await context.release_capacity()
            while len(self.started) < 3:
                await asyncio.sleep(0)
            await context.reserve_capacity()

    adapter = SuspendedAdapter()
    results = run([case(str(index), profile=profile) for index in range(3)], adapter, profiles=[profile])
    assert all(result.status == "PASSED" for result in results)
    assert adapter.max_live == 3


SERIAL_BUDGETS = Budgets(execution=5, lease=5, placement=5, collection=5, release=5, retirement=5, provisioning=5)


@pytest.mark.parametrize("exclusive", [False, True])
def test_suspended_owner_resumes_while_another_case_waits(exclusive):
    """An admitted owner never has to acquire a work permit again.

    Under serial execution the queued case holds the only permit. A shared
    waiter needs the owner to free capacity and an exclusive waiter needs every
    owner to retire, so if resuming had to reacquire a permit the owner and the
    waiter would wait on each other until the execution budget quarantined the
    target.
    """
    profile = replace(PROFILE, capacity=1, native_concurrency=1)

    class SuspendingAdapter(Adapter):
        async def execute(self, context):
            if context.scenario.case_id != "suspended":
                return
            await context.release_capacity()
            # Give the waiter time to queue for the permit and the lease.
            await asyncio.sleep(0.05)
            await context.reserve_capacity(timeout=2)

    adapter = SuspendingAdapter()
    results = run(
        [case("suspended", profile=profile), case("waiter", profile=profile, exclusive=exclusive)],
        adapter,
        profiles=[profile],
        scenario_concurrency=1,
        budgets=SERIAL_BUDGETS,
    )
    assert [result.status for result in results] == ["PASSED", "PASSED"], results
    assert adapter.retired == ["suspended", "waiter"]


def test_a_case_waiting_for_a_work_permit_holds_no_native_capacity():
    """Admission and resumption must acquire in one order: permit, then capacity.

    Resumption has to take the permit first, so admission must too. Otherwise a
    queued case can hold the slot a resuming owner needs while that owner holds
    the permit the queued case waits for, and the two never make progress.
    """
    first = replace(PROFILE, profile_id="p1", resource_id="fn-1", capacity=1, native_concurrency=1)
    second = replace(PROFILE, profile_id="p2", resource_id="fn-2", capacity=1, native_concurrency=1)
    scheduler = ResourceScheduler()
    waiting_owners = []

    class HoldingAdapter(Adapter):
        async def execute(self, context):
            if context.scenario.profile_id != "p1":
                return
            # The other case cannot run while this one holds the only permit, so
            # it must still be queued for that permit and own no capacity.
            await asyncio.sleep(0.05)
            waiting_owners.append(sorted(scheduler._resources["fn-2"].owners))

    results = run(
        [case("first", profile=first), case("second", profile=second)],
        HoldingAdapter(),
        profiles=[first, second],
        scenario_concurrency=1,
        scheduler=scheduler,
        budgets=SERIAL_BUDGETS,
    )
    assert [result.status for result in results] == ["PASSED", "PASSED"], results
    assert waiting_owners == [[]], waiting_owners


def test_cancelled_waiter_does_not_block_later_admission():
    async def exercise():
        scheduler = ResourceScheduler()
        scheduler.register("function", 1)
        first = await scheduler.acquire("function", "first")
        cancelled = asyncio.create_task(scheduler.acquire("function", "cancelled", exclusive=True))
        await asyncio.sleep(0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await first.release(quiescent=True)
        last = await scheduler.acquire("function", "last", timeout=0.1)
        await last.release(quiescent=True)

    asyncio.run(exercise())


def test_unretired_case_quarantines_only_its_target():
    other = replace(PROFILE, profile_id="other", resource_id="other-function")

    class LeakingAdapter(Adapter):
        async def retire(self, context):
            await super().retire(context)
            return context.profile.profile_id == "other"

    adapter = LeakingAdapter()
    scheduler = ResourceScheduler()
    results = run(
        [case("bad", exclusive=True), case("queued"), case("healthy", profile=other)],
        adapter,
        profiles=[PROFILE, other],
        scheduler=scheduler,
    )
    assert results[0].status == "INFRASTRUCTURE_ERROR"
    assert results[1].status == "INFRASTRUCTURE_ERROR"
    assert results[2].status == "PASSED"
    assert "queued" not in adapter.started
    assert scheduler.quarantine_reason(PROFILE.resource_id)
    assert scheduler.quarantine_reason(other.resource_id) is None
    assert sorted(adapter.finished) == [(PROFILE.profile_id, False), ("other", True)]


def test_timeout_still_releases_and_retires_but_cannot_pass():
    class StalledAdapter(Adapter):
        async def execute(self, context):
            await asyncio.sleep(60)

    adapter = StalledAdapter()
    scheduler = ResourceScheduler()
    result = run([case("deadline")], adapter, budgets=Budgets(execution=0.01), scheduler=scheduler)[0]
    assert result.status == "INFRASTRUCTURE_ERROR"
    assert any("execution exceeded" in error for error in result.errors)
    assert result.attempts[0]["requestId"] == "deadline:victim"
    assert adapter.released == adapter.retired == ["deadline"]
    assert scheduler.quarantine_reason(PROFILE.resource_id)
    assert adapter.finished == [(PROFILE.profile_id, False)]


def test_emergency_release_cannot_overwrite_observed_failure():
    class CleanupAdapter(Adapter):
        async def collect(self, context):
            evidence = await super().collect(context)
            evidence["assertionErrors"] = ["Worker did not recover before fault release"]
            return evidence

        async def release(self, context):
            context.evidence["assertionErrors"] = []
            await super().release(context)

    result = run([case("victim")], CleanupAdapter())[0]
    assert result.status == "FAILED"
    assert "Worker did not recover before fault release" in result.errors
    assert result.evidence["assertionErrors"] == ["Worker did not recover before fault release"]


def test_original_request_cannot_be_rebound_and_dispatch_is_stable():
    context = ScenarioContext("run", case("victim"), PROFILE)
    assert context.dispatcher("victim") == {
        "scenario": "overlap",
        "runId": "run",
        "caseId": "victim",
        "role": "victim",
    }
    context.record_attempt("launch")
    context.update_attempt("launch", requestId="original")
    with pytest.raises(ValueError, match="original"):
        context.update_attempt("launch", requestId="retry")
    with pytest.raises(ValueError, match="Unknown cohort"):
        context.dispatcher("unknown")


def test_missing_fixture_is_visible_and_retires():
    class MissingAdapter(Adapter):
        async def start(self, context):
            raise NotImplementedError("native overlap")

    adapter = MissingAdapter()
    result = run([case("missing")], adapter)[0]
    assert result.status == "NOT_IMPLEMENTED"
    assert adapter.retired == ["missing"]


def test_cancelling_run_retires_owners_before_finishing_profile():
    async def exercise():
        entered = asyncio.Event()

        class BlockingAdapter(Adapter):
            async def execute(self, context):
                entered.set()
                await asyncio.sleep(60)

        adapter = BlockingAdapter()
        scheduler = ResourceScheduler()
        running = asyncio.create_task(
            run_and_adjudicate(
                [case("victim")], [PROFILE], adapter, run_id="cancelled", validator=validator, scheduler=scheduler
            )
        )
        await entered.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert adapter.retired == ["victim"]
        assert adapter.finished == [(PROFILE.profile_id, False)]
        assert scheduler.quarantine_reason(PROFILE.resource_id)

    asyncio.run(exercise())


def test_cancellation_at_lease_acquisition_does_not_strand_resource_ownership():
    async def exercise():
        class UnstartedAdapter(Adapter):
            async def start(self, context):
                await asyncio.Event().wait()

        adapter = UnstartedAdapter()
        scheduler = ResourceScheduler()
        emitted: list[ScenarioResult] = []
        running = asyncio.create_task(
            run_and_adjudicate(
                [case("victim")],
                [PROFILE],
                adapter,
                run_id="cancelled",
                validator=validator,
                scheduler=scheduler,
                on_result=emitted.append,
            )
        )
        # Observe the real scheduler's handoff without changing its behavior.
        # The old child-task wrapper left this owner behind when cancelled here.
        async with asyncio.timeout(1):
            while (
                PROFILE.resource_id not in scheduler._resources
                or "cancelled:victim" not in scheduler._resources[PROFILE.resource_id].owners
            ):
                await asyncio.sleep(0)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert adapter.started == []
        assert adapter.released == adapter.retired == ["victim"]
        assert emitted[-1].status == "INFRASTRUCTURE_ERROR"
        assert "lease" in emitted[-1].timings

        # The cancelled owner's lease was released rather than stranded.
        assert scheduler._resources[PROFILE.resource_id].owners == {}

    asyncio.run(exercise())


def test_case_lease_timeout_retains_phase_error_and_timing_without_quarantine():
    async def exercise():
        waiter_finished = asyncio.Event()

        class HoldingAdapter(Adapter):
            async def execute(self, context):
                await waiter_finished.wait()

        def on_result(result):
            if result.case_id == "waiting":
                waiter_finished.set()

        adapter = HoldingAdapter()
        results = await run_and_adjudicate(
            [case("holding", exclusive=True), case("waiting")],
            [PROFILE],
            adapter,
            run_id="run",
            validator=validator,
            budgets=Budgets(lease=0.01, execution=1),
            on_result=on_result,
        )
        assert results[0].status == "PASSED", results[0].errors
        assert results[1].status == "INFRASTRUCTURE_ERROR"
        assert results[1].errors == ["PhaseTimeout: lease exceeded its 0.01s harness budget"]
        assert adapter.started == ["holding"]
        assert adapter.finished == [(PROFILE.profile_id, True)]
        for result in results:
            timing = result.timings["lease"]
            assert timing["finishedAtMs"] >= timing["startedAtMs"]
            assert timing["durationMs"] >= 0

    asyncio.run(exercise())


@pytest.mark.parametrize("budget", [0, -1, float("nan"), float("inf"), True])
def test_invalid_budgets_rejected(budget):
    with pytest.raises(ValueError):
        Budgets(execution=budget)


def test_missing_retirement_evidence_cannot_grant_next_exclusive_window():
    class UnprovenAdapter(Adapter):
        async def retire(self, context):
            return True

    adapter = UnprovenAdapter()
    results = run([case("unproven", exclusive=True), case("next", exclusive=True)], adapter)
    assert all(result.status == "INFRASTRUCTURE_ERROR" for result in results)
    assert adapter.started == ["unproven"]
    assert adapter.finished == [(PROFILE.profile_id, False)]


def test_failed_fault_release_still_attempts_retirement():
    class ReleaseFailure(Adapter):
        async def release(self, context):
            raise RuntimeError("control backend unavailable")

    adapter = ReleaseFailure()
    result = run([case("victim")], adapter)[0]
    assert result.status == "INFRASTRUCTURE_ERROR"
    assert adapter.retired == ["victim"]
    assert adapter.finished == [(PROFILE.profile_id, False)]


def test_failed_profile_preparation_does_not_block_other_profile():
    other = replace(PROFILE, profile_id="other", resource_id="other-function")

    class PreparationFailure(Adapter):
        async def prepare(self, profile):
            if profile.profile_id == PROFILE.profile_id:
                raise RuntimeError("deployment failed")
            await super().prepare(profile)

    adapter = PreparationFailure()
    results = run([case("blocked"), case("healthy", profile=other)], adapter, profiles=[PROFILE, other])
    assert results[0].status == "INFRASTRUCTURE_ERROR"
    assert results[1].status == "PASSED"
    assert adapter.started == ["healthy"]
    assert sorted(adapter.finished) == [(PROFILE.profile_id, False), ("other", True)]


@pytest.mark.parametrize("finish_failure", ["raise", "timeout"])
def test_preparation_failure_diagnostics_are_owned_by_each_case(finish_failure):
    class PreparationAndFinishFailure(Adapter):
        async def prepare(self, profile):
            await super().prepare(profile)
            raise RuntimeError("deployment failed")

        async def finish(self, profile, *, quiescent):
            await super().finish(profile, quiescent=quiescent)
            if finish_failure == "raise":
                raise RuntimeError("lock reconciliation failed")
            await asyncio.Event().wait()

    adapter = PreparationAndFinishFailure()
    reported = []
    results = run(
        [case("first"), case("second"), case("third")],
        adapter,
        budgets=Budgets(retirement=0.02),
        on_result=lambda result: reported.append(copy.deepcopy(result)),
    )
    final_reports = {result.case_id: result for result in reported}
    assert len(results) == len(final_reports) == 3
    assert adapter.preparations == [PROFILE.profile_id]
    assert adapter.finished == [(PROFILE.profile_id, False)]
    for result in (*results, *final_reports.values()):
        assert result.status == "INFRASTRUCTURE_ERROR"
        assert sum("deployment failed" in error for error in result.errors) == 1
        assert sum("Run ownership release failed" in error for error in result.errors) == 1


def test_external_run_lock_release_failure_cannot_report_pass():
    class FinishFailure(Adapter):
        async def finish(self, profile, *, quiescent):
            raise RuntimeError("lock reconciliation failed")

    result = run([case("victim")], FinishFailure())[0]
    assert result.status == "INFRASTRUCTURE_ERROR"
    assert any("lock reconciliation" in error for error in result.errors)


def test_queued_exclusive_profile_does_not_consume_other_profiles_scenario_slots():
    async def exercise():
        other = replace(PROFILE, profile_id="other", resource_id="other-function")
        victim_running = asyncio.Event()
        other_progressed = asyncio.Event()

        class IndependentAdapter(Adapter):
            async def prepare(self, profile):
                # Let the busy profile finish preparation first, so its two
                # cases reach admission before the idle profile's case. A work
                # budget handed to the queued case here would starve the idle
                # profile, which must stay independent of a blocked target.
                if profile.profile_id == "other":
                    await asyncio.sleep(0.05)
                await super().prepare(profile)

            async def execute(self, context):
                if context.scenario.case_id == "exclusive":
                    victim_running.set()
                    await asyncio.wait_for(other_progressed.wait(), 0.5)
                elif context.scenario.case_id == "independent":
                    await victim_running.wait()
                    other_progressed.set()

        adapter = IndependentAdapter()
        results = await run_and_adjudicate(
            [case("exclusive", exclusive=True), case("queued"), case("independent", profile=other)],
            [PROFILE, other],
            adapter,
            run_id="run",
            validator=validator,
            scenario_concurrency=2,
        )
        assert other_progressed.is_set()
        assert all(result.status == "PASSED" for result in results)
        assert adapter.started.index("independent") < adapter.started.index("queued")

    asyncio.run(exercise())


def test_attempt_sink_runs_before_transport_and_preserves_cancelled_result():
    async def exercise():
        started = asyncio.Event()
        snapshots: list[list[dict[str, Any]]] = []
        emitted = []

        class JournalAdapter(Adapter):
            async def start(self, context):
                context.record_attempt("launch")
                assert snapshots[-1][0]["attemptId"] == "launch"
                assert "requestId" not in snapshots[-1][0]
                context.update_attempt("launch", requestId="original")
                started.set()

            async def execute(self, context):
                await asyncio.sleep(60)

        task = asyncio.create_task(
            run_and_adjudicate(
                [case("victim")],
                [PROFILE],
                JournalAdapter(),
                run_id="run",
                validator=validator,
                on_attempt=lambda context: snapshots.append(copy.deepcopy(context.attempts)),
                on_result=lambda result: emitted.append(copy.deepcopy(result)),
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert snapshots[-1][0]["requestId"] == "original"
        assert emitted[-1].status == "INFRASTRUCTURE_ERROR"
        assert emitted[-1].attempts == snapshots[-1]
        assert "profileRetirement" in emitted[-1].timings

    asyncio.run(exercise())


def test_attempt_journal_failure_prevents_unrecorded_transport_dispatch():
    transported = []

    class JournalAdapter(Adapter):
        async def start(self, context):
            context.record_attempt("launch")
            transported.append("sent")

    def failing_sink(context):
        raise OSError("evidence disk full")

    result = run([case("victim")], JournalAdapter(), on_attempt=failing_sink)[0]
    assert result.status == "INFRASTRUCTURE_ERROR"
    assert transported == []
    assert result.attempts == []


def test_journaled_execution_cannot_be_rebound():
    context = ScenarioContext("run", case("victim"), PROFILE)
    context.record_attempt("launch", requestId="original", executionArn="execution-a")
    context.update_attempt("launch", executionArn="execution-a")
    with pytest.raises(ValueError, match="original durable execution"):
        context.update_attempt("launch", executionArn="execution-b")


def test_withdraw_attempt_removes_undispatched_launch_and_reinvokes_sink():
    publications = []
    context = ScenarioContext("run", case("one"), PROFILE, _on_attempt=lambda ctx: publications.append(ctx.attempts))
    context.record_attempt("launch", source="driver-invoke")
    assert publications[-1][0]["attemptId"] == "launch"
    context.withdraw_attempt("launch")
    assert context.attempts == []
    assert publications[-1] == []


def test_withdraw_attempt_restores_the_attempt_when_the_sink_fails():
    calls: list[list[dict[str, Any]]] = []

    def sink(ctx):
        calls.append(copy.deepcopy(ctx.attempts))
        if len(calls) > 1:
            raise OSError("journal disk full")

    context = ScenarioContext("run", case("one"), PROFILE, _on_attempt=sink)
    context.record_attempt("launch", source="driver-invoke")
    assert len(calls) == 1
    with pytest.raises(OSError, match="journal disk full"):
        context.withdraw_attempt("launch")
    # Memory keeps the attempt even though the sink may already have
    # recorded its removal.
    assert [item["attemptId"] for item in context.attempts] == ["launch"]


def test_withdraw_attempt_refuses_a_non_last_attempt():
    context = ScenarioContext("run", case("cohort", roles=("victim", "peer")), PROFILE)
    context.record_attempt("first", role="victim", source="driver-invoke")
    context.record_attempt("second", role="peer", source="driver-invoke")
    with pytest.raises(ValueError, match="last journaled attempt"):
        context.withdraw_attempt("first")
    assert [item["attemptId"] for item in context.attempts] == ["first", "second"]


def test_withdraw_attempt_refuses_a_non_driver_invoke_source():
    context = ScenarioContext("run", case("one"), PROFILE)
    context.record_attempt("launch", source="poller-replay")
    with pytest.raises(ValueError, match="undispatched driver launch"):
        context.withdraw_attempt("launch")
    assert [item["attemptId"] for item in context.attempts] == ["launch"]


def test_withdraw_attempt_refuses_an_attempt_with_native_identity():
    context = ScenarioContext("run", case("one"), PROFILE)
    context.record_attempt("launch", source="driver-invoke", requestId="request", executionArn="execution")
    with pytest.raises(ValueError, match="undispatched driver launch"):
        context.withdraw_attempt("launch")
    assert [item["attemptId"] for item in context.attempts] == ["launch"]


def test_withdraw_attempt_refuses_a_service_discovered_attempt():
    context = ScenarioContext("run", case("one"), PROFILE)
    context.record_attempt("launch", source="driver-invoke", requestId="original", executionArn="execution-a")
    context.record_service_attempt("retry", original_request_id="original", execution_arn="execution-a", role="victim")
    with pytest.raises(ValueError, match="undispatched driver launch"):
        context.withdraw_attempt("retry")
    assert [item["attemptId"] for item in context.attempts] == ["launch", "retry"]


def test_multi_role_launches_require_immutable_role_and_per_role_order():
    context = ScenarioContext("run", case("cohort", roles=("victim", "peer")), PROFILE)
    with pytest.raises(ValueError, match="role"):
        context.record_attempt("unknown")
    context.record_attempt("victim-original", role="victim")
    context.record_attempt("peer-original", role="peer", attempt=1)
    context.record_attempt("victim-retry", role="victim", attempt=2)
    assert [(item["role"], item["attempt"]) for item in context.attempts] == [("victim", 1), ("peer", 1), ("victim", 2)]
    with pytest.raises(ValueError, match="launch order"):
        context.record_attempt("retry-substitution", role="victim", attempt=1)
    with pytest.raises(ValueError, match="original role"):
        context.update_attempt("victim-original", role="peer")
    with pytest.raises(ValueError, match="original role"):
        context.update_attempt("victim-retry", attempt=1)
    context.update_attempt("victim-original", requestId="request-original")
    assert context.attempts[0]["requestId"] == "request-original"


def test_valid_retry_retains_original_selection_and_all_attempts():
    class RetryingAdapter(Adapter):
        async def start(self, context):
            await super().start(context)
            context.record_attempt("retry", requestId="later-retry")

    result = run([case("victim")], RetryingAdapter())[0]
    assert result.status == "PASSED"
    assert result.evidence["selectedRequests"] == {"victim": "victim:victim"}
    assert [item["attempt"] for item in result.attempts] == [1, 2]


def test_cancellation_during_preparation_reports_waiting_cases():
    # Cases waiting for their target's preparation are part of the run's
    # audit record, so cancellation must still publish a result for them.
    reported: list[ScenarioResult] = []
    entered = asyncio.Event()

    class BlockedPreparation(Adapter):
        async def prepare(self, profile):
            entered.set()
            await asyncio.Event().wait()

    async def exercise():
        run = asyncio.create_task(
            run_and_adjudicate(
                [case("waiting")],
                [PROFILE],
                BlockedPreparation(),
                run_id="run",
                validator=validator,
                on_result=reported.append,
            )
        )
        await entered.wait()
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run

    asyncio.run(exercise())
    assert [result.case_id for result in reported][:1] == ["waiting"], reported
    assert reported[0].status == "INFRASTRUCTURE_ERROR"
    assert any("preparation" in error for error in reported[0].errors), reported[0].errors


def test_single_run_cancellation_allows_slower_case_retirement_to_finish():
    async def exercise():
        all_started = asyncio.Event()
        slow_retiring = asyncio.Event()

        class UnevenRetirement(Adapter):
            def __init__(self):
                super().__init__()
                self.completed_retirements = []

            async def execute(self, context):
                if len(self.started) == 2:
                    all_started.set()
                await asyncio.Event().wait()

            async def retire(self, context):
                if context.scenario.case_id == "slow":
                    slow_retiring.set()
                    await asyncio.sleep(0.03)
                else:
                    await slow_retiring.wait()
                self.completed_retirements.append(context.scenario.case_id)
                return await super().retire(context)

        adapter = UnevenRetirement()
        reported: list[ScenarioResult] = []
        running = asyncio.create_task(
            run_and_adjudicate(
                [case("fast"), case("slow")],
                [PROFILE],
                adapter,
                run_id="cancelled",
                validator=validator,
                budgets=Budgets(retirement=1),
                on_result=reported.append,
            )
        )
        await all_started.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert set(adapter.completed_retirements) == {"fast", "slow"}
        assert {result.case_id for result in reported} == {"fast", "slow"}
        assert not any("retirement: CancelledError" in error for result in reported for error in result.errors)

    asyncio.run(exercise())


def test_result_sink_failure_does_not_interrupt_other_cases():
    def sink(result):
        if result.case_id == "one":
            raise OSError("audit unavailable")

    adapter = Adapter()
    first, other = run([case("one"), case("other")], adapter, on_result=sink)
    assert first.status == "INFRASTRUCTURE_ERROR"
    assert other.status == "PASSED"
    assert sorted(adapter.retired) == ["one", "other"]


def test_independent_retirement_projects_post_window_io_exit_from_events():
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_failure_retirement.py")))
    value = fixtures["_standard_retirement_snapshot"]("evidence", requirement=1)
    victim = next(attempt for attempt in value["attempts"] if attempt["role"] == "victim")
    start = {
        "kind": "io_start",
        "requestId": victim["requestId"],
        "ownerRequestId": victim["requestId"],
        "executionArn": victim["executionArn"],
        "taskId": "pending-io",
        "atMs": victim["startedAtMs"] + 1,
    }
    value["events"].append(start)
    fixtures["_resequence"](value)
    cutoff = value["observationEndedAtMs"]

    def retire(snapshot):
        at = cutoff + 1
        snapshot["events"].append({**start, "kind": "io_exit", "atMs": at})
        snapshot["observationEndedAtMs"] = at
        snapshot["collection"]["watermarks"] = dict.fromkeys(snapshot["collection"]["watermarks"], at)

    result = fixtures["_run_standard_cleanup_discovery"](value, retire, phase="retirement", path="independent")
    assert result.status == "PASSED", result.errors
    from aws_durable_execution_conformance_tests.lmi.adjudication import evaluate_evidence

    assert any(
        event["kind"] == "io_exit" and event.get("taskId") == "pending-io"
        for event in result.retirement_evidence["events"]
    )
    assert all(event["kind"] != "io_exit" for event in result.evidence["events"] if event.get("taskId") == "pending-io")

    def check_projection(
        evidence: Mapping[str, Any], *, phase: Literal["observations", "final"] = "final"
    ) -> list[str]:
        if phase == "final":
            assert evidence["retirement"]["completions"][0]["taskId"] == "pending-io"
        return []

    assert (
        evaluate_evidence(
            result.evidence,
            retirement_evidence=result.retirement_evidence,
            cleanup_evidence=result.cleanup_evidence,
            validator=check_projection,
            standard=True,
        )
        == []
    )


def test_finish_is_skipped_when_target_admission_prevents_prepare():
    async def check():
        scheduler = ResourceScheduler()
        scheduler.register(PROFILE.resource_id, PROFILE.capacity)
        lease = await scheduler.acquire(PROFILE.resource_id, "active")
        adapter = Adapter()
        try:
            result = await run_and_adjudicate(
                [case("one")],
                [PROFILE],
                adapter,
                run_id="run",
                validator=validator,
                scheduler=scheduler,
                budgets=replace(Budgets(), lease=0.01),
            )
            assert result[0].status == "INFRASTRUCTURE_ERROR"
            assert not adapter.preparations and not adapter.finished
            assert scheduler.quarantine_reason(PROFILE.resource_id) is None
        finally:
            await lease.release(quiescent=True)

    asyncio.run(check())


def test_undispatched_settlement_does_not_satisfy_a_prior_frames_native_owner_reference():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _cleanup_record_errors

    earlier = {
        "attempts": [{"requestId": "rejected", "executionArn": "execution"}],
        "events": [
            {
                "requestId": "rejected",
                "ownerRequestId": "rejected",
                "executionArn": "execution",
                "kind": "task_start",
                "atMs": 10,
            }
        ],
    }
    final: dict[str, Any] = {
        "attempts": [{"requestId": "rejected", "executionArn": "execution"}],
        "retirement": {
            "resources": [
                {
                    "kind": "invocation",
                    "id": "rejected",
                    "outcome": "UNDISPATCHED",
                    "settledAtMs": 20,
                    "executionArn": "execution",
                    "serviceErrorType": "TooManyRequestsException",
                    "sourceEventId": 4,
                    "startedAtMs": 5,
                },
                {"kind": "execution", "id": "execution", "outcome": "SUCCEEDED", "settledAtMs": 20},
            ]
        },
    }
    # A proven UNDISPATCHED settlement still needs QUIESCENT and a bound
    # attempt to satisfy an earlier frame's raw reference to the same request;
    # an earlier or later ledger row contradicts the undispatched proof.
    assert any(
        "native owner rejected is not settled" in error for error in _cleanup_record_errors([earlier, final], final)
    )
    quiescent_final = copy.deepcopy(final)
    quiescent_resource = next(item for item in quiescent_final["retirement"]["resources"] if item["id"] == "rejected")
    quiescent_resource["outcome"] = "QUIESCENT"
    assert _cleanup_record_errors([earlier, quiescent_final], quiescent_final) == []


def test_normal_post_window_completion_does_not_require_independent_proof():
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence

    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = fixtures["evidence"]()
    fixtures["complete_sdk_dispatches"](value)
    owner = value["attempts"][0]
    value["events"].append(fixtures["event"](owner, "io_start", 30, taskId="held"))
    value["retirement"]["completions"] = [fixtures["event"](owner, "io_exit", 205, taskId="held")]
    assert validate_evidence(value) == []
    deployment = value["profile"]
    profile = Profile(
        "fixture",
        deployment["functionArn"],
        2,
        deployment["nativeConcurrency"],
        value["artifact"]["sdk"],
        deployment["configurationDigest"],
    )

    class CompletionAdapter(Adapter):
        async def start(self, context):
            for row in value["attempts"]:
                context.record_attempt(row["requestId"], role=row["role"], requestId=row["requestId"])

        async def collect(self, context):
            observed = copy.deepcopy(value)
            observed.pop("retirement")
            return observed

        async def retire(self, context):
            context.evidence["retirement"] = copy.deepcopy(value["retirement"])
            return True

    scenario = Scenario(
        value["caseId"], value["requirementId"], profile.profile_id, "fixture", roles=tuple(value["selectedRequests"])
    )
    results = asyncio.run(
        run_and_adjudicate(
            [scenario],
            [profile],
            CompletionAdapter(),
            run_id=value["runId"],
            validator=lambda evidence, *, phase="final": validate_evidence(
                evidence, phase="observations" if phase == "observations" else "final"
            ),
        )
    )
    assert results[0].status == "PASSED", results[0].errors
    assert results[0].retirement_evidence.get("retirement") == results[0].evidence.get("retirement")


def test_verified_pre_window_completion_releases_target_despite_incomplete_frozen_observations():
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_failure_retirement.py")))
    value = fixtures["_standard_retirement_snapshot"]("evidence", requirement=1)
    owner = value["attempts"][0]
    start = {
        "kind": "io_start",
        "requestId": owner["requestId"],
        "ownerRequestId": owner["requestId"],
        "executionArn": owner["executionArn"],
        "taskId": "held",
        "atMs": 30,
    }
    value["events"].append(start)
    fixtures["_resequence"](value)

    def complete(snapshot):
        snapshot["events"].append({**start, "kind": "io_exit", "atMs": 199})

    result = fixtures["_run_standard_cleanup_discovery"](value, complete, phase="retirement", path="independent")
    assert result.status == "INCOMPLETE_EVIDENCE", result.errors


def test_pending_callback_claim_without_launch_keeps_target_quarantined():
    class CallbackAdapter(Adapter):
        async def start(self, context):
            context.evidence["retirement"] = {
                "resources": [{"id": "callback", "kind": "callback", "outcome": "PENDING", "ownerCaseId": "one"}],
            }
            raise RuntimeError("setup failed")

        async def retire(self, context):
            return True

    scheduler = ResourceScheduler()
    result = run([case("one")], CallbackAdapter(), scheduler=scheduler)[0]
    assert "Owned work has no independent retirement evidence" in result.errors
    assert scheduler.quarantine_reason(PROFILE.resource_id)


def test_adapter_cannot_clear_coordinator_journal_after_dispatch():
    class ClearingAdapter(Adapter):
        async def start(self, context):
            context.record_attempt("first", requestId="request")
            context.attempts.clear()
            raise RuntimeError("transport failed after dispatch")

        async def retire(self, context):
            return True

    scheduler = ResourceScheduler()
    result = run([case("one")], ClearingAdapter(), scheduler=scheduler)[0]
    assert [row["requestId"] for row in result.attempts] == ["request"]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


def test_repeated_cancellation_during_retirement_cannot_strand_profile():
    async def check():
        entered, proceed = asyncio.Event(), asyncio.Event()
        scheduler = ResourceScheduler()

        class SlowRetirement(Adapter):
            async def retire(self, context):
                entered.set()
                await proceed.wait()
                return await super().retire(context)

        adapter = SlowRetirement()
        task = asyncio.create_task(
            run_and_adjudicate(
                [case("one")],
                [PROFILE],
                adapter,
                run_id="run",
                validator=validator,
                scheduler=scheduler,
            )
        )
        async with asyncio.timeout(2):
            await entered.wait()
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            proceed.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        # No lease is stranded, so the target could be deployed or admitted again.
        assert scheduler._resources[PROFILE.resource_id].owners == {}
        assert adapter.finished == [(PROFILE.profile_id, True)]

    asyncio.run(check())


def test_partial_sdk_sequence_does_not_mix_with_an_inherited_closure():
    class PartialSequenceAdapter(Adapter):
        async def start(self, context):
            await super().start(context)
            self.collected = await super().collect(context)
            attempt = self.collected["attempts"][0]
            request = attempt["requestId"]
            self.dispatch = {
                "requestId": request,
                "ownerRequestId": request,
                "executionArn": attempt["executionArn"],
                "method": "CheckpointDurableExecution",
                "outcome": "DISPATCHED",
                "callId": "checkpoint",
                "atMs": 50,
                "sequence": 9,
            }
            self.closure = {**self.collected["events"][-1], "sequence": 10}
            context.evidence = {"sdkRequests": [self.dispatch], "events": [self.closure]}

        async def place(self, context):
            context.evidence = {"sdkRequests": [{**self.dispatch, "sequence": 99}]}

        async def collect(self, context):
            return {
                **self.collected,
                "sdkRequests": [{**self.dispatch, "sequence": 99}],
                "events": [
                    *self.collected["events"][:-1],
                    {**self.dispatch, "kind": "checkpoint_call", "sequence": 97},
                    {**self.dispatch, "kind": "checkpoint_exit", "sequence": 98},
                    {**self.closure, "sequence": 100},
                ],
            }

    result = run([case("one")], PartialSequenceAdapter())[0]
    assert result.status == "PASSED", result.errors


def test_prelaunch_control_can_retire_without_native_records():
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_failure_retirement.py")))
    control = {"id": "control", "kind": "control", "ownerCaseId": "failed"}

    class PrelaunchAdapter(Adapter):
        async def start(self, context):
            if context.scenario.case_id != "failed":
                await super().start(context)
                return
            proof = fixtures["_snapshot"](context)
            for field in ("attempts", "events", "histories", "sdkRequests", "faultsCreated"):
                proof[field] = []
            proof["selectedRequests"] = {}
            proof["resourcesCreated"] = [control]
            proof["retirement"].update(
                resources=[{**control, "outcome": "DELETED", "settledAtMs": 20900, "evidenceUri": "memory://proof"}],
                faults=[],
            )
            self.proof = proof
            context.evidence = {"resourcesCreated": [control]}
            raise RuntimeError("setup failed before launch")

        async def retire(self, context):
            if context.scenario.case_id != "failed":
                return await super().retire(context)
            context.retirement_evidence = self.proof
            return True

    adapter = PrelaunchAdapter()
    results, scheduler = fixtures["_run"](adapter)
    assert results[0].status == "INFRASTRUCTURE_ERROR"
    assert results[1].status == "PASSED", results[1].errors
    assert adapter.finished == [(fixtures["PROFILE"].profile_id, True)]
    assert scheduler.quarantine_reason(fixtures["PROFILE"].resource_id) is None


def test_cancelled_drain_does_not_spin_when_finishing_profile():
    async def check():
        finishing = asyncio.Event()

        class BlockingFinish(Adapter):
            async def finish(self, profile, *, quiescent):
                finishing.set()
                await asyncio.Event().wait()

        running = asyncio.create_task(
            run_and_adjudicate(
                [case("one")],
                [PROFILE],
                BlockingFinish(),
                run_id="run",
                validator=validator,
            )
        )
        async with asyncio.timeout(2):
            await finishing.wait()
            drain = next(task for task in asyncio.all_tasks() if task.get_name() == "drain_and_finish")
            running.cancel()
            drain.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running

    asyncio.run(check())


def test_nested_attempt_diagnostics_do_not_alias_caller_data():
    publications = []
    context = ScenarioContext("run", case("one"), PROFILE, _on_attempt=lambda ctx: publications.append(ctx.attempts))
    initial = {"200": 1}
    context.record_attempt("launch", statusCounts=initial)
    initial.clear()
    assert context.attempts[0]["statusCounts"] == {"200": 1}
    updated = {"202": 2}
    context.update_attempt("launch", statusCounts=updated)
    updated.clear()
    assert context.attempts == publications[-1]
    assert context.attempts[0]["statusCounts"] == {"202": 2}


def test_released_lease_cannot_alter_capacity_of_reacquired_owner():
    async def check():
        scheduler = ResourceScheduler()
        scheduler.register("function", 1)
        old = await scheduler.acquire("function", "owner")
        await old.release(quiescent=True)
        current = await scheduler.acquire("function", "owner")
        with pytest.raises(RuntimeError, match="has been released"):
            await old.release_capacity()
        with pytest.raises(RuntimeError, match="has been released"):
            await old.reserve_capacity()
        await old.release(quiescent=False)
        peer = asyncio.create_task(scheduler.acquire("function", "peer"))
        await asyncio.sleep(0)
        assert not peer.done()
        await current.release(quiescent=True)
        await (await peer).release(quiescent=True)

    asyncio.run(check())


def test_adapter_operation_cancellation_does_not_abort_other_profiles():
    class CancellingAdapter(Adapter):
        async def execute(self, context):
            if context.scenario.case_id == "cancelled":
                cancelled = asyncio.get_running_loop().create_future()
                cancelled.cancel()
                await cancelled
            else:
                await super().execute(context)

    other = replace(PROFILE, profile_id="other", resource_id="other-function")
    adapter = CancellingAdapter()
    scheduler = ResourceScheduler()
    results = run(
        [case("cancelled"), case("healthy", profile=other)], adapter, profiles=(PROFILE, other), scheduler=scheduler
    )
    assert results[0].status == "INFRASTRUCTURE_ERROR"
    assert "RuntimeError: execution operation was cancelled" in results[0].errors
    assert "A bounded phase was interrupted; residual work requires reconciliation" in results[0].errors
    assert results[1].status == "PASSED"
    # The cancelled phase may have left work running, so no target is quiescent.
    assert scheduler.quarantine_reason(PROFILE.resource_id)
    assert sorted(adapter.finished) == [("native-2", False), ("other", False)]


def test_cancelled_drain_allows_case_retirement_and_finishes_profile():
    async def check():
        executing = asyncio.Event()
        retiring = asyncio.Event()
        retired = asyncio.Event()

        class BlockingRetirement(Adapter):
            async def execute(self, context):
                executing.set()
                await asyncio.Event().wait()

            async def retire(self, context):
                retiring.set()
                await retired.wait()
                return await super().retire(context)

        adapter = BlockingRetirement()
        scheduler = ResourceScheduler()
        running = asyncio.create_task(
            run_and_adjudicate(
                [case("one")], [PROFILE], adapter, run_id="run", validator=validator, scheduler=scheduler
            )
        )
        async with asyncio.timeout(2):
            await executing.wait()
            running.cancel()
            await retiring.wait()
            drain = next(task for task in asyncio.all_tasks() if task.get_name() == "drain_and_finish")
            drain.cancel()
            await asyncio.sleep(0)
            assert not running.done()
            retired.set()
            with pytest.raises(asyncio.CancelledError):
                await running
            assert adapter.finished == [(PROFILE.profile_id, False)]

    asyncio.run(check())


def test_service_attempt_keeps_native_identity_when_audit_fails():
    def audit(context):
        if context.attempts[-1].get("discoveredAtMs"):
            raise OSError("disk full")

    context = ScenarioContext("run", case("one"), PROFILE, _on_attempt=audit)
    context.record_attempt("first", requestId="original", executionArn="first-execution")
    context.record_attempt("second", requestId="second", executionArn="second-execution")
    with pytest.raises(OSError, match="disk full"):
        context.record_service_attempt(
            "retry", original_request_id="second", execution_arn="second-execution", role="victim"
        )
    assert [item["requestId"] for item in context.attempts] == ["original", "second", "retry"]


def test_interrupted_adapter_task_keeps_every_profile_non_quiescent():
    async def check():
        other = replace(PROFILE, profile_id="other", resource_id="arn:other")
        finishing = asyncio.Event()
        launched_late = asyncio.Event()

        class CancellationResistant(Adapter):
            async def execute(self, context):
                if context.profile != other:
                    return await super().execute(context)
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    # Ignore cancellation and keep launching work after the
                    # run barrier has passed.
                    await finishing.wait()
                    context.record_attempt("late", requestId="late-request", executionArn="arn:execution:slow:late")
                    launched_late.set()

            async def finish(self, profile, *, quiescent):
                finishing.set()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(launched_late.wait(), 0.5)
                await super().finish(profile, quiescent=quiescent)

        adapter = CancellationResistant()
        await run_scenarios(
            [case("fast"), case("slow", profile=other)],
            [PROFILE, other],
            adapter,
            run_id="capture",
            budgets=Budgets(execution=0.05),
        )
        assert launched_late.is_set()
        assert set(adapter.finished) == {(PROFILE.profile_id, False), (other.profile_id, False)}

    asyncio.run(check())


def test_any_interrupted_adapter_phase_keeps_every_profile_non_quiescent():
    async def check():
        other = replace(PROFILE, profile_id="other", resource_id="arn:other")

        class SlowToCancel(Adapter):
            async def execute(self, context):
                if context.profile != other:
                    return await super().execute(context)
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    # Finish soon after cancellation; work it started may still be running.
                    await asyncio.sleep(0)

        adapter = SlowToCancel()
        await run_scenarios(
            [case("fast"), case("slow", profile=other)],
            [PROFILE, other],
            adapter,
            run_id="capture",
            budgets=Budgets(execution=0.05),
        )
        assert set(adapter.finished) == {(PROFILE.profile_id, False), (other.profile_id, False)}

    asyncio.run(check())


def test_failed_preparation_quarantines_only_its_own_profile():
    async def check():
        other = replace(PROFILE, profile_id="other", resource_id="arn:other")

        class SlowPreparation(Adapter):
            async def prepare(self, profile):
                await super().prepare(profile)
                if profile == other:
                    await asyncio.sleep(10)

        adapter = SlowPreparation()
        results = await run_scenarios(
            [case("fast"), case("slow", profile=other)],
            [PROFILE, other],
            adapter,
            run_id="capture",
            budgets=Budgets(provisioning=0.05),
        )
        assert next(result for result in results if result.case_id == "slow").status == "INFRASTRUCTURE_ERROR"
        # A failed preparation quarantines only its own target.
        assert (PROFILE.profile_id, True) in adapter.finished

    asyncio.run(check())


def test_finish_uses_admitted_profiles_even_if_the_caller_changes_its_list():
    async def check():
        scenarios = [case("one")]
        adapter = Adapter()
        # Change the list in the loop iteration that schedules the drain.
        asyncio.get_running_loop().call_soon(scenarios.clear)
        await run_scenarios(scenarios, [PROFILE], adapter, run_id="capture")
        assert adapter.finished == [(PROFILE.profile_id, True)]

    asyncio.run(check())


def test_profiles_finish_only_after_every_case_retires():
    async def check():
        other = replace(PROFILE, profile_id="other", resource_id="arn:other")
        fast_retired = asyncio.Event()
        release_slow = asyncio.Event()
        order: list[tuple[str, str]] = []

        class SlowProfile(Adapter):
            async def execute(self, context):
                if context.profile == other:
                    await release_slow.wait()
                await super().execute(context)

            async def retire(self, context):
                retired = await super().retire(context)
                order.append(("retire", context.scenario.case_id))
                if context.scenario.case_id == "fast":
                    fast_retired.set()
                return retired

            async def finish(self, profile, *, quiescent):
                order.append(("finish", profile.profile_id))
                await super().finish(profile, quiescent=quiescent)

        async def unblock_slow_case():
            await fast_retired.wait()
            # Without the barrier, the fast case's profile would finish meanwhile.
            await asyncio.sleep(0.05)
            release_slow.set()

        adapter = SlowProfile()
        _, results = await asyncio.wait_for(
            asyncio.gather(
                unblock_slow_case(),
                run_scenarios([case("fast"), case("slow", profile=other)], [PROFILE, other], adapter, run_id="capture"),
            ),
            10,
        )
        assert [result.status for result in results] == ["COLLECTED", "COLLECTED"], [r.errors for r in results]
        first_finish = next(index for index, (kind, _) in enumerate(order) if kind == "finish")
        assert first_finish > order.index(("retire", "slow")), order
        assert sorted(adapter.finished) == sorted([(PROFILE.profile_id, True), (other.profile_id, True)])

    asyncio.run(check())


def test_caller_suite_service_transition_preserves_original_execution():
    scenario = Scenario(
        "case",
        "custom-replay",
        PROFILE.profile_id,
        "replay",
        roles=("initial", "continued"),
        coverage_profile="custom",
        service_role_transitions=frozenset({("initial", "continued")}),
    )
    context = ScenarioContext("run", scenario, PROFILE)
    context.record_attempt("initial", role="initial", requestId="initial", executionArn="original-execution")
    with pytest.raises(ValueError, match="existing journaled execution"):
        context.record_service_attempt(
            "new", original_request_id="initial", execution_arn="unrelated-execution", role="continued"
        )
    context.record_service_attempt(
        "continued", original_request_id="initial", execution_arn="original-execution", role="continued"
    )
    assert [(item["role"], item["attempt"]) for item in context.attempts] == [("initial", 1), ("continued", 1)]


@pytest.mark.parametrize("inventory", ["operationExecutions", "retry.attempts"])
def test_cleanup_replay_discovery_requires_settlement(inventory):
    from aws_durable_execution_conformance_tests.lmi.coordinator import _cleanup_record_errors

    record = {"requestId": "discovered", "operationId": "step"}
    replay = {inventory: [record]} if inventory == "operationExecutions" else {"retry": {"attempts": [record]}}
    final: dict[str, Any] = {
        "attempts": [{"requestId": "discovered", "executionArn": "execution"}],
        "retirement": {"resources": []},
    }
    assert any(
        f"cleanup replay.{inventory} native owner discovered is not settled" in error
        for error in _cleanup_record_errors([{"replay": replay}, final], final)
    )
    final["retirement"]["resources"] = [
        {"kind": "invocation", "id": "discovered", "outcome": "QUIESCENT", "settledAtMs": 20},
        {"kind": "execution", "id": "execution", "outcome": "SUCCEEDED", "settledAtMs": 20},
    ]
    assert _cleanup_record_errors([{"replay": replay}, final], final) == []


def test_coordinator_reports_collected_and_deduplicates_unchanged_observations():
    # Conformance verdicts come only from adjudication.
    assert "validator" not in inspect.signature(run_scenarios).parameters

    class ConstantObservations(Adapter):
        async def start(self, context):
            await super().start(context)
            context.evidence = await super().collect(context)

        async def collect(self, context):
            return context.evidence

    statuses = []
    (result,) = asyncio.run(
        run_scenarios(
            [case("one")],
            [PROFILE],
            ConstantObservations(),
            run_id="capture",
            on_result=lambda emitted: statuses.append(emitted.status),
        )
    )
    assert result.status == "COLLECTED"
    assert statuses and set(statuses) == {"COLLECTED"}
    assert len(result.observation_evidence) == 1


def test_prelaunch_partial_collection_surfaces_missing_control_retirement():
    class PrelaunchControl(Adapter):
        async def start(self, context):
            raise OSError("failed before launch")

        async def collect_partial(self, context):
            return {"resourcesCreated": [{"kind": "control", "id": "control", "ownerCaseId": context.scenario.case_id}]}

        async def retire(self, context):
            return True

    adapter = PrelaunchControl()
    (result,) = asyncio.run(run_scenarios([case("one")], [PROFILE], adapter, run_id="capture"))
    assert result.status == "INFRASTRUCTURE_ERROR"
    assert not result.attempts
    assert result.evidence["resourcesCreated"][0]["id"] == "control"
    assert adapter.finished == [(PROFILE.profile_id, False)]
