# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Scheduling and fault isolation tests, without an AWS deployment."""

import asyncio
import copy
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Literal

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Budgets,
    Profile,
    QuarantinedError,
    ResourceScheduler,
    Scenario,
    ScenarioContext,
    ScenarioResult,
    run_scenarios,
)

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
    return asyncio.run(run_scenarios(cases, profiles, adapter, run_id="run-1", validator=validator, **kwargs))


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


def test_quarantine_requires_positive_recovery_and_no_live_owner():
    async def exercise():
        scheduler = ResourceScheduler()
        scheduler.register("function", 1)
        owner = await scheduler.acquire("function", "owner")
        await scheduler.quarantine("function", "uncertain")

        async def yes():
            return True

        async def no():
            return False

        with pytest.raises(QuarantinedError, match="active"):
            await scheduler.recover("function", yes)
        await owner.release(quiescent=False)
        with pytest.raises(QuarantinedError):
            await scheduler.acquire("function", "later")
        with pytest.raises(QuarantinedError, match="quiescence"):
            await scheduler.recover("function", no)
        await scheduler.recover("function", yes)
        later = await scheduler.acquire("function", "later")
        await later.release(quiescent=True)

    asyncio.run(exercise())


@pytest.mark.parametrize("action", ["retirement", "admission"])
def test_recovery_verification_does_not_hold_admission_conditions(action):
    async def exercise():
        scheduler = ResourceScheduler()
        scheduler.register("recovering", 1)
        scheduler.register("healthy", 1)
        scheduler.limit_concurrency("run", 1)
        owner = await scheduler.acquire("healthy", "owner", budget="run")
        await scheduler.quarantine("recovering", "unretired work")
        entered, finish = asyncio.Event(), asyncio.Event()

        async def verify():
            entered.set()
            await finish.wait()
            return True

        recovery = asyncio.create_task(scheduler.recover("recovering", verify, timeout=1))
        await entered.wait()
        pending = asyncio.create_task(
            owner.release(quiescent=True)
            if action == "retirement"
            else scheduler.acquire("recovering", "blocked", timeout=0.01)
        )
        try:
            done, _ = await asyncio.wait((pending,), timeout=0.2)
            assert pending in done, "external verification blocked an admission condition"
            if action == "retirement":
                assert pending.result() is None
                assert owner.released
            else:
                with pytest.raises(QuarantinedError):
                    pending.result()
            assert not recovery.done()
        finally:
            if action == "admission":
                pending.cancel()
            finish.set()
            await recovery
            await asyncio.gather(pending, return_exceptions=True)
            if not owner.released:
                await owner.release(quiescent=True)

    asyncio.run(exercise())


def test_recovery_timeout_includes_waiting_for_another_verifier():
    async def exercise():
        scheduler = ResourceScheduler()
        scheduler.register("target", 1)
        await scheduler.quarantine("target", "unretired work")
        entered, finish = asyncio.Event(), asyncio.Event()
        later_calls = []

        async def verify():
            entered.set()
            await finish.wait()
            return True

        async def later_verify():
            later_calls.append(True)
            return True

        first = asyncio.create_task(scheduler.recover("target", verify, timeout=1))
        await entered.wait()
        second = asyncio.create_task(scheduler.recover("target", later_verify, timeout=0.01))
        try:
            done, _ = await asyncio.wait((second,), timeout=0.2)
            assert second in done, "waiting for recovery bypassed its timeout"
            with pytest.raises(TimeoutError):
                second.result()
            assert later_calls == []
        finally:
            second.cancel()
            finish.set()
            await first
            await asyncio.gather(second, return_exceptions=True)

    asyncio.run(exercise())


def test_recovery_serializes_verifiers_until_a_positive_result():
    async def exercise():
        scheduler = ResourceScheduler()
        scheduler.register("target", 1)
        await scheduler.quarantine("target", "unretired work")
        entered, finish = asyncio.Event(), asyncio.Event()
        calls = []

        async def first_verify():
            calls.append("first")
            entered.set()
            await finish.wait()
            return False

        async def second_verify():
            calls.append("second")
            return True

        first = asyncio.create_task(scheduler.recover("target", first_verify, timeout=1))
        await entered.wait()
        second = asyncio.create_task(scheduler.recover("target", second_verify, timeout=1))
        await asyncio.sleep(0)
        assert calls == ["first"]
        finish.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        assert isinstance(results[0], QuarantinedError)
        assert results[1] is None
        assert calls == ["first", "second"]
        assert scheduler.quarantine_reason("target") is None

    asyncio.run(exercise())


@pytest.mark.parametrize("new_reason", ["unretired work", "newly discovered work"])
def test_recovery_does_not_clear_a_new_quarantine(new_reason):
    async def exercise():
        scheduler = ResourceScheduler()
        scheduler.register("target", 1)
        await scheduler.quarantine("target", "unretired work")
        entered, finish = asyncio.Event(), asyncio.Event()

        async def verify():
            entered.set()
            await finish.wait()
            return True

        recovery = asyncio.create_task(scheduler.recover("target", verify, timeout=1))
        await entered.wait()
        refresh = asyncio.create_task(scheduler.quarantine("target", new_reason))
        try:
            done, _ = await asyncio.wait((refresh,), timeout=0.2)
            assert refresh in done, "new quarantine was blocked by verification"
            finish.set()
            with pytest.raises(QuarantinedError, match="changed during recovery"):
                await recovery
            assert scheduler.quarantine_reason("target") == new_reason
        finally:
            finish.set()
            await asyncio.gather(recovery, refresh, return_exceptions=True)

    asyncio.run(exercise())


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


@pytest.mark.parametrize("mutation", ["request", "case", "artifact", "profile", "missing-launch"])
def test_collected_evidence_must_match_admitted_originals(mutation):
    class SubstitutingAdapter(Adapter):
        async def collect(self, context):
            evidence = await super().collect(context)
            if mutation == "request":
                evidence["attempts"] = [{"requestId": "retry"}]
            elif mutation == "case":
                evidence["caseId"] = "peer"
            elif mutation == "artifact":
                evidence["artifact"]["sdk"] = "replacement"
            elif mutation == "profile":
                evidence["profile"]["functionArn"] = "replacement"
            else:
                context.record_attempt("unresolved-transport")
            return evidence

    result = run([case("victim")], SubstitutingAdapter())[0]
    assert result.status == "INCOMPLETE_EVIDENCE"
    assert result.errors


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


def test_separate_runs_cannot_replace_an_artifact_with_live_owners():
    async def exercise():
        scheduler = ResourceScheduler()
        entered = asyncio.Event()
        finish_first = asyncio.Event()

        class BlockingAdapter(Adapter):
            async def execute(self, context):
                entered.set()
                await finish_first.wait()

        first_adapter = BlockingAdapter()
        next_adapter = Adapter()
        first = asyncio.create_task(
            run_scenarios(
                [case("first")], [PROFILE], first_adapter, run_id="first", validator=validator, scheduler=scheduler
            )
        )
        await entered.wait()
        second = asyncio.create_task(
            run_scenarios(
                [case("second")], [PROFILE], next_adapter, run_id="second", validator=validator, scheduler=scheduler
            )
        )
        await asyncio.sleep(0.01)
        assert next_adapter.preparations == []
        finish_first.set()
        one, two = await asyncio.gather(first, second)
        assert one[0].status == two[0].status == "PASSED"
        assert first_adapter.finished == next_adapter.finished == [(PROFILE.profile_id, True)]

    asyncio.run(exercise())


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
            run_scenarios(
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
            run_scenarios(
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

        async def verify_no_launched_work():
            return True

        await scheduler.recover(PROFILE.resource_id, verify_no_launched_work)
        following = await run_scenarios(
            [case("following")], [PROFILE], Adapter(), run_id="next", validator=validator, scheduler=scheduler
        )
        assert following[0].status == "PASSED", following[0].errors

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
        results = await run_scenarios(
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
        results = await run_scenarios(
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
            run_scenarios(
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


def test_cleanup_cannot_launch_uncollected_work_and_claim_quiescence():
    class LateLaunchAdapter(Adapter):
        async def retire(self, context):
            context.record_attempt("cleanup-retry", requestId="uncollected-request")
            return await super().retire(context)

    adapter = LateLaunchAdapter()
    result = run([case("victim")], adapter)[0]
    assert result.status == "INFRASTRUCTURE_ERROR"
    assert len(result.attempts) == 2
    assert any("all original journaled launches" in error for error in result.errors)
    assert adapter.finished == [(PROFILE.profile_id, False)]


def test_retained_request_ids_cannot_relabel_a_retry_as_the_selected_original():
    class RelabelingAdapter(Adapter):
        async def start(self, context):
            await super().start(context)
            context.record_attempt("retry", requestId="later-retry")

        async def collect(self, context):
            evidence = await super().collect(context)
            evidence["attempts"][0]["attempt"] = 2
            evidence["attempts"][1]["attempt"] = 1
            evidence["selectedRequests"]["victim"] = "later-retry"
            return evidence

    result = run([case("victim")], RelabelingAdapter())[0]
    assert result.status == "INCOMPLETE_EVIDENCE"
    assert [item["attempt"] for item in result.attempts] == [1, 2]
    assert any("role or ordinal substitutes" in error for error in result.errors)
    assert any("first journaled invocation" in error for error in result.errors)


def test_journaled_execution_cannot_be_rebound():
    context = ScenarioContext("run", case("victim"), PROFILE)
    context.record_attempt("launch", requestId="original", executionArn="execution-a")
    context.update_attempt("launch", executionArn="execution-a")
    with pytest.raises(ValueError, match="original durable execution"):
        context.update_attempt("launch", executionArn="execution-b")


def test_retained_request_ids_cannot_substitute_another_execution():
    # Matching request, role and ordinal is not enough: collecting another
    # execution's attempts, histories and retirement under the journaled
    # request would accept its result and leave the original unaudited.
    class SubstitutingAdapter(Adapter):
        async def start(self, context):
            context.record_attempt(
                "launch", role="victim", requestId="victim-request", executionArn="journaled-execution"
            )

        async def collect(self, context):
            evidence = await super().collect(context)
            evidence["attempts"][0]["executionArn"] = "substituted-execution"
            return evidence

    result = run([case("victim")], SubstitutingAdapter())[0]
    assert result.status == "INCOMPLETE_EVIDENCE"
    assert any("substitutes the journaled execution" in error for error in result.errors), result.errors


def test_retained_request_ids_cannot_swap_cohort_roles():
    class RoleSwappingAdapter(Adapter):
        async def collect(self, context):
            evidence = await super().collect(context)
            evidence["attempts"][0]["role"] = "peer"
            evidence["attempts"][1]["role"] = "victim"
            evidence["selectedRequests"] = {"victim": "cohort:peer", "peer": "cohort:victim"}
            return evidence

    result = run([case("cohort", roles=("victim", "peer"))], RoleSwappingAdapter())[0]
    assert result.status == "INCOMPLETE_EVIDENCE"
    assert any("role or ordinal substitutes" in error for error in result.errors)


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


def test_runs_sharing_a_scheduler_keep_separate_work_budgets():
    # A second run's limit must neither widen nor narrow the first run's.
    serial = replace(PROFILE, profile_id="serial", resource_id="fn-serial", capacity=3, native_concurrency=3)
    wide = replace(PROFILE, profile_id="wide", resource_id="fn-wide", capacity=3, native_concurrency=3)

    class SlowAdapter(Adapter):
        async def execute(self, context):
            await asyncio.sleep(0.05)

    async def exercise():
        scheduler = ResourceScheduler()
        first, second = SlowAdapter(), SlowAdapter()
        await asyncio.gather(
            run_scenarios(
                [case(f"s{i}", profile=serial) for i in range(3)],
                [serial],
                first,
                run_id="run-serial",
                validator=validator,
                scenario_concurrency=1,
                scheduler=scheduler,
            ),
            run_scenarios(
                [case(f"w{i}", profile=wide) for i in range(3)],
                [wide],
                second,
                run_id="run-wide",
                validator=validator,
                scenario_concurrency=3,
                scheduler=scheduler,
            ),
        )
        return first.max_live, second.max_live

    assert asyncio.run(exercise()) == (1, 3)


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
            run_scenarios(
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
            run_scenarios(
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


@pytest.mark.parametrize("dispatch_sequence", [9, 11])
def test_cleanup_dispatch_uses_semantic_sequence_at_closure(dispatch_sequence):
    from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

    original: dict[str, Any] = {
        "attempts": [{"requestId": "request"}],
        "events": [{"requestId": "request", "kind": "wrapper_return", "atMs": 40, "sequence": 10}],
    }
    snapshot = {
        "events": [{**original["events"][0], "sequence": 100}],
        "sdkRequests": [
            {
                "requestId": "request",
                "method": "CheckpointDurableExecution",
                "outcome": "DISPATCHED",
                "atMs": 40,
                "sequence": dispatch_sequence + 90,
            }
        ],
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=False)
    assert any("dispatched SDK work after admission closed" in error for error in errors) == (dispatch_sequence == 11)


@pytest.mark.parametrize("timeout_sequence", [9, 11])
def test_cleanup_deadline_uses_semantic_sequence_for_inflight_work(timeout_sequence):
    from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

    original: dict[str, Any] = {
        "requirementId": "lmi-lifecycle-3",
        "attempts": [{"requestId": "request", "startedAtMs": 10, "endedAtMs": 40}],
        "selectedRequests": {"victim": "request"},
        "profile": {"timeoutMs": 20},
        "events": [{"requestId": "request", "kind": "work_in_flight", "atMs": 30, "sequence": 10}],
    }
    snapshot = {
        "events": [
            {**original["events"][0], "sequence": 100},
            {
                "requestId": "request",
                "kind": "platform_timeout",
                "source": "platform",
                "atMs": 30,
                "sequence": timeout_sequence + 90,
            },
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=True)
    assert any("contradicts the frozen in-flight" in error for error in errors) == (timeout_sequence == 9)


def test_cleanup_body_event_cannot_replay_completed_user_code():
    from pathlib import Path
    from runpy import run_path

    from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

    original = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))["replay_evidence"]()
    resumed = next(row for row in original["attempts"] if row["role"] == "resume")
    operation_id = original["replay"]["before"][0]["id"]
    snapshot = {
        "events": [
            {
                "kind": "body",
                "requestId": resumed["requestId"],
                "executionArn": resumed["executionArn"],
                "operationId": operation_id,
                "atMs": resumed["startedAtMs"] + 1,
            }
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=True)
    assert f"ASSERTION: cleanup evidence reports repeated completed user code for {operation_id}" in errors


def test_independent_ownership_audit_does_not_require_selected_cohort():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _ownership_errors

    async def check():
        adapter = Adapter()
        context = ScenarioContext("run", case("one"), PROFILE)
        await adapter.start(context)
        observed = await adapter.collect(context)
        observed["selectedRequests"] = {}
        assert not _ownership_errors(context, observed, complete_cohort=False)
        assert _ownership_errors(context, observed)

    asyncio.run(check())


def test_retirement_completions_project_only_to_matching_frozen_starts():
    from aws_durable_execution_conformance_tests.lmi.offline import _project_retirement_completions

    retained = {"kind": "io_exit", "requestId": "request", "taskId": "present"}
    completions = [
        retained,
        {"kind": "io_exit", "requestId": "other", "taskId": "present"},
        {"kind": "io_exit", "requestId": "request", "taskId": "discovered-later"},
        {"kind": "task_exit", "requestId": "request", "taskId": "present"},
    ]
    frozen = [{"kind": "io_start", "requestId": "request", "taskId": "present"}]
    assert _project_retirement_completions(frozen, completions) == [retained]
    assert len(completions) == 4


def test_widened_native_invocation_end_can_enrich_fixture_source_to_platform():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retain_observations

    original = {"attempts": [{"requestId": "request", "endedAtMs": 30, "endedAtMsSource": "fixture"}]}
    collected = {"attempts": [{"requestId": "request", "endedAtMs": 40, "endedAtMsSource": "platform"}]}
    merged, changes = _retain_observations(original, collected)
    assert merged == collected
    assert not changes
    later_fixture = {"attempts": [{"requestId": "request", "endedAtMs": 45, "endedAtMsSource": "fixture"}]}
    merged, changes = _retain_observations(collected, later_fixture)
    assert merged == collected
    assert not changes


def test_result_sink_failure_does_not_interrupt_other_cases():
    def sink(result):
        if result.case_id == "one":
            raise OSError("audit unavailable")

    adapter = Adapter()
    first, other = run([case("one"), case("other")], adapter, on_result=sink)
    assert first.status == "INFRASTRUCTURE_ERROR"
    assert other.status == "PASSED"
    assert sorted(adapter.retired) == ["one", "other"]


def test_cleanup_reconciles_call_with_later_request():
    from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

    call = {"kind": "checkpoint_call", "requestId": "request", "callId": "call", "atMs": 20}
    request = {**call, "method": "CheckpointDurableExecution", "outcome": "DISPATCHED"}
    assert any(
        "lacks a unique matching SDK request" in error
        for error in _cleanup_behavior_errors({}, {"events": [call]}, standard=False, final=True)
    )
    assert not any(
        "lacks a unique matching SDK request" in error
        for error in _cleanup_behavior_errors(
            {}, {"sdkRequests": [request]}, known_snapshots=[{"events": [call]}], standard=False, final=True
        )
    )


def test_attempt_delta_carries_native_timestamp_source():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _observation_updates

    first = {"attempts": [{"requestId": "request", "endedAtMs": 30, "endedAtMsSource": "fixture"}]}
    later = {"attempts": [{"requestId": "request", "endedAtMs": 35, "endedAtMsSource": "fixture"}]}
    assert _observation_updates(first, later) == later


@pytest.mark.parametrize("kind", ["alias", "dispatch", "execution"])
def test_retirement_cannot_discard_previously_observed_work(kind):
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retirement_ownership_errors

    async def check():
        context = ScenarioContext("run", case("one"), PROFILE)
        final = await Adapter().collect(context)
        final["retirement"] = {"retiredAtMs": 220, "resources": []}
        previous: dict[str, Any] = {}
        if kind == "alias":
            previous["events"] = [
                {
                    "kind": "observed_task_start",
                    "sourceKind": "task_start",
                    "requestId": "request",
                    "taskId": "child",
                    "atMs": 40,
                }
            ]
            expected = "Retirement omitted or changed previously observed work"
        elif kind == "dispatch":
            call = {"kind": "checkpoint_call", "requestId": "request", "callId": "call", "atMs": 40}
            previous["events"] = [call]
            final["events"] = [{**call, "outcome": "REJECTED"}]
            expected = "Retirement omitted or changed previously observed work"
        else:
            operation = {"id": "operation", "status": "SUCCEEDED", "result": None}
            previous["histories"] = [
                {
                    "executionArn": "execution",
                    "startedAtMs": 40,
                    "operations": [{**operation, "endedAtMs": 150}],
                }
            ]
            final["histories"] = [{"executionArn": "execution", "operations": [operation]}]
            final["retirement"]["resources"] = [{"id": "execution", "kind": "execution", "settledAtMs": 120}]
            expected = "Retirement execution execution settled before its originally observed service activity"
        assert expected in _retirement_ownership_errors(context, previous, final)

    asyncio.run(check())


def test_cleanup_dispatch_tie_without_a_shared_snapshot_cannot_establish_order():
    from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

    original = {
        "attempts": [{"requestId": "request"}],
        "events": [{"requestId": "request", "kind": "wrapper_return", "atMs": 40, "sequence": 10}],
    }
    snapshot = {
        "sdkRequests": [
            {
                "requestId": "request",
                "method": "CheckpointDurableExecution",
                "outcome": "DISPATCHED",
                "atMs": 40,
                "sequence": 11,
            }
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=False, final=True)
    assert any("same-time observations need sequences in one snapshot" in error for error in errors)
    assert not any("dispatched SDK work after admission closed" in error for error in errors)


def test_independent_retirement_projects_post_window_io_exit_from_events():
    from pathlib import Path
    from runpy import run_path

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
    from aws_durable_execution_conformance_tests.lmi.offline import evaluate_evidence

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


def test_independent_retirement_cannot_backdate_observed_completion():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retirement_ownership_errors

    async def check():
        context = ScenarioContext("run", case("one"), PROFILE)
        final = await Adapter().collect(context)
        completion = {"kind": "io_exit", "requestId": "request", "atMs": 215, "taskId": "work"}
        previous = {"retirement": {"completions": [completion]}}
        final["retirement"] = {"retiredAtMs": 220, "completions": [{**completion, "atMs": 205}]}
        assert "Retirement omitted or changed previously observed work" in _retirement_ownership_errors(
            context, previous, final
        )

    asyncio.run(check())


def test_later_duplicate_sdk_request_is_rejected_despite_earlier_unique_match():
    from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

    call = {"kind": "checkpoint_call", "requestId": "request", "callId": "call", "atMs": 20, "outcome": "REJECTED"}
    request = {**call, "method": "CheckpointDurableExecution"}
    errors = _cleanup_behavior_errors(
        {"events": [call], "sdkRequests": [request]},
        {"sdkRequests": [request, request]},
        standard=False,
        final=True,
    )
    assert "ASSERTION: cleanup observed SDK call lacks a unique matching SDK request" in errors


@pytest.mark.parametrize("dispatch", [99, 101])
def test_retirement_sdk_request_renumbering_preserves_relative_order(dispatch):
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retirement_ownership_errors

    async def check():
        context = ScenarioContext("run", case("one"), PROFILE)
        context.record_attempt("first", requestId="request")
        final = await Adapter().collect(context)
        request = {
            "requestId": "request",
            "method": "CheckpointDurableExecution",
            "callId": "call",
            "outcome": "DISPATCHED",
            "atMs": 40,
            "sequence": 9,
        }
        closure = {"requestId": "request", "kind": "wrapper_return", "atMs": 40, "sequence": 10}
        final.update(
            sdkRequests=[{**request, "sequence": dispatch}],
            events=[{**closure, "sequence": 100}],
            retirement={"retiredAtMs": 50},
        )
        errors = _retirement_ownership_errors(context, {"sdkRequests": [request], "events": [closure]}, final)
        assert bool(errors) == (dispatch > 100), errors

    asyncio.run(check())


def test_source_only_attempt_update_uses_its_own_platform_timestamp():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _observation_updates, _retain_observations

    fixture = {"attempts": [{"requestId": "request", "endedAtMs": 30, "endedAtMsSource": "fixture"}]}
    platform = {"attempts": [{"requestId": "request", "endedAtMs": 30, "endedAtMsSource": "platform"}]}
    later = {"attempts": [{"requestId": "request", "endedAtMs": 35, "endedAtMsSource": "fixture"}]}
    updates = _observation_updates(fixture, platform)
    merged, changes = _retain_observations(later, updates)
    assert not changes
    assert merged == platform


def test_finish_is_skipped_when_target_admission_prevents_prepare():
    async def check():
        scheduler = ResourceScheduler()
        scheduler.register(PROFILE.resource_id, PROFILE.capacity)
        lease = await scheduler.acquire(PROFILE.resource_id, "active")
        adapter = Adapter()
        try:
            result = await run_scenarios(
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


def test_invalid_attempt_update_retains_native_identity_without_bad_diagnostics():
    def validate(attempt):
        if "invalid" in attempt:
            raise ValueError("unsupported diagnostic")

    context = ScenarioContext("run", case("one"), PROFILE, _validate_attempt=validate)
    context.record_attempt("first")
    with pytest.raises(ValueError, match="unsupported diagnostic"):
        context.update_attempt("first", requestId="request", executionArn="execution", invalid=object())
    assert context.attempts[0]["requestId"] == "request"
    assert context.attempts[0]["executionArn"] == "execution"
    assert "invalid" not in context.attempts[0]


def test_normal_retirement_audits_previously_reported_resource_identity_and_time():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retained_retirement_errors

    earlier = {
        "retirement": {
            "resources": [
                {"id": "lost", "kind": "callback", "ownerCaseId": "one", "outcome": "PENDING"},
                {"id": "late", "kind": "callback", "ownerCaseId": "one", "settledAtMs": 230},
            ]
        }
    }
    final = {
        "retirement": {
            "retiredAtMs": 220,
            "resources": [
                {"id": "late", "kind": "callback", "ownerCaseId": "one", "settledAtMs": 210, "outcome": "SUCCEEDED"},
            ],
        }
    }
    assert _retained_retirement_errors([earlier], final) == [
        "Final retirement omitted or changed observed resource lost",
        "Observed settlement for late follows final retirement",
    ]


def test_switching_from_quiescent_to_undispatched_still_changes_the_terminal_outcome():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retained_retirement_errors

    earlier = {
        "retirement": {
            "resources": [
                {
                    "id": "rejected",
                    "kind": "invocation",
                    "ownerCaseId": "one",
                    "outcome": "QUIESCENT",
                    "settledAtMs": 100,
                },
            ]
        }
    }
    final = {
        "retirement": {
            "retiredAtMs": 220,
            "resources": [
                {
                    "id": "rejected",
                    "kind": "invocation",
                    "ownerCaseId": "one",
                    "outcome": "UNDISPATCHED",
                    "settledAtMs": 110,
                },
            ],
        }
    }
    assert _retained_retirement_errors([earlier], final) == [
        "Final retirement changed the terminal outcome of observed resource rejected",
    ]


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


def test_recover_waits_until_residual_adapter_task_has_settled():
    async def check():
        scheduler = ResourceScheduler()
        cancelled, allow_exit, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()

        class InterruptedAdapter(Adapter):
            async def execute(self, context):
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    await allow_exit.wait()
                    exited.set()

        async def verify():
            assert exited.is_set()
            return True

        result = await run_scenarios(
            [case("one")],
            [PROFILE],
            InterruptedAdapter(),
            run_id="run",
            validator=validator,
            scheduler=scheduler,
            budgets=replace(Budgets(), execution=0.01),
        )
        assert cancelled.is_set()
        assert result[0].status == "INFRASTRUCTURE_ERROR"
        with pytest.raises(QuarantinedError, match="interrupted adapter task"):
            await scheduler.recover(PROFILE.resource_id, verify)
        allow_exit.set()
        await asyncio.wait_for(exited.wait(), timeout=1)
        await scheduler.recover(PROFILE.resource_id, verify)

    asyncio.run(check())


def test_public_deployment_waits_until_run_finishes():
    async def check():
        scheduler = ResourceScheduler()
        preparing, proceed = asyncio.Event(), asyncio.Event()
        order = []

        class BlockingAdapter(Adapter):
            async def prepare(self, profile):
                preparing.set()
                await proceed.wait()

            async def start(self, context):
                order.append("case")
                await super().start(context)

            async def finish(self, profile, *, quiescent):
                order.append("finished")

        async def publish():
            async with scheduler.deployment(PROFILE.resource_id, "publisher", timeout=1):
                order.append("published")

        async with asyncio.timeout(2):
            run_task = asyncio.create_task(
                run_scenarios(
                    [case("one")],
                    [PROFILE],
                    BlockingAdapter(),
                    run_id="run",
                    validator=validator,
                    scheduler=scheduler,
                )
            )
            await preparing.wait()
            publisher = asyncio.create_task(publish())
            await asyncio.sleep(0)
            proceed.set()
            results, _ = await asyncio.gather(run_task, publisher)
        assert results[0].status == "PASSED"
        assert order == ["case", "finished", "published"]

    asyncio.run(check())


def test_normal_post_window_completion_does_not_require_independent_proof():
    from pathlib import Path
    from runpy import run_path

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
        run_scenarios(
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
    from pathlib import Path
    from runpy import run_path

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


def test_collection_merges_renumbered_sdk_request_without_duplicate():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retain_observations

    request = {
        "requestId": "request",
        "method": "CheckpointDurableExecution",
        "callId": "call",
        "outcome": "DISPATCHED",
        "atMs": 40,
        "sequence": 9,
    }
    closure = {"requestId": "request", "kind": "wrapper_return", "atMs": 40, "sequence": 10}
    early = {"sdkRequests": [request], "events": [closure]}
    complete = {"sdkRequests": [{**request, "sequence": 99}], "events": [{**closure, "sequence": 100}]}
    merged, changes = _retain_observations(early, complete)
    assert merged == complete and not changes
    complete["sdkRequests"][0]["sequence"] = 101
    _, changes = _retain_observations(early, complete)
    assert ("sdkRequests", "sequence") in changes.conflicts


def test_source_less_endpoint_cannot_replace_platform_timestamp():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retain_observations

    platform = {"attempts": [{"requestId": "request", "endedAtMs": 30, "endedAtMsSource": "platform"}]}
    estimate = {"attempts": [{"requestId": "request", "endedAtMs": 35}]}
    merged, _ = _retain_observations(platform, estimate)
    assert merged == platform


def test_physical_audit_does_not_treat_fixture_end_as_activity_after_platform_correction():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retirement_ownership_errors

    async def check():
        context = ScenarioContext("run", case("one"), PROFILE)
        context.record_attempt("launch", requestId="request")
        final = await Adapter().collect(context)
        final["attempts"][0].update(endedAtMs=30, endedAtMsSource="platform")
        final["retirement"] = {
            "retiredAtMs": 33,
            "resources": [{"kind": "invocation", "id": "request", "settledAtMs": 32}],
        }
        before = {
            "attempts": [
                {**final["attempts"][0], "endedAtMs": 35, "endedAtMsSource": "fixture"},
            ]
        }
        assert _retirement_ownership_errors(context, before, final) == []

    asyncio.run(check())


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


def test_final_retirement_cannot_drop_fault_claim_without_release_time():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retained_retirement_errors

    early = {"retirement": {"faults": [{"id": "pending", "ownerCaseId": "one"}]}}
    final = {"retirement": {"retiredAtMs": 50, "faults": []}}
    assert _retained_retirement_errors([early], final) == ["Final retirement omitted or changed observed fault pending"]


def test_collection_cannot_move_authoritative_platform_start_earlier():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retain_observations

    baseline = {"attempts": [{"requestId": "request", "startedAtMs": 12, "startedAtMsSource": "platform"}]}
    rewritten = {"attempts": [{"requestId": "request", "startedAtMs": 10, "startedAtMsSource": "platform"}]}
    merged, changes = _retain_observations(baseline, rewritten)
    assert merged == baseline
    assert ("attempts", "startedAtMs") in changes.conflicts


def test_cleanup_uses_platform_end_to_reject_timeout_after_completion():
    from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

    before = {
        "requirementId": "lmi-lifecycle-3",
        "selectedRequests": {"victim": "request"},
        "profile": {"timeoutMs": 20},
        "attempts": [{"requestId": "request", "startedAtMs": 10, "endedAtMs": 40, "endedAtMsSource": "fixture"}],
        "events": [{"kind": "platform_timeout", "source": "platform", "requestId": "request", "atMs": 30}],
    }
    final = {"attempts": [{"requestId": "request", "endedAtMs": 29, "endedAtMsSource": "platform"}]}
    assert "ASSERTION: cleanup timeout falls outside the configured native deadline" in _cleanup_behavior_errors(
        before, final, standard=True, final=True
    )


def test_in_place_sdk_request_delta_cannot_hide_late_dispatch():
    class RenumberingAdapter(Adapter):
        async def execute(self, context):
            request_id = context.attempts[0]["requestId"]
            context.evidence["events"] = [
                {"requestId": request_id, "kind": "wrapper_return", "atMs": 40, "sequence": 10}
            ]
            context.evidence["sdkRequests"] = [
                {
                    "requestId": request_id,
                    "method": "CheckpointDurableExecution",
                    "outcome": "DISPATCHED",
                    "atMs": 40,
                    "sequence": 11,
                }
            ]

        async def collect(self, context):
            context.evidence["sdkRequests"][0]["sequence"] = 9
            return {**await super().collect(context), **context.evidence}

    result = run([case("one")], RenumberingAdapter())[0]
    assert result.status != "PASSED"
    assert any("conflict with retained facts" in error and "sdkRequests.sequence" in error for error in result.errors)


@pytest.mark.parametrize("source", [None, "fixture"])
def test_unchanged_platform_time_keeps_authority_when_later_label_is_weaker(source):
    from aws_durable_execution_conformance_tests.lmi.coordinator import (
        _retain_observations,
        _retirement_ownership_errors,
    )

    async def check():
        context = ScenarioContext("run", case("one"), PROFILE)
        context.record_attempt("launch", requestId="request")
        final = await Adapter().collect(context)
        final["attempts"][0]["endedAtMs"] = 30
        if source is not None:
            final["attempts"][0]["endedAtMsSource"] = source
        final["retirement"] = {
            "retiredAtMs": 33,
            "resources": [{"kind": "invocation", "id": "request", "settledAtMs": 32}],
        }
        before = {"attempts": [{**final["attempts"][0], "endedAtMsSource": "platform"}]}
        merged, changes = _retain_observations(before, {"attempts": final["attempts"]})
        assert merged == before and not changes
        assert _retirement_ownership_errors(context, before, final) == []

    asyncio.run(check())


def test_sdk_request_keeps_known_sequence_if_later_partial_row_omits_it():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retain_observations

    request = {"requestId": "request", "method": "CheckpointDurableExecution", "outcome": "DISPATCHED", "atMs": 40}
    original = {"sdkRequests": [{**request, "sequence": 9}]}
    merged, changes = _retain_observations(original, {"sdkRequests": [request]})
    assert merged == original
    assert changes.omitted


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
            run_scenarios(
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
            async with scheduler.deployment(PROFILE.resource_id, "publisher", timeout=0.1):
                pass
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


def test_unpaired_platform_source_cannot_certify_later_fixture_time():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retain_observations

    source_only = {"attempts": [{"requestId": "request", "endedAtMsSource": "platform"}]}
    fixture = {"attempts": [{"requestId": "request", "endedAtMs": 40, "endedAtMsSource": "fixture"}]}
    kept, _ = _retain_observations({}, source_only)
    collected, _ = _retain_observations(kept, fixture)
    assert collected == fixture


def test_source_aware_native_interval_ignores_superseded_fixture_times():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _native_intervals

    first = {"attempts": [{"requestId": "request", "startedAtMs": 50, "endedAtMs": 90}]}
    later = {
        "attempts": [
            {
                "requestId": "request",
                "startedAtMs": 55,
                "startedAtMsSource": "platform",
                "endedAtMs": 80,
                "endedAtMsSource": "platform",
            }
        ]
    }
    assert _native_intervals([first, later]) == {"request": {"startedAtMs": (55, True), "endedAtMs": (80, True)}}


def test_prelaunch_control_can_retire_without_native_records():
    from pathlib import Path
    from runpy import run_path

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
            run_scenarios(
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


def test_collection_replaces_unlabeled_attempt_estimates_with_platform_pairs():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _retain_observations

    before = {"attempts": [{"requestId": "request", "startedAtMs": 50, "endedAtMs": 90}]}
    measured = {
        "attempts": [
            {
                "requestId": "request",
                "startedAtMs": 55,
                "endedAtMs": 80,
                "startedAtMsSource": "platform",
                "endedAtMsSource": "platform",
            }
        ]
    }
    retained, changes = _retain_observations(before, measured)
    assert retained == measured
    assert not changes


def test_partial_event_sequence_does_not_mix_with_inherited_sequence():
    from aws_durable_execution_conformance_tests.lmi.coordinator import (
        _event_frames_order_changed,
        _retain_observations,
    )

    first = {"kind": "one", "requestId": "request", "atMs": 30, "sequence": 9}
    second = {"kind": "two", "requestId": "request", "atMs": 30, "sequence": 10}
    early = {"events": [first, second]}
    partial = {"events": [{**first, "sequence": 99}]}
    final = {"events": [{**first, "sequence": 99}, {**second, "sequence": 100}]}
    merged, _ = _retain_observations(early, partial, compare_sdk_order=False)
    retained, changes = _retain_observations(merged, final, compare_sdk_order=False)
    assert not _event_frames_order_changed(early, final)
    assert ("events", "sequence") not in changes.conflicts
    assert retained == final


def test_partial_same_time_dispatch_does_not_override_complete_order():
    from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

    request = {"requestId": "request", "method": "CheckpointDurableExecution", "outcome": "DISPATCHED", "atMs": 40}
    closure = {"kind": "wrapper_return", "requestId": "request", "atMs": 40, "sequence": 10}
    original = {
        "attempts": [{"requestId": "request"}],
        "events": [closure],
        "sdkRequests": [{**request, "sequence": 9}],
    }
    partial = {"events": [closure], "sdkRequests": [request]}
    errors = _cleanup_behavior_errors(original, original, standard=False, known_snapshots=[partial], final=True)
    assert not any("dispatched SDK work after admission closed" in error or "same-time" in error for error in errors)


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
    results = run([case("cancelled"), case("healthy", profile=other)], CancellingAdapter(), profiles=(PROFILE, other))
    assert results[0].status == "INFRASTRUCTURE_ERROR"
    assert "RuntimeError: execution operation was cancelled" in results[0].errors
    assert results[1].status == "PASSED"


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
            run_scenarios([case("one")], [PROFILE], adapter, run_id="run", validator=validator, scheduler=scheduler)
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
            assert not scheduler._resources[PROFILE.resource_id].profile_active

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


def test_recovery_timeout_keeps_quarantine_until_residual_verifier_stops():
    async def check():
        scheduler = ResourceScheduler()
        scheduler.register("target", 1)
        await scheduler.quarantine("target", "unretired")
        stopped = asyncio.Event()
        release = asyncio.Event()

        async def verify():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
            stopped.set()
            return True

        async def fresh_verify():
            return True

        with pytest.raises(TimeoutError):
            await scheduler.recover("target", verify, timeout=0.01)
        try:
            with pytest.raises(QuarantinedError, match="interrupted adapter task"):
                await scheduler.recover("target", fresh_verify)
            assert scheduler.quarantine_reason("target") == "unretired"
        finally:
            release.set()
            await stopped.wait()
        await scheduler.recover("target", fresh_verify)
        assert scheduler.quarantine_reason("target") is None

    asyncio.run(check())


def test_finished_profile_releases_cross_run_ownership_while_other_profile_runs():
    async def check():
        other = replace(PROFILE, profile_id="other", resource_id="arn:other")
        scheduler = ResourceScheduler()
        release = asyncio.Event()
        running = asyncio.Event()

        class BlockingOther(Adapter):
            async def execute(self, context):
                if context.profile == other:
                    running.set()
                    await release.wait()

        adapter = BlockingOther()
        run = asyncio.create_task(
            run_scenarios(
                [case("fast"), case("slow", profile=other)],
                [PROFILE, other],
                adapter,
                run_id="first",
                validator=validator,
                scheduler=scheduler,
            )
        )
        try:
            await running.wait()
            result = await run_scenarios(
                [case("next")],
                [PROFILE],
                Adapter(),
                run_id="second",
                validator=validator,
                scheduler=scheduler,
                budgets=Budgets(lease=0.2),
            )
            assert result[0].status == "PASSED", result[0].errors
            assert adapter.finished == [(PROFILE.profile_id, True)]
        finally:
            release.set()
            await run

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


def test_cleanup_frames_keep_an_explicitly_malformed_event_sequence():
    from aws_durable_execution_conformance_tests.lmi.coordinator import _cleanup_record_errors

    event = {"requestId": "r", "ownerRequestId": "r", "executionArn": "e", "kind": "barrier_arrived", "atMs": 5}
    # Later frames that omit or correct the sequence do not erase the malformed value.
    frames = [{"events": [{**event, "sequence": False}]}, {"events": [event]}, {"events": [{**event, "sequence": 3}]}]

    assert _cleanup_record_errors(frames, None) == [
        "INCOMPLETE: cleanup events.sequence must be a non-negative integer"
    ]
    assert _cleanup_record_errors(frames[1:], None) == []


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


@pytest.mark.parametrize(
    ("released", "diagnostic"),
    [
        (False, "releasedAtMs must be a finite non-negative number"),
        (81, "observed fault release occurred after retirement ended"),
    ],
)
def test_cleanup_cannot_erase_invalid_fault_release(released, diagnostic):
    class InvalidRelease(Adapter):
        async def collect(self, context):
            evidence = await super().collect(context)
            evidence["faultsCreated"] = [{"id": "fault", "ownerCaseId": context.scenario.case_id}]
            return evidence

        async def release(self, context):
            context.evidence["faultsCreated"][0]["releasedAtMs"] = released
            await super().release(context)

        async def retire(self, context):
            context.evidence["faultsCreated"][0].pop("releasedAtMs")
            await super().retire(context)
            context.evidence["retirement"]["faults"] = [
                {"id": "fault", "ownerCaseId": context.scenario.case_id, "releasedAtMs": 75, "mode": "normal"}
            ]
            return True

    adapter = InvalidRelease()
    result = run([case("one")], adapter)[0]
    assert any(diagnostic in error for error in result.errors)
    assert adapter.finished == [(PROFILE.profile_id, False)]


def test_no_validator_reports_collected_and_deduplicates_unchanged_observations():
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


@pytest.mark.parametrize("history_only", [False, True])
def test_known_cross_profile_claim_blocks_both_external_releases(history_only):
    async def check():
        other = replace(PROFILE, profile_id="other", resource_id="arn:other")
        ready = asyncio.Event()
        claimed: set[str] = set()

        class Claims(Adapter):
            async def start(self, context):
                if history_only and context.scenario.case_id == "first":
                    context.evidence["histories"] = [
                        {
                            "executionArn": "arn:execution:shared",
                            "ownerCaseId": "first",
                            "status": "RUNNING",
                        }
                    ]
                else:
                    context.record_attempt("launch", requestId="shared-request", executionArn="arn:execution:shared")

            async def execute(self, context):
                claimed.add(context.scenario.case_id)
                if len(claimed) == 2:
                    ready.set()
                await ready.wait()

        adapter = Claims()
        results = await run_scenarios(
            [case("first"), case("second", profile=other)], [PROFILE, other], adapter, run_id="capture"
        )
        assert all(result.status == "FAILED" for result in results)
        assert set(adapter.finished) == {(PROFILE.profile_id, False), (other.profile_id, False)}

    asyncio.run(check())
