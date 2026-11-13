# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Scheduling and fault isolation tests, without an AWS deployment."""

import asyncio
import copy
from dataclasses import replace
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Budgets,
    Profile,
    QuarantinedError,
    ResourceScheduler,
    Scenario,
    ScenarioContext,
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
        return {
            "runId": context.run_id,
            "caseId": context.scenario.case_id,
            "cohortId": context.scenario.cohort_id,
            "requirementId": context.scenario.requirement_id,
            "profile": {
                "functionArn": context.profile.resource_id,
                "configurationDigest": context.profile.configuration_id,
                "nativeConcurrency": context.profile.native_concurrency,
            },
            "artifact": {"sdk": context.profile.artifact_id},
            "selectedRequests": {item["role"]: item["requestId"] for item in context.attempts if item["attempt"] == 1},
            "attempts": [{name: item[name] for name in ("requestId", "role", "attempt")} for item in context.attempts],
        }

    async def release(self, context):
        self.released.append(context.scenario.case_id)

    async def retire(self, context):
        self.retired.append(context.scenario.case_id)
        self.live.discard(context.scenario.case_id)
        context.evidence["retirement"] = {"ownerCaseId": context.scenario.case_id}
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
    assert "Launched work has no independent retirement evidence" in result.errors
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
        "cohortId": "cohort-1",
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
    assert result.attempts[0]["attemptId"] == "launch"


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
