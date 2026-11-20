# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Bounded, resource-scoped orchestration for opt-in LMI scenarios.

Adapters own AWS calls and fixtures. The coordinator owns admission and keeps a
case's lease through retirement, including time spent suspended or collecting
traces. A scheduler is local to one event loop: adapters must also hold a
cross-process deployment/run lock when persistent targets are shared by runs.
"""

from __future__ import annotations

import asyncio
import copy
import math
import time
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol

from aws_durable_execution_conformance_tests.lmi.artifacts import invoke_rejection_proven
from aws_durable_execution_conformance_tests.lmi.evidence import (
    SCHEMA_VERSION,
    EvidenceValidator,
    _cleanup_behavior_errors,
    _observation_updates,
    _retain_observations,
    _retry_attempt_order,
    _side_effect_observation_errors,
    active_pre_cutoff_errors,
    controls_only_retirement,
    deferred_service_requests,
    frozen_attempt_retirement_errors,
    frozen_history_work_errors,
    retirement_artifact_errors,
    same_json,
    validate_retirement_evidence,
)
from aws_durable_execution_conformance_tests.lmi.observations import active_retry_interval, requests_after_cutoff


@dataclass(frozen=True)
class Profile:
    """One persistent, unchanged function deployment and its capacity budget."""

    profile_id: str
    resource_id: str
    capacity: int
    native_concurrency: int
    artifact_id: str
    configuration_id: str
    recovery_policy: str | None = None

    def __post_init__(self) -> None:
        if not all((self.profile_id, self.resource_id, self.artifact_id, self.configuration_id)):
            raise ValueError("Profile identity, artifact and configuration must be nonempty")
        if any(type(value) is not int or value < 1 for value in (self.capacity, self.native_concurrency)):
            raise ValueError("Profile capacity and native_concurrency must be positive integers")
        if self.recovery_policy not in (None, "reuse-worker", "replace-worker"):
            raise ValueError("Unknown recovery policy")


@dataclass(frozen=True)
class Scenario:
    """A cohort admitted as one owner; identifiers never name durable operations."""

    case_id: str
    requirement_id: str
    profile_id: str
    scenario: str
    roles: tuple[str, ...] = ("victim",)
    slots: int = 1
    exclusive: bool = False
    coverage_profile: str = "standard"
    behavior_budgets: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not all((self.case_id, self.requirement_id, self.profile_id, self.scenario)):
            raise ValueError("Scenario identities must be nonempty")
        if not self.roles or len(set(self.roles)) != len(self.roles) or not all(self.roles):
            raise ValueError("Scenario roles must be nonempty and unique")
        if type(self.slots) is not int or self.slots < 1:
            raise ValueError("Scenario slots must be a positive integer")
        # Catalog applicability belongs to the suite owner. The coordinator
        # also serves SDK-local suites supplied through the programmatic API.
        if not isinstance(self.coverage_profile, str) or not self.coverage_profile.strip():
            raise ValueError("Scenario coverage profile must be a nonempty string")


@dataclass(frozen=True)
class Budgets:
    """Harness time bounds in seconds, not SDK cleanup or delivery SLAs."""

    provisioning: float = 300
    lease: float = 300
    placement: float = 120
    execution: float = 300
    collection: float = 120
    release: float = 30
    retirement: float = 120

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} budget must be finite and positive")


class QuarantinedError(RuntimeError):
    """The target may still own work and cannot admit or deploy another case."""


class PhaseTimeout(TimeoutError):
    """A harness stage exceeded its own budget."""


@dataclass
class _Waiter:
    owner: str
    slots: int
    exclusive: bool
    budget: str | None = None


@dataclass
class _Resource:
    capacity: int
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    recovery_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    queue: deque[_Waiter] = field(default_factory=deque)
    owners: dict[str, _Waiter] = field(default_factory=dict)
    quarantine_reason: str | None = None
    quarantine_version: int = 0


@dataclass
class Lease:
    scheduler: ResourceScheduler
    resource_id: str
    owner: str
    released: bool = False
    reserved_slots: int = 1

    async def release_capacity(self) -> None:
        await self.scheduler._capacity(self.resource_id, self.owner, 0)

    async def reserve_capacity(self, *, timeout: float = 300) -> None:
        async with asyncio.timeout(timeout):
            await self.scheduler._capacity(self.resource_id, self.owner, self.reserved_slots)

    async def release(self, *, quiescent: bool) -> None:
        if self.released:
            return
        await self.scheduler._release(self.resource_id, self.owner, quiescent)
        self.released = True


class ResourceScheduler:
    """FIFO admission per target, with shared capacity and exclusive windows.

    Native capacity is a reservation budget, not proof of Lambda placement.
    Ownership persists until the caller explicitly verifies retirement.

    A run's global active-work budget is granted together with a target's
    capacity, never before or after it. Acquiring them separately deadlocks in
    one direction or starves unrelated targets in the other: a case that holds
    capacity while queueing for the budget blocks an owner that needs that
    capacity back, and a case that holds budget while queueing for a blocked
    target consumes a slot an idle profile's case could use. Admitted owners
    keep their budget slot until they retire, so resuming never queues behind
    another case.
    """

    def __init__(self) -> None:
        self._resources: dict[str, _Resource] = {}
        # Runs can share a scheduler, so each keeps its own work budget.
        self._limits: dict[str, int] = {}
        self._active: Counter[str] = Counter()

    def limit_concurrency(self, budget: str, concurrency: int) -> None:
        if not budget or type(concurrency) is not int or concurrency < 1:
            raise ValueError("A work budget needs a name and a positive concurrency")
        self._limits[budget] = concurrency

    def _budget_available(self, budget: str | None) -> bool:
        return budget is None or budget not in self._limits or self._active[budget] < self._limits[budget]

    async def _wake_other_resources(self, released: str) -> None:
        """Freed budget can admit a waiter on any other target.

        Each condition is taken on its own, never two at once, so two
        concurrent releases cannot wait on each other's lock.
        """
        for resource_id, resource in list(self._resources.items()):
            if resource_id == released:
                continue
            async with resource.condition:
                resource.condition.notify_all()

    def register(self, resource_id: str, capacity: int) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("Resource capacity must be a positive integer")
        previous = self._resources.get(resource_id)
        if previous is not None and previous.capacity != capacity:
            raise ValueError(f"Conflicting capacities for {resource_id}")
        self._resources.setdefault(resource_id, _Resource(capacity))

    def quarantine_reason(self, resource_id: str) -> str | None:
        return self._resources[resource_id].quarantine_reason

    async def quarantine(self, resource_id: str, reason: str) -> None:
        resource = self._resources[resource_id]
        async with resource.condition:
            resource.quarantine_reason = reason
            resource.quarantine_version += 1
            resource.condition.notify_all()

    async def acquire(
        self,
        resource_id: str,
        owner: str,
        *,
        slots: int = 1,
        exclusive: bool = False,
        timeout: float = 300,
        budget: str | None = None,
    ) -> Lease:
        """Grant a target's capacity, and a slot in the named work budget.

        Deployment and run-ownership leases are never budgeted: they serialize
        a target rather than run a case's work.
        """
        resource = self._resources[resource_id]
        if not owner or type(slots) is not int or not 1 <= slots <= resource.capacity:
            raise ValueError("A lease needs an owner and slots within the resource capacity")
        request = _Waiter(owner, slots, exclusive, budget)
        async with resource.condition:
            if owner in resource.owners or any(waiter.owner == owner for waiter in resource.queue):
                raise ValueError(f"Duplicate resource owner {owner}")
            resource.queue.append(request)
            try:
                async with asyncio.timeout(timeout):
                    while True:
                        if resource.quarantine_reason is not None:
                            raise QuarantinedError(f"{resource_id}: {resource.quarantine_reason}")
                        first = resource.queue[0] is request
                        available = resource.capacity - sum(item.slots for item in resource.owners.values())
                        active_exclusive = any(item.exclusive for item in resource.owners.values())
                        if (
                            first
                            and not active_exclusive
                            and self._budget_available(budget)
                            and (not resource.owners if exclusive else slots <= available)
                        ):
                            resource.queue.popleft()
                            resource.owners[owner] = request
                            if budget is not None:
                                self._active[budget] += 1
                            resource.condition.notify_all()
                            return Lease(self, resource_id, owner, reserved_slots=slots)
                        await resource.condition.wait()
            finally:
                if request in resource.queue:
                    resource.queue.remove(request)
                    resource.condition.notify_all()

    async def _capacity(self, resource_id: str, owner: str, slots: int) -> None:
        resource = self._resources[resource_id]
        async with resource.condition:
            owned = resource.owners[owner]
            # Existing owners must be able to resume and retire while an
            # exclusive waiter prevents new admission. Otherwise it deadlocks.
            while slots > resource.capacity - sum(item.slots for key, item in resource.owners.items() if key != owner):
                await resource.condition.wait()
            owned.slots = slots
            resource.condition.notify_all()

    async def _release(self, resource_id: str, owner: str, quiescent: bool) -> None:
        resource = self._resources[resource_id]
        async with resource.condition:
            if not quiescent:
                resource.quarantine_reason = f"Unretired owner {owner}"
                resource.quarantine_version += 1
            released = resource.owners.pop(owner)
            if released.budget is not None:
                self._active[released.budget] -= 1
            resource.condition.notify_all()
        if released.budget is not None:
            await self._wake_other_resources(resource_id)

    @asynccontextmanager
    async def deployment(self, resource_id: str, owner: str, *, timeout: float = 300):
        """Exclude all shared/exclusive owners while publishing an artifact."""
        lease = await self.acquire(resource_id, owner, exclusive=True, timeout=timeout)
        completed = False
        try:
            yield
            completed = True
        finally:
            await lease.release(quiescent=completed)

    async def recover(self, resource_id: str, verify: Callable[[], Awaitable[bool]], *, timeout: float = 120) -> None:
        """Clear quarantine only after explicit, bounded external reconciliation.

        No active owner is allowed. The verification must retire residual AWS
        work and establish the deployed artifact before returning literal True.
        Verification leaves admission quarantined without holding its condition,
        so external I/O cannot block retirement or admission on other targets.
        """
        resource = self._resources[resource_id]
        async with asyncio.timeout(timeout):
            async with resource.recovery_lock:
                async with resource.condition:
                    if resource.owners:
                        raise QuarantinedError("Cannot recover a resource while an owner is active")
                    if resource.quarantine_reason is None:
                        return
                    version = resource.quarantine_version
                if await verify() is not True:
                    raise QuarantinedError("Recovery could not prove quiescence")
                async with resource.condition:
                    if resource.quarantine_version != version:
                        raise QuarantinedError("Quarantine changed during recovery; verification must be repeated")
                    resource.quarantine_reason = None
                    resource.condition.notify_all()


@dataclass
class ScenarioContext:
    run_id: str
    scenario: Scenario
    profile: Profile
    evidence: dict[str, Any] = field(default_factory=dict)
    retirement_evidence: dict[str, Any] = field(default_factory=dict, kw_only=True)
    attempts: list[dict[str, Any]] = field(default_factory=list)
    timings: dict[str, dict[str, float]] = field(default_factory=dict)
    _lease: Lease | None = field(default=None, repr=False)
    _on_attempt: Callable[[ScenarioContext], None] | None = field(default=None, repr=False)
    _collection_deadline: float | None = field(default=None, init=False, repr=False, compare=False)

    async def release_capacity(self) -> None:
        """Keep ownership while positively observed suspended work is idle.

        Call only once the cohort no longer occupies its reserved native slots.
        The service may later resume work; reserve capacity before controlling
        another launch. Exclusive scenarios remain exclusive even while idle.
        The run's work budget stays with this case, so resuming never queues
        behind another case; freeing the native slot only admits another case
        when the run's concurrency budget has room for one.
        """
        if self._lease is None:
            raise RuntimeError("Scenario has no active lease")
        await self._lease.release_capacity()

    async def reserve_capacity(self, *, timeout: float = 300) -> None:
        if self._lease is None:
            raise RuntimeError("Scenario has no active lease")
        await self._lease.reserve_capacity(timeout=timeout)

    def dispatcher(self, role: str) -> dict[str, Any]:
        if role not in self.scenario.roles:
            raise ValueError(f"Unknown cohort role {role}")
        payload: dict[str, Any] = {
            "scenario": self.scenario.scenario,
            "runId": self.run_id,
            "caseId": self.scenario.case_id,
            "role": role,
        }
        if self.scenario.coverage_profile != "standard":
            payload["coverageProfile"] = self.scenario.coverage_profile
            payload["behaviorBudgets"] = dict(self.scenario.behavior_budgets)
        return payload

    def record_attempt(self, attempt_id: str, **diagnostics: Any) -> None:
        """Journal a launch's immutable cohort role and ordinal before dispatch.

        A single-role scenario can omit ``role``. For a multi-role cohort the
        adapter must supply it. Ordinals are assigned in launch order per role;
        an explicit ``attempt`` must equal that next ordinal.
        """
        if not attempt_id or any(item["attemptId"] == attempt_id for item in self.attempts):
            raise ValueError("Attempt identities must be nonempty and unique")
        role = diagnostics.get("role")
        if role is None and len(self.scenario.roles) == 1:
            role = self.scenario.roles[0]
        if role not in self.scenario.roles:
            raise ValueError("Every launch needs a role from the admitted cohort")
        ordinal = 1 + sum(item["role"] == role for item in self.attempts)
        if "attempt" in diagnostics and (type(diagnostics["attempt"]) is not int or diagnostics["attempt"] != ordinal):
            raise ValueError("Attempt ordinal must follow the journaled launch order for its role")
        self.attempts.append(
            {
                **diagnostics,
                "attemptId": attempt_id,
                "role": role,
                "attempt": ordinal,
                "launchedAtMs": time.time() * 1000,
            }
        )
        if self._on_attempt is not None:
            self._on_attempt(self)

    def update_attempt(self, attempt_id: str, **diagnostics: Any) -> None:
        for attempt in self.attempts:
            if attempt["attemptId"] == attempt_id:
                if "attemptId" in diagnostics or "launchedAtMs" in diagnostics:
                    raise ValueError("Cannot replace launch identity or time")
                if any(name in diagnostics and diagnostics[name] != attempt[name] for name in ("role", "attempt")):
                    raise ValueError("Cannot replace a launch's original role or attempt ordinal")
                if "attempt" in diagnostics and type(diagnostics["attempt"]) is not int:
                    raise ValueError("Attempt ordinal must remain an integer")
                # Once bound, a launch keeps its request and execution: a later
                # update must not rebind it to another invocation's work.
                for name, label in (
                    ("requestId", "invocation request ID"),
                    ("executionArn", "durable execution"),
                    ("nativeStartedAtMs", "service native start"),
                    ("nativeEndedAtMs", "service native completion"),
                ):
                    if name in attempt and diagnostics.get(name, attempt[name]) != attempt[name]:
                        raise ValueError(f"Cannot replace the original {label}")
                attempt.update(diagnostics)
                if self._on_attempt is not None:
                    self._on_attempt(self)
                return
        raise ValueError(f"Unknown attempt {attempt_id}")


class ScenarioAdapter(Protocol):
    """SDK-owned fixture bridge; all methods must cooperate with cancellation.

    ``prepare`` must hold an external run/deployment lock if another process can
    use this persistent target. Retain that lock until every case has retired.
    ``place`` must positively identify the cohort and release only its barriers.
    ``collect`` returns observation evidence; ``release`` touches only this
    context's controls; ``retire`` writes final ``context.evidence['retirement']``
    and returns True only when all executions, retries, callbacks and late work
    are settled. ``finish`` releases external run ownership only when quiescent
    is True; otherwise it must preserve quarantine until external recovery.
    Every attempted launch is journaled before transport dispatch.

    An optional ``collect_partial(context)`` captures a bounded read-only snapshot
    after a non-cancelled phase failure, before fault release. It must not wait for
    cleanup or invent missing observations. A case that failed before cleanup can release its lease
    only when ``retire`` additionally supplies ``context.retirement_evidence``:
    independently auditable post-cleanup facts. That snapshot never replaces the
    original behavior evidence or makes its result pass.

    Cleanup may discover additional owned work. Retain it on ``context.evidence``
    even when a cleanup method raises. The coordinator preserves each cleanup
    phase's snapshots separately in ``ScenarioResult.cleanup_evidence``. A case
    that passed observations may supply its complete physical retirement snapshot
    through the normal ``context.evidence`` path; cases already failing before cleanup require
    ``context.retirement_evidence``. All discovered work must be reconciled by
    that snapshot under the independent physical retirement checks. Cleanup
    cannot replace the original behavioral observations.
    A newly discovered SDK behavior violation fails the case without invalidating
    otherwise complete physical retirement, including through the normal path.
    """

    async def prepare(self, profile: Profile) -> None: ...
    async def finish(self, profile: Profile, *, quiescent: bool) -> None: ...
    async def start(self, context: ScenarioContext) -> None: ...
    async def place(self, context: ScenarioContext) -> None: ...
    async def execute(self, context: ScenarioContext) -> None: ...
    async def collect(self, context: ScenarioContext) -> Mapping[str, Any]: ...
    async def release(self, context: ScenarioContext) -> None: ...
    async def retire(self, context: ScenarioContext) -> bool: ...


@dataclass
class ScenarioResult:
    """Frozen behavior, physical retirement, and separately retained cleanup snapshots."""

    case_id: str
    requirement_id: str
    profile_id: str
    status: str
    errors: list[str]
    evidence: dict[str, Any]
    attempts: list[dict[str, Any]]
    timings: dict[str, dict[str, float]]
    retirement_evidence: dict[str, Any] = field(default_factory=dict)
    cleanup_evidence: dict[str, list[dict[str, Any]]] = field(default_factory=dict, kw_only=True)


async def _bounded(
    operation: Awaitable[Any], name: str, budget: float, context: ScenarioContext, residual: list[asyncio.Task[Any]]
) -> Any:
    started = time.time() * 1000
    monotonic_started = time.monotonic()
    if name in {"collection", "partialCollection"}:
        # Leave the worker time to return its retained evidence before the watchdog.
        context._collection_deadline = monotonic_started + budget - min(0.1, budget / 10)
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait((task,), timeout=budget)
        if not done:
            raise PhaseTimeout(f"{name} exceeded its {budget:g}s harness budget")
        return task.result()
    finally:
        if name in {"collection", "partialCollection"}:
            context._collection_deadline = None
        context.timings[name] = {
            "startedAtMs": started,
            "finishedAtMs": time.time() * 1000,
            "durationMs": (time.monotonic() - monotonic_started) * 1000,
        }
        if not task.done():
            task.cancel()
            residual.append(task)
            # Do not wait without a bound for a cancellation-resistant adapter.
            task.add_done_callback(lambda finished: None if finished.cancelled() else finished.exception())


def _controls_only_dispatch_reconciled(context: ScenarioContext, evidence: Mapping[str, Any]) -> bool:
    retirement = evidence["retirement"]
    launches = retirement.get("undispatchedLaunches")
    if not isinstance(launches, list):
        return False
    rejected: dict[str, Mapping[str, Any]] = {}
    for launch in launches:
        if not isinstance(launch, Mapping):
            return False
        if invoke_rejection_proven(launch):
            marker = launch.get("marker")
            if not isinstance(marker, str) or marker in rejected:
                return False
            rejected[marker] = launch
    if len(rejected) != len(context.attempts):
        return False
    for journaled in context.attempts:
        marker = journaled.get("attemptId")
        if not isinstance(marker, str) or marker not in rejected or not invoke_rejection_proven(journaled):
            return False
        launch = rejected.pop(marker)
        if journaled.get("source") != "driver-invoke" or set(launch) != {
            "marker",
            "functionArn",
            "durableExecutionName",
            "dispatchState",
            "retryAttempts",
            "rejectionCode",
        }:
            return False
        if any(key not in journaled or not same_json(journaled[key], value) for key, value in launch.items()):
            return False
    return not rejected


def _ownership_errors(
    context: ScenarioContext, evidence: Mapping[str, Any], *, complete_cohort: bool = True
) -> list[str]:
    expected = {
        "runId": context.run_id,
        "caseId": context.scenario.case_id,
        "requirementId": context.scenario.requirement_id,
    }
    errors = [
        f"Collected {name} does not match the admitted case"
        for name, value in expected.items()
        if evidence.get(name) != value
    ]
    profile = evidence.get("profile", {})
    if not isinstance(profile, Mapping) or any(
        profile.get(name) != value
        for name, value in {
            "functionArn": context.profile.resource_id,
            "configurationDigest": context.profile.configuration_id,
            "nativeConcurrency": context.profile.native_concurrency,
        }.items()
    ):
        errors.append("Collected deployment profile does not match the leased target")
    artifact = evidence.get("artifact", {})
    if not isinstance(artifact, Mapping) or artifact.get("sdk") != context.profile.artifact_id:
        errors.append("Collected SDK artifact does not match the deployed revision")
    attempts = evidence.get("attempts")
    if not complete_cohort and controls_only_retirement(evidence):
        if (
            attempts != []
            or evidence.get("selectedRequests") != {}
            or not _controls_only_dispatch_reconciled(context, evidence)
        ):
            errors.append("Controls-only retirement cannot reconcile the dispatch journal")
        return errors
    journaled_attempts = context.attempts
    if complete_cohort:
        excluded = requests_after_cutoff(journaled_attempts, evidence.get("observationEndedAtMs"))
        journaled_attempts = [
            item
            for item in journaled_attempts
            if not isinstance(request := item.get("requestId"), str) or request not in excluded
        ]
    else:
        journaled_attempts = [
            item
            for item in journaled_attempts
            if not (
                invoke_rejection_proven(item)
                and item.get("source") == "driver-invoke"
                and item.get("functionArn") == context.profile.resource_id
                and item.get("marker") == item.get("attemptId")
                and isinstance(item.get("durableExecutionName"), str)
                and bool(item["durableExecutionName"].strip())
            )
        ]
    if not isinstance(attempts, list) or not all(isinstance(item, Mapping) for item in attempts):
        errors.append("Collected invocation attempts are missing or malformed")
    else:
        collected = [item.get("requestId") for item in attempts]
        recorded = [item.get("requestId") for item in journaled_attempts]
        if not recorded or any(not isinstance(item, str) or not item for item in recorded):
            errors.append("Every launched attempt must retain an identified invocation request")
        elif (
            any(not isinstance(item, str) or not item for item in collected)
            or set(collected) != set(recorded)
            or len(collected) != len(recorded)
            or len(set(recorded)) != len(recorded)
        ):
            errors.append("Collected attempts do not match all original journaled launches")
        else:
            journal = {item["requestId"]: item for item in journaled_attempts}
            for item in attempts:
                original = journal[item["requestId"]]
                if (
                    item.get("role") != original["role"]
                    or type(item.get("attempt")) is not int
                    or item["attempt"] != original["attempt"]
                ):
                    errors.append(f"Collected role or ordinal substitutes journaled launch {item['requestId']}")
                # Another execution's evidence under the journaled request would
                # accept its result and leave the journaled execution unaudited.
                if "executionArn" in original and item.get("executionArn") != original["executionArn"]:
                    errors.append(f"Collected execution substitutes the journaled execution of {item['requestId']}")
                if "nativeStartedAtMs" in original and item.get("startedAtMs") != original["nativeStartedAtMs"]:
                    errors.append(f"Collected native start contradicts service history for {item['requestId']}")
                if item.get("activeAtObservationEnd") is True:
                    selected = evidence.get("selectedRequests")
                    victim_id = selected.get("victim") if isinstance(selected, Mapping) else None
                    victim = journal.get(victim_id, {}) if isinstance(victim_id, str) else {}
                    if not complete_cohort or not active_retry_interval(
                        original, evidence.get("observationEndedAtMs"), victim.get("executionArn")
                    ):
                        errors.append(
                            f"Collected active retry lacks a verified service interval for {item['requestId']}"
                        )
                elif "nativeEndedAtMs" in original and item.get("endedAtMs") != original["nativeEndedAtMs"]:
                    errors.append(f"Collected native end contradicts service history for {item['requestId']}")
        selected = evidence.get("selectedRequests")
        originals = {item["role"]: item.get("requestId") for item in journaled_attempts if item["attempt"] == 1}
        if (
            not isinstance(selected, Mapping)
            or set(selected) != set(originals)
            or (complete_cohort and set(originals) != set(context.scenario.roles))
            or any(selected.get(role) != request_id for role, request_id in originals.items())
        ):
            errors.append("Selected requests must be the first journaled invocation for every admitted role")
    return errors


def _retirement_ownership_errors(
    context: ScenarioContext, original: Mapping[str, Any], final: Mapping[str, Any]
) -> list[str]:
    errors = _ownership_errors(context, final, complete_cohort=False)
    if controls_only_retirement(final) and (
        any(original.get(key, []) != [] for key in ("attempts", "events", "histories", "sdkRequests", "faultsCreated"))
        or original.get("selectedRequests", {}) != {}
        or "replay" in original
    ):
        errors.append("Controls-only retirement cannot replace retained runtime evidence")
    if final.get("coverageProfile", "standard") != context.scenario.coverage_profile:
        errors.append("Retirement coverageProfile changed the admitted case")
    if "runtime" in original and not same_json(original["runtime"], final.get("runtime")):
        errors.append("Retirement runtime differs from the original observations")
    errors.extend(retirement_artifact_errors(original, final))
    errors.extend(frozen_attempt_retirement_errors(original, final))
    original_attempts = original.get("attempts", [])
    for name, identifier, identity_fields in (
        ("resourcesCreated", "id", ("kind", "ownerCaseId")),
        ("faultsCreated", "id", ("ownerCaseId",)),
        ("histories", "executionArn", ("ownerCaseId",)),
    ):
        before, after = original.get(name, []), final.get(name, [])
        if not isinstance(before, list) or not isinstance(after, list):
            raise ValueError("Retirement resource inventories must be lists")
        for resource in before:
            if (
                not isinstance(resource, Mapping)
                or not isinstance(resource.get(identifier), str)
                or not resource[identifier].strip()
            ):
                errors.append(f"Original {name} entry requires a stable non-empty {identifier}")
                continue
            matched_resource = next(
                (
                    item
                    for item in after
                    if isinstance(item, Mapping)
                    and identifier in item
                    and same_json(resource[identifier], item[identifier])
                ),
                None,
            )
            # Provisional rows can lack ownership metadata; final completeness
            # is checked independently. Explicit null and known values persist.
            if matched_resource is None or any(
                key in resource and (key not in matched_resource or not same_json(resource[key], matched_resource[key]))
                for key in identity_fields
            ):
                errors.append(f"Retirement omitted or changed an original {name} entry")

    errors.extend(frozen_history_work_errors(original, final))
    errors.extend(active_pre_cutoff_errors(original, final))

    def latest_time(values: Sequence[Any]) -> float:
        return max(
            (
                value
                for value in values
                if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
            ),
            default=0,
        )

    before_events = original.get("events", [])
    before_requests = original.get("sdkRequests", [])
    # Audit settlement against the frozen observations as well as the final
    # snapshot: even an otherwise valid snapshot cannot erase prior activity.
    retirement = final.get("retirement")
    settlements = retirement.get("resources", []) if isinstance(retirement, Mapping) else []
    if not isinstance(settlements, list):
        raise ValueError("Retirement settlements must be a list")
    invocation_settlements = {
        item.get("id"): item.get("settledAtMs")
        for item in settlements
        if isinstance(item, Mapping) and item.get("kind") == "invocation"
    }
    for attempt in original_attempts:
        request_id = attempt.get("requestId")
        activity = [attempt.get("endedAtMs")]
        activity.extend(
            record.get("atMs")
            for record in (*before_events, *before_requests)
            if isinstance(record, Mapping) and record.get("requestId") == request_id
        )
        settled_at = invocation_settlements.get(request_id)
        if (
            not isinstance(settled_at, (int, float))
            or isinstance(settled_at, bool)
            or not math.isfinite(settled_at)
            or settled_at < latest_time(activity)
        ):
            errors.append("Retirement invocation settled before its originally observed activity")
    observed = [record.get("atMs") for record in (*before_events, *before_requests)]
    observed += [attempt.get("endedAtMs") for attempt in original_attempts]
    observed.append(original.get("observationEndedAtMs"))
    latest = latest_time(observed)
    retired_at = retirement.get("retiredAtMs") if isinstance(retirement, Mapping) else None
    if not isinstance(retired_at, (int, float)) or isinstance(retired_at, bool) or retired_at < latest:
        errors.append("Retirement snapshot predates previously observed activity")
    return errors


def _cleanup_envelope_errors(sources: Sequence[Mapping[str, Any]], final: Mapping[str, Any]) -> list[str]:
    """Retain supplied metadata errors and reconcile cumulative window coverage."""
    findings: list[str] = []
    observation_end: int | float = 0
    watermarks: dict[str, int | float] = {}

    def timestamp(value: Any, path: str) -> int | float | None:
        try:
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
                return value
        except OverflowError:
            pass
        findings.append(f"INCOMPLETE: cleanup {path} must be a finite non-negative number")
        return None

    for source in sources:
        if "schemaVersion" in source and (
            type(source["schemaVersion"]) is not int or source["schemaVersion"] != SCHEMA_VERSION
        ):
            findings.append(f"INCOMPLETE: cleanup unsupported schemaVersion; expected {SCHEMA_VERSION}")
        if "observationEndedAtMs" in source:
            end = timestamp(source["observationEndedAtMs"], "observationEndedAtMs")
            if end is not None:
                observation_end = max(observation_end, end)
        if "collection" not in source:
            continue
        collection = source["collection"]
        if not isinstance(collection, Mapping):
            findings.append("INCOMPLETE: cleanup collection must be an object")
            continue
        if "watermarks" in collection:
            supplied = collection["watermarks"]
            if not isinstance(supplied, Mapping):
                findings.append("INCOMPLETE: cleanup collection.watermarks must be an object")
            else:
                for name in ("lifecycle", "sdkRequests", "history"):
                    if name in supplied:
                        covered = timestamp(supplied[name], f"collection.watermarks.{name}")
                        if covered is not None:
                            watermarks[name] = max(watermarks.get(name, 0), covered)
        if "evidenceUris" in collection:
            references = collection["evidenceUris"]
            if not isinstance(references, list) or any(not isinstance(uri, str) or not uri for uri in references):
                findings.append("INCOMPLETE: cleanup collection.evidenceUris must be an array of non-empty strings")

    # Partial snapshots may precede watermark completion or omit envelope fields.
    # Every retained window still needs eventual collection coverage and a
    # retirement that follows it, regardless of whether owned inventories changed.
    for name, covered in watermarks.items():
        if covered < observation_end:
            findings.append(f"INCOMPLETE: cleanup {name} collection does not cover the retained observation window")
    retirement = final.get("retirement")
    if isinstance(retirement, Mapping) and "retiredAtMs" in retirement:
        retired_at = timestamp(retirement["retiredAtMs"], "retirement.retiredAtMs")
        if retired_at is not None and retired_at < observation_end:
            findings.append("INCOMPLETE: cleanup retirement precedes a retained observation window")
    return findings


def _is_cleanup_proof_error(finding: str) -> bool:
    return finding.startswith("INCOMPLETE:") and not finding.startswith("INCOMPLETE: cleanup replay projection")


async def run_scenarios(
    scenarios: Sequence[Scenario],
    profiles: Sequence[Profile],
    adapter: ScenarioAdapter,
    *,
    run_id: str,
    validator: EvidenceValidator,
    scenario_concurrency: int = 8,
    budgets: Budgets | None = None,
    scheduler: ResourceScheduler | None = None,
    on_result: Callable[[ScenarioResult], None] | None = None,
    on_attempt: Callable[[ScenarioContext], None] | None = None,
) -> list[ScenarioResult]:
    """Prepare each profile once, then run compatible cases concurrently.

    Results preserve pre-release failures. Normal final collection can only add
    the retirement record. Failed cases and cleanup discoveries retain a separate
    audited retirement snapshot, so cleanup cannot rewrite their original behavior
    observations.
    Missing handlers, infrastructure failures and evidence gaps are never passes.
    ``on_attempt`` is a synchronous durable journal sink called before transport
    dispatch. ``on_result`` receives case retirement and final profile-retirement
    updates, including cancelled cases; sinks should replace records by case ID.
    """
    if not run_id or type(scenario_concurrency) is not int or scenario_concurrency < 1:
        raise ValueError("run_id and positive scenario_concurrency are required")
    limits = budgets or Budgets()
    leases = scheduler or ResourceScheduler()
    profile_map = {profile.profile_id: profile for profile in profiles}
    if len(profile_map) != len(profiles) or len({profile.resource_id for profile in profiles}) != len(profiles):
        raise ValueError("Profiles must have unique identities and target resources")
    if len({case.case_id for case in scenarios}) != len(scenarios):
        raise ValueError("Case IDs must be unique within a run")
    admission_identities: dict[str, dict[str, Any]] = {}
    for case in scenarios:
        if case.profile_id not in profile_map or case.slots > profile_map[case.profile_id].capacity:
            raise ValueError(f"Invalid profile or capacity budget for {case.case_id}")
        profile = profile_map[case.profile_id]
        deployment: dict[str, Any] = {
            "functionArn": profile.resource_id,
            "configurationDigest": profile.configuration_id,
            "nativeConcurrency": profile.native_concurrency,
        }
        if profile.recovery_policy is not None:
            deployment["recoveryPolicy"] = profile.recovery_policy
        # Capture admission before adapter callbacks can replace context metadata.
        admission_identities[case.case_id] = {
            "runId": run_id,
            "caseId": case.case_id,
            "requirementId": case.requirement_id,
            "coverageProfile": case.coverage_profile,
            "artifact": {"sdk": profile.artifact_id},
            "profile": deployment,
        }
    for profile in profiles:
        leases.register(profile.resource_id, profile.capacity)
    if not callable(getattr(adapter, "finish", None)):
        raise ValueError("Adapter must implement finish(profile, *, quiescent) for cross-run ownership")
    leases.limit_concurrency(run_id, scenario_concurrency)
    contexts = {
        case.case_id: ScenarioContext(run_id, case, profile_map[case.profile_id], _on_attempt=on_attempt)
        for case in scenarios
    }
    completed: dict[str, ScenarioResult] = {}

    def emit(result: ScenarioResult) -> ScenarioResult:
        completed[result.case_id] = result
        if on_result is not None:
            on_result(result)
        return result

    run_leases: dict[str, Lease] = {}
    prepared: dict[str, asyncio.Task[tuple[list[str], dict[str, dict[str, float]]]]] = {}

    async def prepare(profile: Profile) -> tuple[list[str], dict[str, dict[str, float]]]:
        context = ScenarioContext(
            run_id, Scenario("deployment", "deployment", profile.profile_id, "deployment"), profile
        )
        residual: list[asyncio.Task[Any]] = []
        try:
            run_resource = f"run:{profile.resource_id}"
            leases.register(run_resource, 1)
            run_leases[profile.profile_id] = await leases.acquire(
                run_resource, run_id, exclusive=True, timeout=limits.lease
            )
            async with leases.deployment(profile.resource_id, f"{run_id}:deployment", timeout=limits.lease):
                await _bounded(adapter.prepare(profile), "provisioning", limits.provisioning, context, residual)
            return [], context.timings
        except Exception as exc:
            return [f"provisioning: {type(exc).__name__}: {exc}"], context.timings

    # A failed target does not prevent another profile from preparing or running.
    for profile_id in {case.profile_id for case in scenarios}:
        prepared[profile_id] = asyncio.create_task(prepare(profile_map[profile_id]))

    async def run_case(case: Scenario) -> ScenarioResult:
        context = contexts[case.case_id]
        admitted_identity = admission_identities[case.case_id]
        evidence: dict[str, Any] = {}
        observation_baselines: list[dict[str, Any]] = []
        retry_orderings: list[tuple[str, ...]] = []
        retirement_evidence: dict[str, Any] = {}
        cleanup_evidence: dict[str, list[dict[str, Any]]] = {}
        cleanup_changed = False
        collected_normally = False
        errors: list[str] = []
        context_retention_failed = False
        context_observations: dict[int, tuple[Any, dict[str, Any]]] = {}
        status = "PASSED"
        residual: list[asyncio.Task[Any]] = []
        lease: Lease | None = None
        cancelled = False

        def record_observations(snapshot: dict[str, Any], phase: str) -> None:
            for issue in _side_effect_observation_errors(snapshot):
                diagnostic = f"{issue} ({phase})"
                if diagnostic not in errors:
                    errors.append(diagnostic)
            order = _retry_attempt_order(snapshot)
            if order is not None and len(set(order)) != len(order):
                diagnostic = f"INCOMPLETE: duplicate retry attemptId in observations ({phase})"
                if diagnostic not in errors:
                    errors.append(diagnostic)
            elif order is not None:
                for previous in retry_orderings:
                    shared = set(previous) & set(order)
                    if tuple(item for item in previous if item in shared) != tuple(
                        item for item in order if item in shared
                    ):
                        diagnostic = f"Observed retry attempt order conflicts with earlier observations ({phase})"
                        if diagnostic not in errors:
                            errors.append(diagnostic)
                if order not in retry_orderings:
                    retry_orderings.append(order)
            # Use raw source orders, before delta extraction or inventory merging:
            # merging disjoint fragments must not invent order between them.
            observation_baselines.append(snapshot)

        def retain_context(previous: dict[str, Any], provided: Any, phase: str) -> dict[str, Any]:
            """Freeze known mappings without requiring provisional evidence to be complete."""
            nonlocal context_retention_failed
            retained = previous
            sources = [provided]
            if context.evidence is not provided:
                sources.append(context.evidence)
            for source in sources:
                try:
                    if not isinstance(source, Mapping):
                        raise ValueError("Context observations must be an evidence object")
                    snapshot = copy.deepcopy(dict(source))
                    if snapshot:
                        record_observations(snapshot, phase)
                    previous_observation = context_observations.get(id(source))
                    # Keep the object alive with its last snapshot: an unchanged
                    # supplied object can legitimately predate newer context data.
                    context_observations[id(source)] = (source, snapshot)
                    updates = (
                        snapshot
                        if previous_observation is None
                        else _observation_updates(previous_observation[1], snapshot)
                    )
                    retained, changes = _retain_observations(retained, updates)
                    if changes.conflicts:
                        paths = ", ".join(".".join(path) for path in sorted(changes.conflicts))
                        diagnostic = f"Context observations conflict with retained facts ({phase}): {paths}"
                        if diagnostic not in errors:
                            errors.append(diagnostic)
                except (Exception, asyncio.CancelledError) as exc:
                    errors.append(f"Context observation retention ({phase}): {type(exc).__name__}: {exc}")
                    context_retention_failed = True
            return retained

        admitted_at = time.time() * 1000
        context.timings["admission"] = {"startedAtMs": admitted_at}
        try:
            preparation_errors, preparation_timings = await asyncio.shield(prepared[case.profile_id])
        except asyncio.CancelledError:
            # Waiting cases belong to the run's audit record even though they
            # never acquired a lease, so publish them before propagating.
            emit(
                ScenarioResult(
                    case.case_id,
                    case.requirement_id,
                    case.profile_id,
                    "INFRASTRUCTURE_ERROR",
                    ["Scenario cancelled during target preparation"],
                    {},
                    [],
                    context.timings,
                )
            )
            raise
        context.timings.update(copy.deepcopy(preparation_timings))
        if preparation_errors:
            return emit(
                ScenarioResult(
                    case.case_id,
                    case.requirement_id,
                    case.profile_id,
                    "INFRASTRUCTURE_ERROR",
                    list(preparation_errors),
                    {},
                    [],
                    context.timings,
                )
            )
        try:
            lease_started = time.time() * 1000
            lease_monotonic_started = time.monotonic()
            try:
                # Acquisition already has a timeout. Await it in this task so
                # cancellation cannot strand a lease in a completed child task.
                # The scheduler grants this target's capacity and the run's work
                # budget together, so waiting for one never holds the other.
                lease = await leases.acquire(
                    context.profile.resource_id,
                    f"{run_id}:{case.case_id}",
                    slots=case.slots,
                    exclusive=case.exclusive,
                    timeout=limits.lease,
                    budget=run_id,
                )
            except TimeoutError as exc:
                raise PhaseTimeout(f"lease exceeded its {limits.lease:g}s harness budget") from exc
            finally:
                context.timings["lease"] = {
                    "startedAtMs": lease_started,
                    "finishedAtMs": time.time() * 1000,
                    "durationMs": (time.monotonic() - lease_monotonic_started) * 1000,
                }
            context._lease = lease
            context.timings["admission"]["finishedAtMs"] = time.time() * 1000

            # Start and placement share a single budget: repeated placement
            # attempts cannot reset the clock or silently replace a cohort.
            establishment_inputs: list[Any] = []

            async def establish() -> None:
                nonlocal evidence
                for phase, operation in (("start", adapter.start), ("place", adapter.place)):
                    provided = context.evidence
                    establishment_inputs.append(provided)
                    evidence = retain_context(evidence, provided, phase)
                    try:
                        await operation(context)
                    finally:
                        evidence = retain_context(evidence, provided, phase)

            try:
                await _bounded(establish(), "placement", limits.placement, context, residual)
            finally:
                # A bounded phase can return before a cancelled adapter settles.
                # Capture discoveries already present on every supplied object.
                for provided in establishment_inputs:
                    evidence = retain_context(evidence, provided, "placement")
            execution_input = context.evidence
            evidence = retain_context(evidence, execution_input, "execution")
            try:
                await _bounded(adapter.execute(context), "execution", limits.execution, context, residual)
            finally:
                evidence = retain_context(evidence, execution_input, "execution")
            collection_input = context.evidence
            evidence = retain_context(evidence, collection_input, "collection")
            try:
                collected = await _bounded(adapter.collect(context), "collection", limits.collection, context, residual)
            finally:
                evidence = retain_context(evidence, collection_input, "collection")
            if not isinstance(collected, Mapping):
                raise ValueError("Adapter collection must return an evidence object")
            record_observations(copy.deepcopy(dict(collected)), "collection")
            # Returned observations are newest, so valid sequence enrichment and
            # monotonic completion win after all input-object discoveries survive.
            evidence, changed = _retain_observations(evidence, collected)
            context.evidence = copy.deepcopy(evidence)
            collected_normally = True
            ownership_errors = _ownership_errors(context, evidence)
            if changed:
                ownership_errors.append("Collection omitted or changed retained observations")
            if context_retention_failed:
                ownership_errors.append("Context did not retain trustworthy observations")
            errors.extend(ownership_errors)
            errors.extend(validator(evidence, phase="observations"))
            if errors:
                status = "INCOMPLETE_EVIDENCE" if ownership_errors else "FAILED"
        except asyncio.CancelledError:
            status = "INFRASTRUCTURE_ERROR"
            errors.append("Scenario cancelled; retirement is still required")
            cancelled = True
        except NotImplementedError as exc:
            status = "NOT_IMPLEMENTED"
            errors.append(f"Missing SDK fixture: {exc}")
        except Exception as exc:
            status = "INCOMPLETE_EVIDENCE" if isinstance(exc, ValueError) else "INFRASTRUCTURE_ERROR"
            errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            if lease is not None:
                retired = False
                physical_retirement_verified = False
                cleanup_errors: list[str] = []
                cleanup_behavior_errors: list[str] = []
                frozen_cleanup_identity: dict[str, Any] = {}
                frozen_behavior: dict[str, Any] = {}
                owned_inventories = (
                    "attempts",
                    "resourcesCreated",
                    "faultsCreated",
                    "histories",
                    "sdkRequests",
                    "events",
                )

                def inventory_changed(previous: Any, current: Any) -> bool:
                    if not isinstance(previous, list) or not isinstance(current, list):
                        return not same_json(previous, current)
                    remaining = list(previous)
                    for record in current:
                        match = next((index for index, item in enumerate(remaining) if same_json(record, item)), None)
                        if match is None:
                            return True
                        remaining.pop(match)
                    return bool(remaining)

                def check_cleanup_identity(snapshot: Mapping[str, Any], phase: str) -> None:
                    # Partial snapshots may omit metadata, but every supplied
                    # identity must agree with admission and known observations.
                    for key in ("runId", "caseId", "requirementId", "coverageProfile"):
                        if key in snapshot and not same_json(snapshot[key], admitted_identity[key]):
                            cleanup_errors.append(f"Cleanup {phase}: {key} differs from the admitted identity")
                    if (
                        "runtime" in snapshot
                        and "runtime" in frozen_cleanup_identity
                        and not same_json(snapshot["runtime"], frozen_cleanup_identity["runtime"])
                    ):
                        cleanup_errors.append(f"Cleanup {phase}: runtime differs from the observed identity")
                    for section in ("artifact", "profile"):
                        if section not in snapshot:
                            continue
                        supplied = snapshot[section]
                        if not isinstance(supplied, Mapping):
                            cleanup_errors.append(f"Cleanup {phase}: {section} must be an object")
                            continue
                        expected = (admitted_identity[section], frozen_cleanup_identity.get(section, {}))
                        for key, value in supplied.items():
                            if any(
                                isinstance(fields, Mapping) and key in fields and not same_json(value, fields[key])
                                for fields in expected
                            ):
                                cleanup_errors.append(
                                    f"Cleanup {phase}: {section}.{key} differs from the frozen identity"
                                )

                def retain_cleanup(phase: str, retained: Any) -> None:
                    nonlocal cleanup_changed
                    # Retain the object handed to the adapter as well as its
                    # replacement: additions made before assigning None or a
                    # fresh snapshot must survive that replacement.
                    sources = [retained]
                    if context.evidence is not retained:
                        sources.append(context.evidence)
                    for source in sources:
                        try:
                            if not isinstance(source, Mapping):
                                raise ValueError("Cleanup observations must remain an evidence object")
                            snapshot = copy.deepcopy(dict(source))
                            snapshots = cleanup_evidence.setdefault(phase, [])
                            if not any(same_json(snapshot, previous) for previous in snapshots):
                                snapshots.append(snapshot)
                                check_cleanup_identity(snapshot, phase)
                                for finding in _cleanup_behavior_errors(
                                    frozen_behavior,
                                    snapshot,
                                    standard=case.coverage_profile == "standard",
                                    known_snapshots=[
                                        item for snapshots in cleanup_evidence.values() for item in snapshots
                                    ],
                                ):
                                    destination = (
                                        cleanup_errors if _is_cleanup_proof_error(finding) else cleanup_behavior_errors
                                    )
                                    if finding not in destination:
                                        destination.append(finding)
                                # Source failures invalidate cleanup evidence
                                # even when ownership inventories did not change
                                # or a later phase removes the diagnostics.
                                source_errors = snapshot.get("collectionErrors", [])
                                if not isinstance(source_errors, list):
                                    cleanup_errors.append(
                                        f"Cleanup {phase}: collectionErrors must be an array of non-empty strings"
                                    )
                                else:
                                    for index, message in enumerate(source_errors):
                                        if not isinstance(message, str) or not message.strip():
                                            cleanup_errors.append(
                                                f"Cleanup {phase}: collectionErrors[{index}] must be a non-empty string"
                                            )
                                        else:
                                            cleanup_errors.append(
                                                f"Cleanup {phase}: collectionErrors[{index}]: {message}"
                                            )
                                source_events = snapshot.get("events", [])
                                if isinstance(source_events, list):
                                    for event in source_events:
                                        if isinstance(event, Mapping) and event.get("kind") in (
                                            "observation_error",
                                            "control_error",
                                        ):
                                            cleanup_errors.append(
                                                f"Cleanup {phase}: {event['kind']} reports failed observation or control publication"
                                            )
                            cleanup_changed |= any(
                                inventory_changed(evidence.get(name, []), snapshot.get(name, []))
                                for name in owned_inventories
                            )
                        except (Exception, asyncio.CancelledError) as exc:
                            cleanup_errors.append(f"Cleanup evidence retention ({phase}): {type(exc).__name__}: {exc}")

                if not collected_normally:
                    # Freeze context-held work before even a read-only collector
                    # can mutate it, fail, or be cancelled.
                    evidence = retain_context(evidence, context.evidence, "failure")
                partial = getattr(adapter, "collect_partial", None)
                if not collected_normally and context.attempts and not cancelled and not residual and callable(partial):
                    partial_input = context.evidence
                    partial_baseline = retain_context(evidence, partial_input, "partial collection")
                    partial_captured: dict[str, Any] | None = None
                    try:
                        captured = await _bounded(
                            partial(context), "partialCollection", limits.collection, context, residual
                        )
                        if not isinstance(captured, Mapping):
                            raise ValueError("Partial collection must return an evidence object")
                        partial_captured = copy.deepcopy(dict(captured))
                        record_observations(partial_captured, "partial collection")
                    except (Exception, asyncio.CancelledError) as exc:
                        errors.append(f"Partial observation collection: {type(exc).__name__}: {exc}")
                        cleanup_errors.append("Partial collection did not retain trustworthy observations")
                    finally:
                        # Preserve mutations to the supplied object even when
                        # the collector replaced the attribute before returning.
                        try:
                            evidence = retain_context(partial_baseline, partial_input, "partial collection")
                            if partial_captured is not None:
                                evidence, changed = _retain_observations(evidence, partial_captured)
                                if changed:
                                    errors.append("Partial collection omitted or changed retained observations")
                        except (Exception, asyncio.CancelledError) as exc:
                            errors.append(f"Partial context retention: {type(exc).__name__}: {exc}")
                            cleanup_errors.append("Context did not retain trustworthy observations")
                if context_retention_failed:
                    cleanup_errors.append("Context did not retain trustworthy observations")
                if not collected_normally:
                    context.evidence = copy.deepcopy(evidence)
                    try:
                        if evidence:
                            errors.extend(_ownership_errors(context, evidence))
                            errors.extend(validator(evidence, phase="observations"))
                    except Exception as exc:
                        errors.append(f"Context observation validation: {type(exc).__name__}: {exc}")
                        cleanup_errors.append("Context did not retain trustworthy observations")
                try:
                    frozen_cleanup_identity = copy.deepcopy(
                        {key: evidence[key] for key in ("runtime", "artifact", "profile") if key in evidence}
                    )
                    frozen_behavior = copy.deepcopy(
                        {
                            key: evidence[key]
                            for key in (
                                "requirementId",
                                "runtime",
                                "profile",
                                "budgets",
                                "observationEndedAtMs",
                                "selectedRequests",
                                "attempts",
                                "events",
                                "sdkRequests",
                                "faultsCreated",
                                "retirement",
                                "replay",
                            )
                            if key in evidence
                        }
                    )
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"Cleanup identity retention: {type(exc).__name__}: {exc}")
                release_evidence = context.evidence
                try:
                    await _bounded(adapter.release(context), "release", limits.release, context, residual)
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"fault release: {type(exc).__name__}: {exc}")
                finally:
                    retain_cleanup("release", release_evidence)
                retire_evidence = context.evidence
                try:
                    retired = (
                        await _bounded(adapter.retire(context), "retirement", limits.retirement, context, residual)
                        is True
                    )
                    if not retired:
                        cleanup_errors.append("Retirement did not prove case-owned quiescence")
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"retirement: {type(exc).__name__}: {exc}")
                finally:
                    retain_cleanup("retire", retire_evidence)
                # A timed-out adapter can continue sending requests despite
                # task cancellation. Conservatively require explicit recovery.
                if residual:
                    cleanup_errors.append("A bounded phase was interrupted; residual work requires reconciliation")
                try:
                    if not isinstance(context.evidence, Mapping):
                        raise ValueError("Cleanup observations must remain an evidence object")
                    if evidence and not cleanup_changed and "retirement" in context.evidence:
                        evidence["retirement"] = copy.deepcopy(context.evidence["retirement"])
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"Cleanup evidence retention: {type(exc).__name__}: {exc}")
                cleanup_snapshots = [item for snapshots in cleanup_evidence.values() for item in snapshots]
                owned_work = (
                    context.attempts
                    or context.retirement_evidence
                    or cleanup_changed
                    or any(
                        snapshot.get(name) for snapshot in [evidence, *cleanup_snapshots] for name in owned_inventories
                    )
                )
                recorded_observations = evidence.get("attempts")
                frozen_collection = evidence.get("collection")
                journaled_late = requests_after_cutoff(context.attempts, evidence.get("observationEndedAtMs"))
                full_runtime_audit_required = (
                    bool(journaled_late)
                    or (
                        isinstance(frozen_collection, Mapping)
                        and bool(frozen_collection.get("deferredServiceRequests"))
                    )
                    or (
                        isinstance(recorded_observations, list)
                        and any(
                            isinstance(attempt, Mapping) and attempt.get("activeAtObservationEnd") is True
                            for attempt in recorded_observations
                        )
                    )
                )
                if owned_work and (
                    status != "PASSED" or context.retirement_evidence or cleanup_changed or full_runtime_audit_required
                ):
                    try:
                        if not isinstance(context.retirement_evidence, Mapping):
                            raise ValueError("Independent retirement snapshot must be an object")
                        retirement_evidence = copy.deepcopy(dict(context.retirement_evidence))
                        if not retirement_evidence and status == "PASSED" and cleanup_changed:
                            if not isinstance(context.evidence, Mapping):
                                raise ValueError("Normal cleanup did not retain a complete retirement snapshot")
                            retirement_evidence = copy.deepcopy(dict(context.evidence))
                        if not retirement_evidence:
                            work = "Launched work" if context.attempts else "Owned work"
                            cleanup_errors.append(f"{work} has no independent retirement evidence")
                        else:
                            check_cleanup_identity(retirement_evidence, "retirement snapshot")
                            for finding in _cleanup_behavior_errors(
                                frozen_behavior,
                                retirement_evidence,
                                standard=case.coverage_profile == "standard",
                                known_snapshots=cleanup_snapshots,
                            ):
                                destination = (
                                    cleanup_errors if _is_cleanup_proof_error(finding) else cleanup_behavior_errors
                                )
                                if finding not in destination:
                                    destination.append(finding)
                            audit = [
                                error
                                for baseline in [*observation_baselines, evidence, *cleanup_snapshots]
                                for error in _retirement_ownership_errors(context, baseline, retirement_evidence)
                            ]
                            audit.extend(validate_retirement_evidence(retirement_evidence))
                            if status == "PASSED" and controls_only_retirement(retirement_evidence):
                                audit.append("Controls-only retirement cannot validate a passing behavior observation")
                            if audit:
                                errors.extend(error for error in audit if error not in errors)
                                cleanup_errors.append("Independent retirement evidence did not establish quiescence")
                            else:
                                physical_retirement_verified = True
                    except (Exception, asyncio.CancelledError) as exc:
                        cleanup_errors.append(f"Independent retirement validation: {type(exc).__name__}: {exc}")
                # Envelope changes do not alter ownership inventories, but they
                # still constrain the collection and retirement proof. Audit all
                # raw snapshots so later corrections cannot erase malformed data.
                for finding in _cleanup_envelope_errors(
                    [*observation_baselines, evidence, *cleanup_snapshots, retirement_evidence],
                    retirement_evidence or evidence,
                ):
                    if finding not in cleanup_errors:
                        cleanup_errors.append(finding)
                # Some discoveries need all eventual sources to resolve native
                # request references, even when no ownership inventory changed.
                try:
                    for finding in _cleanup_behavior_errors(
                        frozen_behavior,
                        retirement_evidence or evidence,
                        standard=case.coverage_profile == "standard",
                        known_snapshots=cleanup_snapshots,
                        final=True,
                    ):
                        destination = cleanup_errors if _is_cleanup_proof_error(finding) else cleanup_behavior_errors
                        if finding not in destination:
                            destination.append(finding)
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"Final cleanup behavior validation: {type(exc).__name__}: {exc}")
                if journaled_late:
                    try:
                        deferred = deferred_service_requests(evidence)
                        deferred.update(
                            {
                                entry["requestId"]: entry["executionArn"]
                                for entry in context.attempts
                                if isinstance(entry.get("requestId"), str) and entry["requestId"] in journaled_late
                            }
                        )
                        evidence.setdefault("collection", {})["deferredServiceRequests"] = [
                            {"requestId": request, "executionArn": arn} for request, arn in sorted(deferred.items())
                        ]
                    except (TypeError, ValueError) as exc:
                        cleanup_errors.append(f"Cannot persist deferred service requests: {exc}")

                if evidence and status == "PASSED":
                    try:
                        if cleanup_changed and physical_retirement_verified:
                            # The full expanded inventory was audited above.
                            # Final behavior validation still receives only its
                            # frozen observations and their retirement records.
                            retirement = copy.deepcopy(retirement_evidence["retirement"])
                            for field, inventory in (("resources", "resourcesCreated"), ("faults", "faultsCreated")):
                                original_ids = {item["id"] for item in evidence.get(inventory, [])}
                                retirement[field] = [item for item in retirement[field] if item["id"] in original_ids]
                            evidence["retirement"] = retirement
                        validation_input = evidence
                        if full_runtime_audit_required and physical_retirement_verified:
                            validation_input = {**evidence, "retirementSnapshot": retirement_evidence}
                        final_errors = _ownership_errors(context, evidence) + list(
                            validator(validation_input, phase="final")
                        )
                        new_errors = [
                            error
                            for error in final_errors
                            if error not in errors and error not in cleanup_behavior_errors
                        ]
                        errors.extend(new_errors)
                        # New final-phase failures concern retirement. Keep
                        # ownership until these checks finish; a boolean from
                        # the adapter cannot grant an unsafe exclusive window.
                        if new_errors:
                            cleanup_errors.append("Final retirement evidence did not establish quiescence")
                    except Exception as exc:
                        cleanup_errors.append(f"Final evidence validation: {type(exc).__name__}: {exc}")
                if cleanup_behavior_errors:
                    errors.extend(finding for finding in cleanup_behavior_errors if finding not in errors)
                    if status == "PASSED":
                        status = (
                            "FAILED"
                            if any(not error.startswith("INCOMPLETE:") for error in cleanup_behavior_errors)
                            else "INCOMPLETE_EVIDENCE"
                        )
                if cleanup_errors:
                    if status == "PASSED":
                        status = "INFRASTRUCTURE_ERROR"
                    errors.extend(cleanup_errors)
                await lease.release(quiescent=retired and not cleanup_errors)
        result = emit(
            ScenarioResult(
                case.case_id,
                case.requirement_id,
                case.profile_id,
                status,
                errors,
                evidence,
                copy.deepcopy(context.attempts),
                context.timings,
                retirement_evidence=retirement_evidence,
                cleanup_evidence=cleanup_evidence,
            )
        )
        if cancelled:
            raise asyncio.CancelledError
        return result

    tasks = [asyncio.create_task(run_case(case)) for case in scenarios]
    results: list[ScenarioResult] = []
    try:
        results = list(await asyncio.gather(*tasks))
        return results
    finally:
        for task in (*tasks, *prepared.values()):
            # The first gather already forwards cancellation. A case may be
            # finishing bounded cleanup while a faster cancelled case returns;
            # do not interrupt that retirement with a second cancellation.
            if not task.done() and not task.cancelling():
                task.cancel()
        await asyncio.gather(*tasks, *prepared.values(), return_exceptions=True)

        async def finish_profile(profile_id: str, run_lease: Lease) -> None:
            profile = profile_map[profile_id]
            context = ScenarioContext(run_id, Scenario("finish", "finish", profile_id, "finish"), profile)
            quiet = leases.quarantine_reason(profile.resource_id) is None
            residual: list[asyncio.Task[Any]] = []
            try:
                await _bounded(
                    adapter.finish(profile, quiescent=quiet), "profileRetirement", limits.retirement, context, residual
                )
            except (Exception, asyncio.CancelledError) as exc:
                await leases.quarantine(profile.resource_id, f"Run ownership release failed: {exc}")
                for result in completed.values():
                    if result.profile_id == profile_id:
                        if result.status == "PASSED":
                            result.status = "INFRASTRUCTURE_ERROR"
                        result.errors.append(f"Run ownership release failed: {type(exc).__name__}: {exc}")
            finally:
                # The target's case resource remains quarantined independently;
                # a later run can acquire this mutex but cannot deploy or admit.
                await run_lease.release(quiescent=True)
                for result in completed.values():
                    if result.profile_id == profile_id:
                        result.timings.update(copy.deepcopy(context.timings))
                        emit(result)

        await asyncio.gather(*(finish_profile(profile_id, lease) for profile_id, lease in run_leases.items()))
