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
from itertools import pairwise
from typing import Any, Protocol, TypeGuard

from aws_durable_execution_conformance_tests.lmi.evidence import (
    _CALL_METHODS,
    HISTORY_STATUSES,
    RECOVERY_POLICIES,
    SCHEMA_VERSION,
    TERMINAL_STATUSES,
    EvidenceValidator,
    is_json_integer,
    normalize_retirement_events,
    retained_fault_releases,
    same_json,
    validate_observed_retirement,
    validate_retirement_evidence,
)


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
        if self.recovery_policy is not None and self.recovery_policy not in RECOVERY_POLICIES:
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
    service_role_transitions: frozenset[tuple[str, str]] = field(default_factory=frozenset, kw_only=True)

    def __post_init__(self) -> None:
        if not all((self.case_id, self.requirement_id, self.profile_id, self.scenario)):
            raise ValueError("Scenario identities must be nonempty")
        if not self.roles or len(set(self.roles)) != len(self.roles) or not all(self.roles):
            raise ValueError("Scenario roles must be nonempty and unique")
        transitions = frozenset(self.service_role_transitions)
        if any(
            source == target or source not in self.roles or target not in self.roles for source, target in transitions
        ):
            raise ValueError("Service attempt transitions must connect two distinct admitted roles")
        object.__setattr__(self, "service_role_transitions", transitions)
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
    residual: set[asyncio.Task[Any]] = field(default_factory=set)
    profile_active: bool = False
    quarantine_reason: str | None = None
    quarantine_version: int = 0


@dataclass
class Lease:
    scheduler: ResourceScheduler
    resource_id: str
    owner: str
    _request: _Waiter = field(repr=False)
    released: bool = False
    reserved_slots: int = 1

    async def release_capacity(self) -> None:
        await self.scheduler._capacity(self.resource_id, self._request, 0)

    async def reserve_capacity(self, *, timeout: float = 300) -> None:
        async with asyncio.timeout(timeout):
            await self.scheduler._capacity(self.resource_id, self._request, self.reserved_slots)

    async def release(self, *, quiescent: bool) -> None:
        if self.released:
            return
        await self.scheduler._release(self, quiescent)


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
        self._run_locks: dict[str, asyncio.Lock] = {}
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

    def _quarantine_claim_conflict(self, resource_id: str, reason: str) -> None:
        # Attempt callbacks are synchronous. Block future admission immediately;
        # wake queued owners once the resource condition can be acquired.
        resource = self._resources[resource_id]
        resource.quarantine_reason = reason
        resource.quarantine_version += 1

        async def wake() -> None:
            async with resource.condition:
                resource.condition.notify_all()

        asyncio.get_running_loop().create_task(wake())

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
                            return Lease(self, resource_id, owner, request, reserved_slots=slots)
                        await resource.condition.wait()
            finally:
                if request in resource.queue:
                    resource.queue.remove(request)
                    resource.condition.notify_all()

    async def _capacity(self, resource_id: str, request: _Waiter, slots: int) -> None:
        resource = self._resources[resource_id]
        async with resource.condition:
            while resource.owners.get(request.owner) is request:
                # Existing owners must be able to resume and retire while an
                # exclusive waiter prevents new admission. Otherwise it deadlocks.
                used = sum(item.slots for item in resource.owners.values() if item is not request)
                if slots <= resource.capacity - used:
                    request.slots = slots
                    resource.condition.notify_all()
                    return
                await resource.condition.wait()
            raise RuntimeError(f"Lease for {request.owner} has been released")

    async def _release(self, lease: Lease, quiescent: bool) -> None:
        resource = self._resources[lease.resource_id]
        async with resource.condition:
            if resource.owners.get(lease.owner) is not lease._request:
                return
            if not quiescent:
                resource.quarantine_reason = f"Unretired owner {lease.owner}"
                resource.quarantine_version += 1
            released = resource.owners.pop(lease.owner)
            lease.released = True
            if released.budget is not None:
                self._active[released.budget] -= 1
            resource.condition.notify_all()
        if released.budget is not None:
            await self._wake_other_resources(lease.resource_id)

    def track_residual(self, resource_id: str, tasks: Sequence[asyncio.Task[Any]]) -> None:
        pending = self._resources[resource_id].residual
        for task in tasks:
            if not task.done():
                pending.add(task)
                task.add_done_callback(pending.discard)

    async def _acquire_run(self, resource_id: str, *, timeout: float) -> asyncio.Lock:
        lock = self._run_locks.setdefault(resource_id, asyncio.Lock())
        async with asyncio.timeout(timeout):
            await lock.acquire()
        return lock

    @asynccontextmanager
    async def deployment(self, resource_id: str, owner: str, *, timeout: float = 300):
        """Exclude runs and individual target owners while publishing an artifact."""
        deadline = asyncio.get_running_loop().time() + timeout
        lock = await self._acquire_run(resource_id, timeout=timeout)
        try:
            async with self._deployment_lease(resource_id, owner, timeout=deadline - asyncio.get_running_loop().time()):
                yield
        finally:
            lock.release()

    @asynccontextmanager
    async def _deployment_lease(self, resource_id: str, owner: str, *, timeout: float):
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
                    if resource.owners or resource.profile_active:
                        raise QuarantinedError("Cannot recover a resource while an owner is active")
                    if any(not task.done() for task in resource.residual):
                        raise QuarantinedError("Cannot recover while an interrupted adapter task is active")
                    if resource.quarantine_reason is None:
                        return
                    version = resource.quarantine_version
                verification = asyncio.ensure_future(verify())
                try:
                    await asyncio.shield(verification)
                    if verification.result() is not True:
                        raise QuarantinedError("Recovery could not prove quiescence")
                finally:
                    if not verification.done():
                        verification.cancel()
                        self.track_residual(resource_id, [verification])
                        verification.add_done_callback(lambda task: None if task.cancelled() else task.exception())
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
    _attempts: list[dict[str, Any]] = field(default_factory=list, init=False, repr=False)
    timings: dict[str, dict[str, float]] = field(default_factory=dict)
    _lease: Lease | None = field(default=None, repr=False)
    _on_attempt: Callable[[ScenarioContext], None] | None = field(default=None, repr=False)
    _on_claim: Callable[[ScenarioContext], None] | None = field(default=None, repr=False, kw_only=True)
    _validate_attempt: Callable[[Mapping[str, Any]], None] | None = field(default=None, repr=False)

    @property
    def attempts(self) -> list[dict[str, Any]]:
        """Return the adapter's copy of the coordinator-owned launch journal."""
        return copy.deepcopy(self._attempts)

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
        self._journal_attempt(attempt_id, diagnostics, "launchedAtMs")

    def _journal_attempt(self, attempt_id: str, diagnostics: Mapping[str, Any], timestamp_key: str) -> None:
        if not attempt_id or any(item["attemptId"] == attempt_id for item in self._attempts):
            raise ValueError("Attempt identities must be nonempty and unique")
        role = diagnostics.get("role")
        if role is None and len(self.scenario.roles) == 1:
            role = self.scenario.roles[0]
        if role not in self.scenario.roles:
            raise ValueError("Every launch needs a role from the admitted cohort")
        ordinal = 1 + sum(item["role"] == role for item in self._attempts)
        if "attempt" in diagnostics and (
            not is_json_integer(diagnostics["attempt"]) or diagnostics["attempt"] != ordinal
        ):
            raise ValueError("Attempt ordinal must follow the journaled launch order for its role")
        attempt = {
            **copy.deepcopy(dict(diagnostics)),
            "attemptId": attempt_id,
            "role": role,
            "attempt": ordinal,
            timestamp_key: time.time() * 1000,
        }
        self._attempts.append(attempt)
        try:
            if self._validate_attempt is not None:
                self._validate_attempt(attempt)
            if self._on_attempt is not None:
                self._on_attempt(self)
        except BaseException:
            # An undispatched launch can be rolled back. An already-running
            # service invocation must remain visible to retirement.
            if timestamp_key == "launchedAtMs":
                self._attempts.pop()
            raise
        finally:
            if self._on_claim is not None:
                self._on_claim(self)

    def record_service_attempt(
        self, request_id: str, *, original_request_id: str, execution_arn: str, role: str
    ) -> None:
        """Journal a service-initiated invocation after discovering its native identity.

        The original request and the new attempt must identify the same durable
        execution. Replay's resume role is distinct from its original victim;
        service retries otherwise retain the original role.
        """
        original = next((item for item in self._attempts if item.get("requestId") == original_request_id), None)
        if (
            original is None
            or not execution_arn.strip()
            or original.get("executionArn") != execution_arn
            or any(item.get("requestId") == request_id for item in self._attempts)
            or not request_id.strip()
        ):
            raise ValueError("Service attempt must identify a new request for an existing journaled execution")
        transition = (original["role"], role)
        standard_resume = self.scenario.requirement_id == "lmi-lifecycle-5" and transition == ("victim", "resume")
        if role != original["role"] and not (standard_resume or transition in self.scenario.service_role_transitions):
            raise ValueError("Service attempt role does not match its original invocation")
        self._journal_attempt(
            request_id,
            {"role": role, "requestId": request_id, "executionArn": execution_arn},
            "discoveredAtMs",
        )

    def update_attempt(self, attempt_id: str, **diagnostics: Any) -> None:
        diagnostics = copy.deepcopy(diagnostics)
        for attempt in self._attempts:
            if attempt["attemptId"] == attempt_id:
                if any(key in diagnostics for key in ("attemptId", "launchedAtMs", "discoveredAtMs")):
                    raise ValueError("Cannot replace launch identity or time")
                if any(name in diagnostics and diagnostics[name] != attempt[name] for name in ("role", "attempt")):
                    raise ValueError("Cannot replace a launch's original role or attempt ordinal")
                if "attempt" in diagnostics and not is_json_integer(diagnostics["attempt"]):
                    raise ValueError("Attempt ordinal must remain an integer")
                # Once bound, a launch keeps its request and execution: a later
                # update must not rebind it to another invocation's work.
                for name, label in (("requestId", "invocation request ID"), ("executionArn", "durable execution")):
                    if name in attempt and diagnostics.get(name, attempt[name]) != attempt[name]:
                        raise ValueError(f"Cannot replace the original {label}")
                if "attempt" in diagnostics:
                    diagnostics["attempt"] = int(diagnostics["attempt"])
                try:
                    if self._validate_attempt is not None:
                        self._validate_attempt({**attempt, **diagnostics})
                except Exception:
                    for field_name in ("requestId", "executionArn"):
                        value = diagnostics.get(field_name)
                        if isinstance(value, str) and value.strip():
                            attempt[field_name] = value
                    if self._on_claim is not None:
                        self._on_claim(self)
                    raise
                attempt.update(diagnostics)
                if self._on_claim is not None:
                    self._on_claim(self)
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
    observation_evidence: list[dict[str, Any]] = field(default_factory=list, kw_only=True)
    trusted_observations: bool = field(default=False, kw_only=True)


async def _bounded(
    operation: Awaitable[Any], name: str, budget: float, context: ScenarioContext, residual: list[asyncio.Task[Any]]
) -> Any:
    started = time.time() * 1000
    monotonic_started = time.monotonic()
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait((task,), timeout=budget)
        if not done:
            raise PhaseTimeout(f"{name} exceeded its {budget:g}s harness budget")
        if task.cancelled():
            raise RuntimeError(f"{name} operation was cancelled")
        return task.result()
    finally:
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
    if not isinstance(attempts, list) or not all(isinstance(item, Mapping) for item in attempts):
        errors.append("Collected invocation attempts are missing or malformed")
    else:
        collected = [item.get("requestId") for item in attempts]
        recorded = [item.get("requestId") for item in context.attempts]
        if (complete_cohort and not recorded) or any(not isinstance(item, str) or not item for item in recorded):
            errors.append("Every launched attempt must retain an identified invocation request")
        elif (
            any(not isinstance(item, str) or not item for item in collected)
            or set(collected) != set(recorded)
            or len(collected) != len(recorded)
            or len(set(recorded)) != len(recorded)
        ):
            errors.append("Collected attempts do not match all original journaled launches")
        else:
            journal = {item["requestId"]: item for item in context.attempts}
            for item in attempts:
                original = journal[item["requestId"]]
                if (
                    item.get("role") != original["role"]
                    or not is_json_integer(item.get("attempt"))
                    or item["attempt"] != original["attempt"]
                ):
                    errors.append(f"Collected role or ordinal substitutes journaled launch {item['requestId']}")
                # Another execution's evidence under the journaled request would
                # accept its result and leave the journaled execution unaudited.
                if "executionArn" in original and item.get("executionArn") != original["executionArn"]:
                    errors.append(f"Collected execution substitutes the journaled execution of {item['requestId']}")
        selected = evidence.get("selectedRequests")
        originals = {item["role"]: item.get("requestId") for item in context.attempts if item["attempt"] == 1}
        if complete_cohort and (
            not isinstance(selected, Mapping)
            or set(selected) != set(originals)
            or set(originals) != set(context.scenario.roles)
            or any(selected.get(role) != request_id for role, request_id in originals.items())
        ):
            errors.append("Selected requests must be the first journaled invocation for every admitted role")
    return errors


# Lambda OperationStatus differs from the execution-level status vocabulary.
_OPERATION_NONTERMINAL_STATUSES = ("STARTED", "PENDING", "READY")
_OPERATION_TERMINAL_STATUSES = ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "STOPPED")
# replay.before contains completed checkpoints; only current operation views progress.
_OPERATION_RECORD_PATHS: frozenset[tuple[str, ...]] = frozenset({("histories", "operations"), ("replay", "after")})
_OUTCOME_RECORD_PATHS = _OPERATION_RECORD_PATHS | {("histories",), ("replay", "before")}
_OBSERVATION_IDENTITIES: dict[tuple[str, ...], str] = {
    ("attempts",): "requestId",
    ("histories",): "executionArn",
    ("histories", "operations"): "id",
    ("faultsCreated",): "id",
    ("resourcesCreated",): "id",
    ("replay", "before"): "id",
    ("replay", "after"): "id",
    ("replay", "retry", "attempts"): "attemptId",
    ("replay", "sideEffects"): "operationId",
}


def _observation_advanced(previous: Any, current: Any, path: tuple[str, ...]) -> bool:
    if path in (("attempts", "startedAtMsSource"), ("attempts", "endedAtMsSource")):
        return previous == "fixture" and current == "platform"
    if path == ("histories", "status"):
        return previous in ("RUNNING", "PENDING") and current in HISTORY_STATUSES
    if path[:-1] in _OPERATION_RECORD_PATHS and path[-1:] == ("status",):
        return previous in _OPERATION_NONTERMINAL_STATUSES and current in (
            *_OPERATION_NONTERMINAL_STATUSES,
            *_OPERATION_TERMINAL_STATUSES,
        )
    if path == ("replay", "sideEffects", "count"):
        return is_json_integer(previous) and is_json_integer(current) and 0 <= previous <= current
    increasing = path == ("observationEndedAtMs",) or path[:2] == ("collection", "watermarks")
    increasing |= path == ("attempts", "endedAtMs")
    decreasing = path == ("attempts", "startedAtMs")
    if not (increasing or decreasing):
        return False
    if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in (previous, current)):
        return False
    return current >= previous if increasing else current <= previous


def _owned_record_enriches(previous: Any, current: Any, *, event: bool = False) -> bool:
    if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
        return False
    metadata = {"observedDuringRequestId"}
    if event:
        metadata.add("sequence")
        for record in (previous, current):
            if "sequence" in record and (not is_json_integer(record["sequence"]) or record["sequence"] < 0):
                return False
        if "sequence" in previous and "sequence" not in current:
            return False
    # The shared owned-record validator treats missing and null correlation as
    # unassigned. Once an invocation ID is present it cannot be replaced.
    if previous.get("observedDuringRequestId") is not None and (
        "observedDuringRequestId" not in current
        or not same_json(previous["observedDuringRequestId"], current["observedDuringRequestId"])
    ):
        return False
    return same_json(
        {key: value for key, value in previous.items() if key not in metadata},
        {key: value for key, value in current.items() if key not in metadata},
    )


@dataclass
class _RetentionChanges:
    omitted: set[tuple[str, ...]] = field(default_factory=set)
    conflicts: set[tuple[str, ...]] = field(default_factory=set)

    def __bool__(self) -> bool:
        return bool(self.omitted or self.conflicts)

    def merge(self, other: _RetentionChanges) -> None:
        self.omitted.update(other.omitted)
        self.conflicts.update(other.conflicts)


def _operation_entries_compatible(previous: Any, current: Any) -> bool:
    if same_json(previous, current):
        return True
    if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
        return False
    for key in ("requestId", "operationId"):
        if not isinstance(previous.get(key), str) or not previous[key].strip() or previous[key] != current.get(key):
            return False
    if ("attemptId" in previous) != ("attemptId" in current):
        return False
    if "attemptId" in previous:
        attempt_id = previous["attemptId"]
        if not isinstance(attempt_id, str) or not attempt_id.strip() or attempt_id != current["attemptId"]:
            return False
    elif not (previous.keys() <= current.keys() or current.keys() <= previous.keys()):
        # Without a retry identity, complementary partial descriptions do not
        # establish that two observations came from the same body occurrence.
        return False
    return all(same_json(previous[key], current[key]) for key in previous.keys() & current.keys())


def _retain_operation_entries(previous: list[Any], current: list[Any]) -> tuple[list[Any], _RetentionChanges]:
    """Match compatible body entries one-to-one; their order is not an identity."""
    candidates = [
        [index for index, item in enumerate(current) if _operation_entries_compatible(record, item)]
        for record in previous
    ]
    by_previous: dict[int, int] = {}
    by_current: dict[int, int] = {}
    for start in range(len(previous)):
        pending = deque([start])
        parents: dict[int, int] = {}
        endpoint: int | None = None
        while pending and endpoint is None:
            original = pending.popleft()
            for observed in candidates[original]:
                if observed in parents:
                    continue
                parents[observed] = original
                if observed not in by_current:
                    endpoint = observed
                    break
                pending.append(by_current[observed])
        # Reassign an earlier match when necessary, so a bare entry cannot
        # consume the only occurrence compatible with another entry's metadata.
        while endpoint is not None:
            original = parents[endpoint]
            prior_match = by_previous.get(original)
            by_previous[original] = endpoint
            by_current[endpoint] = original
            endpoint = prior_match

    retained = copy.deepcopy(current)
    changes = _RetentionChanges()
    for index, record in enumerate(previous):
        match = by_previous.get(index)
        if match is None:
            retained.append(copy.deepcopy(record))
            changes.omitted.add(("replay", "operationExecutions"))
        elif isinstance(record, Mapping):
            for key, value in record.items():
                if key not in retained[match]:
                    retained[match][key] = copy.deepcopy(value)
                    changes.omitted.add(("replay", "operationExecutions"))
    return retained, changes


def _observation_updates(
    previous: Mapping[str, Any], current: Mapping[str, Any], path: tuple[str, ...] = ()
) -> dict[str, Any]:
    """Extract new facts without replaying stale fields from a supplied object."""
    updates: dict[str, Any] = {}
    for key, value in current.items():
        if key in previous and same_json(previous[key], value):
            continue
        atomic = path == ("expectedHistories",) or (path in _OUTCOME_RECORD_PATHS and key in ("result", "error"))
        if key not in previous or atomic:
            updates[key] = copy.deepcopy(value)
        elif isinstance(previous[key], Mapping) and isinstance(value, Mapping):
            nested = _observation_updates(previous[key], value, (*path, key))
            if nested:
                updates[key] = nested
        elif isinstance(previous[key], list) and isinstance(value, list) and (*path, key) in _OBSERVATION_IDENTITIES:
            identity = _OBSERVATION_IDENTITIES[(*path, key)]
            previous_counts = Counter(
                record[identity]
                for record in previous[key]
                if isinstance(record, Mapping) and isinstance(record.get(identity), str)
            )
            current_counts = Counter(
                record[identity]
                for record in value
                if isinstance(record, Mapping) and isinstance(record.get(identity), str)
            )
            if any(current_counts[identifier] > count for identifier, count in previous_counts.items()):
                # A lone extra occurrence would merge back into the known row.
                # Preserve this inventory's full multiplicity and facts instead.
                updates[key] = copy.deepcopy(value)
                continue
            remaining = list(previous[key])
            records = []
            for record in value:
                match = next(
                    (
                        index
                        for index, item in enumerate(remaining)
                        if isinstance(record, Mapping)
                        and isinstance(record.get(identity), str)
                        and isinstance(item, Mapping)
                        and record[identity] == item.get(identity)
                    ),
                    None,
                )
                if match is None:
                    records.append(copy.deepcopy(record))
                    continue
                prior = remaining.pop(match)
                nested = _observation_updates(prior, record, (*path, key))
                if nested:
                    nested[identity] = record[identity]
                    if (*path, key) == ("attempts",):
                        for time_field in ("startedAtMs", "endedAtMs"):
                            source_field = f"{time_field}Source"
                            if time_field in nested or source_field in nested:
                                for field_name in (time_field, source_field):
                                    if field_name in record:
                                        nested[field_name] = record[field_name]
                    if (
                        (*path, key) in _OUTCOME_RECORD_PATHS
                        and {"status", "result", "error"} & nested.keys()
                        and "status" in record
                    ):
                        nested["status"] = record["status"]
                        # Status can make an unchanged outcome immutable. Carry
                        # present values (including null), but not removed fields.
                        for outcome in ("result", "error"):
                            if outcome in record:
                                nested[outcome] = copy.deepcopy(record[outcome])
                    records.append(nested)
            if records:
                updates[key] = records
        else:
            # Unkeyed lists stay whole: operationExecutions can contain multiple
            # body occurrences with the same request/operation identities.
            updates[key] = copy.deepcopy(value)
    return updates


def _retain_observations(
    original: Mapping[str, Any],
    captured: Mapping[str, Any],
    path: tuple[str, ...] = (),
    *,
    compare_sdk_order: bool = True,
) -> tuple[dict[str, Any], _RetentionChanges]:
    """Keep every prior fact while adding a failed collector's new observations.

    Native intervals, observation watermarks and unfinished service histories
    may advance. Other conflicting facts retain their original values. Lists
    preserve known records, including multiplicity, so collection cannot erase
    owned work. The raw snapshots are audited separately during retirement.
    """
    retained = copy.deepcopy(dict(captured))
    changed = _RetentionChanges()
    completing_execution = (
        path == ("histories",)
        and original.get("status") in ("RUNNING", "PENDING")
        and captured.get("status") in ("SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED")
    )
    progressing_operation = path in _OPERATION_RECORD_PATHS and _observation_advanced(
        original.get("status"), captured.get("status"), (*path, "status")
    )
    for key, previous in original.items():
        if path == ("attempts",) and key in ("startedAtMs", "endedAtMs"):
            source_key = f"{key}Source"
            if original.get(source_key) == captured.get(source_key) == "platform" and key in captured:
                if type(captured[key]) not in (int, float) or previous != captured[key]:
                    retained[key] = previous
                    changed.conflicts.add((*path, key))
                continue
        if path == ("attempts",) and key in ("startedAtMsSource", "endedAtMsSource"):
            timestamp_key = key.removesuffix("Source")
            if (
                previous == "platform"
                and key not in captured
                and type(captured.get(timestamp_key)) in (int, float)
                and captured[timestamp_key] == original.get(timestamp_key)
            ):
                retained[key] = previous
                continue
            if retained.get(key) == "platform" and timestamp_key not in captured and previous != "platform":
                retained[key] = previous
                changed.conflicts.add((*path, key))
                continue

        if (
            path == ("attempts",)
            and key in ("startedAtMs", "endedAtMs")
            and original.get(f"{key}Source") in (None, "fixture")
            and captured.get(f"{key}Source") == "platform"
            and key in captured
        ):
            continue  # The platform measurement supersedes the fixture estimate.

        if (
            path == ("attempts",)
            and key in ("startedAtMs", "endedAtMs")
            and original.get(f"{key}Source") == "platform"
            and captured.get(f"{key}Source") in (None, "fixture")
        ):
            retained[key] = previous
            continue
        if key in ("result", "error") and (progressing_operation or (completing_execution and previous is None)):
            # Active operations can replace intermediate poll results or retry
            # errors. Terminal checkpoints and non-null execution outcomes stay
            # immutable; an unfinished execution's null is only a placeholder.
            continue
        if path == ("expectedHistories",) or (path in _OUTCOME_RECORD_PATHS and key in ("result", "error")):
            # Checkpoint JSON and each predeclared execution expectation are
            # atomic contracts: arrays, object contents and status allowances
            # cannot be rewritten to accommodate the observed result.
            if key not in retained or not same_json(previous, retained[key]):
                if key in retained:
                    changed.conflicts.add((*path, key))
                else:
                    changed.omitted.add((*path, key))
                retained[key] = copy.deepcopy(previous)
            continue
        if key not in retained:
            retained[key] = copy.deepcopy(previous)
            changed.omitted.add((*path, key))
        elif isinstance(previous, Mapping) and isinstance(retained[key], Mapping):
            retained[key], nested_changed = _retain_observations(previous, retained[key], (*path, key))
            changed.merge(nested_changed)
        elif (
            (*path, key) == ("replay", "operationExecutions")
            and isinstance(previous, list)
            and isinstance(retained[key], list)
        ):
            retained[key], nested_changed = _retain_operation_entries(previous, retained[key])
            changed.merge(nested_changed)
        elif isinstance(previous, list) and isinstance(retained[key], list):
            remaining = list(range(len(retained[key])))
            identity = _OBSERVATION_IDENTITIES.get((*path, key))
            events = (*path, key) == ("events",)
            owned_records = (*path, key) in (("events",), ("sdkRequests",))
            event_order: dict[str, list[tuple[int, int]]] = {}
            prior_records = previous
            if events and all(
                isinstance(record, Mapping) and is_json_integer(record.get("sequence"))
                for record in [*previous, *retained[key]]
            ):
                prior_records = sorted(previous, key=lambda record: record["sequence"])
                remaining.sort(key=lambda index: retained[key][index]["sequence"])
            elif owned_records:
                # Reserve already correlated occurrences before matching an
                # unattributed record that could fit more than one occurrence.
                prior_records = sorted(
                    previous,
                    key=lambda record: not isinstance(record, Mapping) or record.get("observedDuringRequestId") is None,
                )
            for record in prior_records:
                match = next(
                    (
                        index
                        for index in remaining
                        if same_json(record, retained[key][index])
                        or (
                            owned_records
                            and (
                                _owned_record_enriches(record, retained[key][index], event=owned_records)
                                or _owned_record_enriches(retained[key][index], record, event=owned_records)
                            )
                        )
                        or (
                            identity is not None
                            and isinstance(record, Mapping)
                            and isinstance(record.get(identity), str)
                            and isinstance(retained[key][index], Mapping)
                            and record[identity] == retained[key][index].get(identity)
                        )
                    ),
                    None,
                )
                if match is None:
                    retained[key].append(copy.deepcopy(record))
                    changed.omitted.add((*path, key))
                else:
                    remaining.remove(match)
                    raw_sequence = retained[key][match].get("sequence") if events else None
                    if owned_records and isinstance(record, Mapping):
                        for metadata in ("observedDuringRequestId", "sequence"):
                            if metadata == "observedDuringRequestId" and record.get(metadata) is None:
                                continue
                            if metadata in record and (
                                metadata not in retained[key][match]
                                or (metadata == "observedDuringRequestId" and retained[key][match][metadata] is None)
                            ):
                                retained[key][match][metadata] = copy.deepcopy(record[metadata])
                                changed.omitted.add((*path, key))
                    if events and isinstance(record, Mapping):
                        old_sequence, new_sequence = record.get("sequence"), raw_sequence
                        request_id = record.get("requestId")
                        if (
                            isinstance(request_id, str)
                            and is_json_integer(old_sequence)
                            and is_json_integer(new_sequence)
                        ):
                            event_order.setdefault(request_id, []).append((int(old_sequence), int(new_sequence)))
                    if identity is not None and isinstance(record, Mapping):
                        retained[key][match], nested_changed = _retain_observations(
                            record, retained[key][match], (*path, key)
                        )
                        changed.merge(nested_changed)
            for order in event_order.values():
                # Numerical renumbering is harmless; reversing known local
                # event order would erase a lifecycle failure before its audit.
                if compare_sdk_order and any(
                    first[0] < second[0] and first[1] >= second[1] for first, second in pairwise(sorted(order))
                ):
                    changed.conflicts.add((*path, key, "sequence"))
        elif (
            path == ("attempts",)
            and key in ("startedAtMsSource", "endedAtMsSource")
            and previous == "platform"
            and retained[key] == "fixture"
        ):
            retained[key] = previous
        elif not same_json(previous, retained[key]) and not _observation_advanced(
            previous, retained[key], (*path, key)
        ):
            retained[key] = copy.deepcopy(previous)
            changed.conflicts.add((*path, key))
    if path == ("attempts",):
        for key in ("startedAtMs", "endedAtMs"):
            source_key = f"{key}Source"
            if (
                captured.get(source_key) == "platform"
                and key not in captured
                and original.get(source_key) != "platform"
            ):
                retained.pop(source_key, None)
                changed.conflicts.add((*path, source_key))
    if not path:
        for attempt in retained.get("attempts", ()) if isinstance(retained.get("attempts"), list) else ():
            if isinstance(attempt, dict):
                for endpoint in ("startedAtMs", "endedAtMs"):
                    source = f"{endpoint}Source"
                    if endpoint not in attempt and attempt.get(source) == "platform":
                        attempt.pop(source)
        if compare_sdk_order and _sdk_frames_order_changed(original, captured):
            changed.conflicts.add(("sdkRequests", "sequence"))
    return retained, changed


def _sdk_closure_order_changed(
    request: Mapping[str, Any],
    settled_request: Mapping[str, Any],
    before_events: Sequence[Any],
    after_events: Sequence[Any],
) -> bool:
    if request.get("outcome") == "DISPATCHED":
        local_closures = []
        for events in (before_events, after_events):
            local_closures.append(
                [
                    event
                    for event in events
                    if event.get("kind") in ("wrapper_return", "wrapper_raise", "admission_closed")
                    and event.get("requestId") == request.get("requestId")
                    and event.get("atMs") == request.get("atMs")
                ]
            )
        for before_boundary in local_closures[0]:
            for after_boundary in local_closures[1]:
                if before_boundary.get("kind") != after_boundary.get("kind"):
                    continue
                sequences = [
                    request.get("sequence"),
                    before_boundary.get("sequence"),
                    settled_request.get("sequence"),
                    after_boundary.get("sequence"),
                ]
                if all(is_json_integer(sequence) for sequence in sequences) and (
                    (sequences[0] < sequences[1]) != (sequences[2] < sequences[3])
                ):
                    return True

    return False


def _sdk_frames_order_changed(before: Mapping[str, Any], after: Mapping[str, Any]) -> bool:
    prior_requests = before.get("sdkRequests", [])
    current_requests = after.get("sdkRequests", [])
    if not isinstance(prior_requests, list) or not isinstance(current_requests, list):
        return False
    return any(
        _sdk_closure_order_changed(request, candidate, before.get("events", []), after.get("events", []))
        for request in prior_requests
        if isinstance(request, Mapping)
        for candidate in current_requests
        if isinstance(candidate, Mapping)
        if _owned_record_enriches(request, candidate, event=True)
    )


def _event_frames_order_changed(before: Mapping[str, Any], after: Mapping[str, Any]) -> bool:
    _, changes = _retain_observations({"events": before.get("events", [])}, {"events": after.get("events", [])})
    return ("events", "sequence") in changes.conflicts


def _native_intervals(
    sources: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, tuple[int | float, bool]]]:
    """Resolve each endpoint with its source before using native intervals."""
    native: dict[str, dict[str, tuple[int | float, bool]]] = {}
    for source in sources:
        attempts = source.get("attempts", [])
        for attempt in attempts if isinstance(attempts, list) else ():
            if not isinstance(attempt, Mapping) or not isinstance(request := attempt.get("requestId"), str):
                continue
            endpoints = native.setdefault(request, {})
            for endpoint in ("startedAtMs", "endedAtMs"):
                at = attempt.get(endpoint)
                if (
                    not isinstance(at, (int, float))
                    or isinstance(at, bool)
                    or at < 0
                    or (isinstance(at, float) and not math.isfinite(at))
                ):
                    continue
                platform = attempt.get(f"{endpoint}Source") == "platform"
                previous = endpoints.get(endpoint)
                if previous is None or (platform and not previous[1]):
                    endpoints[endpoint] = (at, platform)
                elif platform == previous[1]:
                    # Conflicting platform measurements are audit errors. Use
                    # the stricter bound for checks that also consume them.
                    select = max if (endpoint == "startedAtMs") == platform else min
                    endpoints[endpoint] = (select(previous[0], at), platform)
    return native


def _retirement_ownership_errors(
    context: ScenarioContext,
    original: Mapping[str, Any],
    final: Mapping[str, Any],
    *,
    compare_order: bool = True,
) -> list[str]:
    errors = _ownership_errors(context, final, complete_cohort=False)
    if final.get("coverageProfile", "standard") != context.scenario.coverage_profile:
        errors.append("Retirement coverageProfile changed the admitted case")
    if "runtime" in original and not same_json(original["runtime"], final.get("runtime")):
        errors.append("Retirement runtime differs from the original observations")
    if "artifact" in original:
        # Provisional observations may know only the deployed SDK revision.
        # Completion can add metadata while keeping every known field intact.
        original_artifact, final_artifact = original["artifact"], final.get("artifact")
        if not isinstance(original_artifact, Mapping) or not isinstance(final_artifact, Mapping):
            errors.append("Retirement artifact metadata must remain an object")
        else:
            for key, value in original_artifact.items():
                if key not in final_artifact or not same_json(value, final_artifact[key]):
                    errors.append(f"Retirement artifact.{key} differs from the original observations")
    original_attempts = original.get("attempts", [])
    observed = final.get("attempts", [])
    if not isinstance(original_attempts, list) or not isinstance(observed, list):
        raise ValueError("Retirement invocation inventories must be lists")
    indexed = {item.get("requestId"): item for item in observed if isinstance(item, Mapping)}
    for attempt in original_attempts:
        if not isinstance(attempt, Mapping):
            raise ValueError("Original invocation inventory is malformed")
        settled = indexed.get(attempt.get("requestId"), {})
        # PENDING belongs to the suspended invocation; its resume has a new
        # request ID and cannot replace that already observed native outcome.
        for key in (
            "executionArn",
            "environmentId",
            "workerId",
            "processId",
            "role",
            "attempt",
            "outcome",
        ):
            if key in attempt and attempt[key] != settled.get(key):
                errors.append(f"Retirement changed an original invocation's {key}")
        # Native completion can widen a provisional fixture-observed interval,
        # but it cannot erase any part of the original observation window.
        for key in ("startedAtMs", "endedAtMs"):
            if key not in attempt:
                continue
            before, after = attempt[key], settled.get(key)
            if any(
                isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0
                for value in (before, after)
            ):
                errors.append(f"Retirement cannot reconcile a malformed invocation {key}")
            elif attempt.get(f"{key}Source") == "platform" and after != before:
                errors.append(f"Retirement changed an authoritative invocation's {key}")
            elif not (
                attempt.get(f"{key}Source") in (None, "fixture") and settled.get(f"{key}Source") == "platform"
            ) and ((key == "startedAtMs" and after > before) or (key == "endedAtMs" and after < before)):
                errors.append(f"Retirement narrowed an original invocation's {key}")
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

    # Cleanup may complete active work, but cannot rewrite already terminal
    # service facts. Fixture expectations are separate behavioral evidence.
    def preserve_history_fields(
        before: Mapping[str, Any], after: Mapping[str, Any] | None, owner: str, fields: tuple[str, ...]
    ) -> None:
        if after is None:
            errors.append(f"Retirement omitted {owner}")
            return
        for key in fields:
            if key in before and (key not in after or not same_json(before[key], after[key])):
                errors.append(f"Retirement changed {owner}.{key}")

    final_histories = {
        item.get("executionArn"): item for item in final.get("histories", []) if isinstance(item, Mapping)
    }
    execution_activity: dict[str, list[Any]] = {}
    for history in original.get("histories", []):
        if not isinstance(history, Mapping):
            continue  # Already reported as an unidentified original inventory row.
        arn = history.get("executionArn")
        if not isinstance(arn, str) or not arn.strip():
            continue
        completed_history = final_histories.get(arn)
        activity = execution_activity.setdefault(arn, [])
        activity.extend(history.get(field) for field in ("startedAtMs", "endedAtMs"))
        if history.get("status") in TERMINAL_STATUSES:
            preserve_history_fields(
                history, completed_history, f"terminal execution {arn}", ("status", "result", "error")
            )
        before_operations = history.get("operations", [])
        after_operations = completed_history.get("operations", []) if completed_history is not None else []
        if not isinstance(before_operations, list) or not isinstance(after_operations, list):
            raise ValueError("Retirement operation inventories must be lists")
        indexed_operations = {item.get("id"): item for item in after_operations if isinstance(item, Mapping)}
        for operation in before_operations:
            if not isinstance(operation, Mapping):
                raise ValueError("Original operation inventory is malformed")
            activity.extend(operation.get(field) for field in ("startedAtMs", "endedAtMs"))
            operation_id = operation.get("id")
            operation_fields: tuple[str, ...] = ("name", "type", "parentId")
            owner = f"operation {operation_id} in {arn}"
            if operation.get("status") in _OPERATION_TERMINAL_STATUSES:
                operation_fields += ("status", "result", "error")
                owner = f"terminal {owner}"
            preserve_history_fields(operation, indexed_operations.get(operation_id), owner, operation_fields)
    before_events, after_events = original.get("events", []), final.get("events", [])
    if not isinstance(before_events, list) or not isinstance(after_events, list):
        raise ValueError("Retirement observation inventories must be lists")
    retirement = final.get("retirement")
    completions = retirement.get("completions", []) if isinstance(retirement, Mapping) else []
    if not isinstance(completions, list):
        raise ValueError("Retirement completions must be a list")
    earlier_retirement = original.get("retirement")
    earlier_completions = earlier_retirement.get("completions", []) if isinstance(earlier_retirement, Mapping) else []
    if not isinstance(earlier_completions, list):
        raise ValueError("Previously observed retirement completions must be a list")
    before_events = normalize_retirement_events([*before_events, *earlier_completions], errors)
    after_events = normalize_retirement_events([*after_events, *completions], errors)
    start_kinds = {
        "task_start",
        "io_start",
        "checkpoint_call",
        "poll_call",
    }
    fields = (
        "kind",
        "requestId",
        "ownerRequestId",
        "executionArn",
        "atMs",
        "taskId",
        "callId",
        "gateId",
    )

    # Completion facts and late activity are preserved as well as starts: a
    # snapshot that drops them would audit less than was already observed.
    preserved_kinds = start_kinds | {
        "wrapper_enter",
        "task_exit",
        "io_exit",
        "checkpoint_exit",
        "poll_exit",
        "wrapper_return",
        "wrapper_raise",
        "late_activity",
    }

    def event_identity(event: Mapping[str, Any]) -> dict[str, Any]:
        identity = {key: event.get(key) for key in fields}
        if event.get("kind") in _CALL_METHODS:
            identity["outcome"] = event.get("outcome", "DISPATCHED")
        if "observedDuringRequestId" in event:
            identity["observedDuringRequestId"] = event["observedDuringRequestId"]
        return identity

    def preserved(events: list[Any]) -> list[dict[str, Any]]:
        if any(not isinstance(event, Mapping) for event in events):
            raise ValueError("Retirement observations must be objects")
        return [event_identity(event) for event in events if event.get("kind") in preserved_kinds]

    remaining_events = preserved(after_events)
    for event in sorted(preserved(before_events), key=lambda item: item.get("observedDuringRequestId") is None):
        match = next(
            (index for index, item in enumerate(remaining_events) if _owned_record_enriches(event, item)), None
        )
        if match is None:
            errors.append("Retirement omitted or changed previously observed work")
        else:
            remaining_events.pop(match)

    def sequenced(events: list[Any]) -> dict[Any, list[tuple[int, dict[str, Any]]]]:
        by_request: dict[Any, list[tuple[int, dict[str, Any]]]] = {}
        for event in events:
            if event.get("kind") not in preserved_kinds or "sequence" not in event:
                continue
            sequence = event["sequence"]
            if not is_json_integer(sequence) or sequence < 0:
                errors.append("Retirement cannot reconcile a malformed semantic sequence")
                continue
            by_request.setdefault(event.get("requestId"), []).append((int(sequence), event_identity(event)))
        return by_request

    # Sequence values can change when additional observations are collected.
    # Preserve the known relative order, particularly at equal timestamps;
    # partial events without sequence numbers do not yet establish that order.
    if compare_order:
        original_order, final_order = sequenced(before_events), sequenced(after_events)
        for request_id, records in original_order.items():
            if len({sequence for sequence, _ in records}) != len(records):
                errors.append("Retirement cannot reconcile duplicate original semantic sequences")
                continue
            remaining_order = iter(sorted(final_order.get(request_id, []), key=lambda item: item[0]))
            for _, identity in sorted(records, key=lambda item: item[0]):
                if not any(_owned_record_enriches(identity, candidate) for _, candidate in remaining_order):
                    errors.append("Retirement changed the semantic order of previously observed work")
                    break
    before_requests, after_requests = original.get("sdkRequests", []), final.get("sdkRequests", [])
    if not isinstance(before_requests, list) or not isinstance(after_requests, list):
        raise ValueError("Retirement SDK request inventories must be lists")
    remaining = list(after_requests)
    for request in sorted(
        before_requests, key=lambda item: not isinstance(item, Mapping) or item.get("observedDuringRequestId") is None
    ):
        if not isinstance(request, Mapping):
            raise ValueError("Original SDK request inventory is malformed")
        match = next(
            (index for index, item in enumerate(remaining) if _owned_record_enriches(request, item, event=True)), None
        )
        if match is None:
            errors.append("Retirement omitted or changed a previously observed SDK request")
        else:
            settled_request = remaining.pop(match)
            if compare_order and _sdk_closure_order_changed(request, settled_request, before_events, after_events):
                errors.append("Retirement changed the semantic order of an SDK request and its closure")

    def observed_time(value: Any) -> int | float | None:
        if type(value) is int and value >= 0:
            return value
        if type(value) is float and math.isfinite(value) and value >= 0:
            return value
        return None

    def latest_time(values: Sequence[Any]) -> int | float:
        return max((time for value in values if (time := observed_time(value)) is not None), default=0)

    # Audit settlement against the frozen observations as well as the final
    # snapshot: even an otherwise valid snapshot cannot erase prior activity.
    retirement = final.get("retirement")
    settlements = retirement.get("resources", []) if isinstance(retirement, Mapping) else []
    if not isinstance(settlements, list):
        raise ValueError("Retirement settlements must be a list")
    indexed_settlements = {
        (item.get("kind"), item.get("id")): item.get("settledAtMs")
        for item in settlements
        if isinstance(item, Mapping) and isinstance(item.get("kind"), str) and isinstance(item.get("id"), str)
    }
    authoritative_admission: dict[str, int | float] = {}
    for attempt in indexed.values():
        arn = attempt.get("executionArn")
        started = observed_time(attempt.get("startedAtMs"))
        if isinstance(arn, str) and started is not None and attempt.get("startedAtMsSource") == "platform":
            authoritative_admission[arn] = min(started, authoritative_admission.get(arn, started))
    for history in original.get("histories", []):
        if not isinstance(history, Mapping):
            continue
        arn = history.get("executionArn")
        started = observed_time(history.get("startedAtMs"))
        if isinstance(arn, str) and started is not None and started > authoritative_admission.get(arn, started):
            errors.append(f"Retirement execution {arn} started after its first native admission")
    for arn, times in execution_activity.items():
        settled_at = observed_time(indexed_settlements.get(("execution", arn)))
        if settled_at is None or settled_at < latest_time(times):
            errors.append(f"Retirement execution {arn} settled before its originally observed service activity")
    observed_invocation_ends = []
    for attempt in original_attempts:
        request_id = attempt.get("requestId")
        final_attempt = indexed.get(request_id, {})
        authoritative_end = (
            attempt.get("endedAtMsSource") in (None, "fixture") and final_attempt.get("endedAtMsSource") == "platform"
        )
        activity = [] if authoritative_end else [attempt.get("endedAtMs")]
        observed_invocation_ends.extend(activity)
        activity.extend(
            record.get("atMs")
            for record in (*before_events, *before_requests)
            if isinstance(record, Mapping) and record.get("requestId") == request_id
        )
        settled_at = observed_time(indexed_settlements.get(("invocation", request_id)))
        if settled_at is None or settled_at < latest_time(activity):
            errors.append("Retirement invocation settled before its originally observed activity")
    observed = [record.get("atMs") for record in (*before_events, *before_requests)]
    observed += observed_invocation_ends
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
            not is_json_integer(source["schemaVersion"]) or source["schemaVersion"] != SCHEMA_VERSION
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


def _cleanup_record_errors(sources: Sequence[Mapping[str, Any]], final_snapshot: Mapping[str, Any] | None) -> list[str]:
    """Reconcile every raw record's native owner, even for opaque event kinds."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    def identifier(value: Any) -> TypeGuard[str]:
        return isinstance(value, str) and bool(value.strip())

    def timestamp(value: Any) -> int | float | None:
        try:
            if type(value) in (int, float) and value >= 0 and math.isfinite(value):
                return value
        except OverflowError:
            pass
        return None

    bindings: dict[str, set[str]] = {}
    native = _native_intervals(sources)
    starts = {request: bounds["startedAtMs"][0] for request, bounds in native.items() if "startedAtMs" in bounds}
    ends = {request: bounds["endedAtMs"][0] for request, bounds in native.items() if "endedAtMs" in bounds}
    windows: list[int | float] = []
    for source in sources:
        end = timestamp(source.get("observationEndedAtMs"))
        if end is not None:
            windows.append(end)
        for attempt in records(source.get("attempts")):
            request_id = attempt.get("requestId")
            if not identifier(request_id):
                continue
            known = bindings.setdefault(request_id, set())
            execution_arn = attempt.get("executionArn")
            if identifier(execution_arn):
                known.add(execution_arn)
    known_executions = {execution for observed in bindings.values() for execution in observed}
    retirement = final_snapshot.get("retirement") if final_snapshot is not None else None
    settlements = {
        (item.get("kind"), item.get("id")): item
        for item in records(retirement.get("resources") if isinstance(retirement, Mapping) else None)
        if identifier(item.get("kind")) and identifier(item.get("id"))
    }

    for source in sources:
        collections = {name: records(source.get(name)) for name in ("events", "sdkRequests")}
        replay = source.get("replay")
        if isinstance(replay, Mapping):
            collections["replay.operationExecutions"] = records(replay.get("operationExecutions"))
            retry = replay.get("retry")
            if isinstance(retry, Mapping):
                collections["replay.retry.attempts"] = records(retry.get("attempts"))
        for collection, inventory in collections.items():
            for record in inventory:
                if collection.startswith("replay.") and not {
                    "requestId",
                    "ownerRequestId",
                    "executionArn",
                    "observedDuringRequestId",
                }.intersection(record):
                    continue  # Behavior-only partial records need no physical owner yet.
                field = {"events": "kind", "sdkRequests": "method"}.get(collection)
                if field is not None and field in record and not identifier(record[field]):
                    findings.append(f"INCOMPLETE: cleanup {collection}.{field} must be a non-empty string")
                if (
                    collection == "sdkRequests"
                    and "outcome" in record
                    and record["outcome"] not in ("DISPATCHED", "REJECTED")
                ):
                    findings.append("INCOMPLETE: cleanup sdkRequests.outcome must be DISPATCHED or REJECTED")
                observed_at = timestamp(record.get("atMs"))
                if "atMs" in record and observed_at is None:
                    findings.append(f"INCOMPLETE: cleanup {collection}.atMs must be a finite non-negative number")
                for key in ("requestId", "ownerRequestId", "executionArn", "observedDuringRequestId"):
                    if (
                        key in record
                        and not (key == "observedDuringRequestId" and record[key] is None)
                        and not identifier(record[key])
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection}.{key} must identify its native owner")
                if final_snapshot is None:
                    continue

                referenced_requests: set[str] = set()
                for key in ("requestId", "ownerRequestId", "observedDuringRequestId"):
                    request_id = record.get(key)
                    if not identifier(request_id):
                        continue
                    referenced_requests.add(request_id)
                    if len(bindings.get(request_id, set())) != 1:
                        findings.append(f"INCOMPLETE: cleanup {collection}.{key} has an unresolved native binding")
                request_id = record.get("requestId", record.get("ownerRequestId"))
                execution_arn = record.get("executionArn")
                if not referenced_requests and identifier(execution_arn):
                    candidates = {request for request, known in bindings.items() if known == {execution_arn}}
                    if len(candidates) == 1:
                        referenced_requests.update(candidates)
                        request_id = next(iter(candidates))
                if not referenced_requests:
                    findings.append(f"INCOMPLETE: cleanup {collection} has no resolved native owner")
                expected = bindings.get(request_id, set()) if identifier(request_id) else set()
                if identifier(execution_arn) and execution_arn not in known_executions:
                    findings.append(f"INCOMPLETE: cleanup {collection}.executionArn has an unresolved execution owner")
                elif identifier(execution_arn) and len(expected) == 1 and execution_arn not in expected:
                    findings.append(f"ASSERTION: cleanup {collection} is attributed to a different execution")
                owner = record.get("ownerRequestId")
                if (
                    identifier(record.get("requestId"))
                    and identifier(owner)
                    and len(bindings.get(record["requestId"], set())) == 1
                    and len(bindings.get(owner, set())) == 1
                    and owner != record["requestId"]
                ):
                    findings.append(f"ASSERTION: cleanup {collection} is attributed to a different invocation")

                # Event-only discoveries need settlement coverage too: the
                # snapshot containing an event may predate its native binding.
                for referenced in referenced_requests:
                    settlement = settlements.get(("invocation", referenced), {})
                    settled_at = timestamp(settlement.get("settledAtMs"))
                    if (
                        settlement.get("outcome") != "QUIESCENT"
                        or settled_at is None
                        or (observed_at is not None and settled_at < observed_at)
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection} native owner {referenced} is not settled")
                referenced_executions = {
                    execution for referenced in referenced_requests for execution in bindings.get(referenced, set())
                }
                if identifier(execution_arn):
                    referenced_executions.add(execution_arn)
                for execution in referenced_executions:
                    settlement = settlements.get(("execution", execution), {})
                    if (
                        settlement.get("outcome") not in TERMINAL_STATUSES
                        or timestamp(settlement.get("settledAtMs")) is None
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection} execution owner {execution} is not settled")

                if observed_at is None:
                    continue  # Omitted timestamp metadata can still be completed.
                started = starts.get(request_id) if identifier(request_id) else None
                if (windows and observed_at > max(windows)) or (started is not None and observed_at < started):
                    findings.append(
                        f"INCOMPLETE: cleanup {collection}.atMs lies outside the retained observation window"
                    )
                during = record.get("observedDuringRequestId")
                if identifier(during):
                    if during not in starts or during not in ends:
                        findings.append(
                            f"INCOMPLETE: cleanup {collection}.observedDuringRequestId lacks a native interval"
                        )
                    elif not starts[during] <= observed_at <= ends[during]:
                        findings.append(
                            f"ASSERTION: cleanup {collection}.atMs lies outside its observedDuringRequestId interval"
                        )
    return findings


def _retained_retirement_errors(sources: Sequence[Mapping[str, Any]], final: Mapping[str, Any]) -> list[str]:
    retirement = final.get("retirement")
    retirement = retirement if isinstance(retirement, Mapping) else {}
    retired_at = retirement.get("retiredAtMs")
    resources = {
        item["id"]: item
        for item in retirement.get("resources", [])
        if isinstance(item, Mapping) and isinstance(item.get("id"), str)
    }
    terminal_outcomes = {
        "invocation": ("QUIESCENT",),
        "execution": TERMINAL_STATUSES,
        "callback": ("SUCCEEDED", "FAILED", "TIMED_OUT"),
        "control": ("DELETED",),
    }
    faults = {
        item["id"]: item
        for item in retirement.get("faults", [])
        if isinstance(item, Mapping) and isinstance(item.get("id"), str)
    }
    events = normalize_retirement_events([*final.get("events", []), *retirement.get("completions", [])], [])
    _, errors = retained_fault_releases([*sources, final], retired_at)
    for source in sources:
        earlier = source.get("retirement")
        if not isinstance(earlier, Mapping):
            continue
        for row in earlier.get("resources", []):
            if not isinstance(row, Mapping) or not isinstance(row.get("id"), str):
                errors.append("Previously observed retirement resource lacks an identity")
                continue
            settled = resources.get(row["id"])
            if settled is None or any(key in row and row[key] != settled.get(key) for key in ("kind", "ownerCaseId")):
                errors.append(f"Final retirement omitted or changed observed resource {row['id']}")
            elif (
                settled.get("kind") in terminal_outcomes
                and settled.get("outcome") not in terminal_outcomes[settled["kind"]]
            ):
                errors.append(f"Final retirement did not settle observed resource {row['id']}")
            elif (
                isinstance(kind := settled.get("kind"), str)
                and row.get("outcome") in terminal_outcomes.get(kind, ())
                and row["outcome"] != settled.get("outcome")
            ):
                errors.append(f"Final retirement changed the terminal outcome of observed resource {row['id']}")
            at = row.get("settledAtMs")
            if isinstance(at, (int, float)) and (not isinstance(retired_at, (int, float)) or at > retired_at):
                errors.append(f"Observed settlement for {row['id']} follows final retirement")
        for row in earlier.get("faults", []):
            if not isinstance(row, Mapping) or not isinstance(row.get("id"), str):
                errors.append("Previously observed retirement fault lacks an identity")
                continue
            released = faults.get(row["id"])
            if (
                released is None
                or ("ownerCaseId" in row and row["ownerCaseId"] != released.get("ownerCaseId"))
                or not isinstance(released.get("releasedAtMs"), (int, float))
                or isinstance(released["releasedAtMs"], bool)
            ):
                errors.append(f"Final retirement omitted or changed observed fault {row['id']}")
        remaining = list(events)
        for completion in normalize_retirement_events(earlier.get("completions", []), errors):
            match = next(
                (
                    index
                    for index, event in enumerate(remaining)
                    if _owned_record_enriches(completion, event, event=True)
                ),
                None,
            )
            if match is None:
                errors.append("Retirement omitted or changed previously observed work completion")
            else:
                remaining.pop(match)
    return list(dict.fromkeys(errors))


def _ownership_claims(
    journal: list[dict[str, Any]], evidence: Any, retirement: Any, cleanup: Any, *, case_id: str
) -> set[tuple[str, str]]:
    sources = [{"attempts": journal}, evidence, retirement]
    if isinstance(cleanup, Mapping):
        sources.extend(snapshot for phase in cleanup.values() if isinstance(phase, list) for snapshot in phase)
    identities: set[tuple[str, str]] = set()
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        attempts = source.get("attempts")
        for attempt in attempts if isinstance(attempts, list) else []:
            if isinstance(attempt, Mapping):
                for field in ("requestId", "executionArn"):
                    if isinstance(identifier := attempt.get(field), str) and identifier.strip():
                        identities.add((field, identifier))
        histories = source.get("histories")
        for history in histories if isinstance(histories, list) else []:
            if (
                isinstance(history, Mapping)
                and history.get("ownerCaseId") == case_id
                and isinstance(arn := history.get("executionArn"), str)
                and arn.strip()
            ):
                identities.add(("executionArn", arn))
        retirement = source.get("retirement")
        inventories = [
            (source.get("resourcesCreated"), False),
            (source.get("faultsCreated"), True),
        ]
        if isinstance(retirement, Mapping):
            inventories.extend([(retirement.get("resources"), False), (retirement.get("faults"), True)])
        for resources, faults in inventories:
            for resource in resources if isinstance(resources, list) else []:
                if not isinstance(resource, Mapping) or not isinstance(resource.get("id"), str):
                    continue
                kind = "fault" if faults else resource.get("kind")
                if kind in ("invocation", "execution", "callback", "control", "fault") and resource["id"].strip():
                    field = {"invocation": "requestId", "execution": "executionArn"}.get(kind, kind)
                    identities.add((field, resource["id"]))
    return identities


async def run_scenarios(
    scenarios: Sequence[Scenario],
    profiles: Sequence[Profile],
    adapter: ScenarioAdapter,
    *,
    run_id: str,
    validator: EvidenceValidator | None = None,
    scenario_concurrency: int = 8,
    budgets: Budgets | None = None,
    scheduler: ResourceScheduler | None = None,
    on_result: Callable[[ScenarioResult], None] | None = None,
    on_attempt: Callable[[ScenarioContext], None] | None = None,
    validate_attempt: Callable[[Mapping[str, Any]], None] | None = None,
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
    completed: dict[str, ScenarioResult] = {}
    trusted_observations: set[str] = set()
    claim_owners: dict[tuple[str, str], set[str]] = {}
    case_profiles = {case.case_id: case.profile_id for case in scenarios}
    claim_conflicts: dict[str, set[str]] = {}

    def emit(result: ScenarioResult) -> ScenarioResult:
        for error in sorted(claim_conflicts.get(result.case_id, ())):
            if error not in result.errors:
                result.errors.append(error)
            result.status = "FAILED"
        completed[result.case_id] = result
        if on_result is not None:
            try:
                on_result(result)
            except Exception as exc:
                error = f"Result audit write failed: {type(exc).__name__}: {exc}"
                if error not in result.errors:
                    result.errors.append(error)
                if result.status == "COLLECTED":
                    result.status = "INFRASTRUCTURE_ERROR"
        return result

    def register_claims(case_id: str, claims: set[tuple[str, str]]) -> None:
        for kind, resource_id in sorted(claims):
            owners = claim_owners.setdefault((kind, resource_id), set())
            if case_id in owners:
                continue
            owners.add(case_id)
            if len(owners) == 1:
                continue
            error = f"INCOMPLETE: {kind} {resource_id} is claimed by multiple cases"
            for owner in sorted(owners):
                conflicts = claim_conflicts.setdefault(owner, set())
                if error in conflicts:
                    continue
                conflicts.add(error)
                target = profile_map[case_profiles[owner]].resource_id
                leases._quarantine_claim_conflict(target, error)
                if owner in completed:
                    emit(completed[owner])

    def record_attempt_claims(context: ScenarioContext) -> None:
        register_claims(
            context.scenario.case_id,
            _ownership_claims(context.attempts, None, None, None, case_id=context.scenario.case_id),
        )

    contexts = {
        case.case_id: ScenarioContext(
            run_id,
            case,
            profile_map[case.profile_id],
            _on_attempt=on_attempt,
            _on_claim=record_attempt_claims,
            _validate_attempt=validate_attempt,
        )
        for case in scenarios
    }
    run_locks: dict[str, asyncio.Lock] = {}
    adapter_preparation_started: set[str] = set()
    prepared: dict[str, asyncio.Task[tuple[list[str], dict[str, dict[str, float]]]]] = {}

    async def prepare(profile: Profile) -> tuple[list[str], dict[str, dict[str, float]]]:
        context = ScenarioContext(
            run_id, Scenario("deployment", "deployment", profile.profile_id, "deployment"), profile
        )
        residual: list[asyncio.Task[Any]] = []

        async def provision() -> None:
            adapter_preparation_started.add(profile.profile_id)
            leases._resources[profile.resource_id].profile_active = True
            await adapter.prepare(profile)

        try:
            run_locks[profile.profile_id] = await leases._acquire_run(profile.resource_id, timeout=limits.lease)
            async with leases._deployment_lease(profile.resource_id, f"{run_id}:deployment", timeout=limits.lease):
                try:
                    await _bounded(provision(), "provisioning", limits.provisioning, context, residual)
                finally:
                    leases.track_residual(profile.resource_id, residual)
            return [], context.timings
        except Exception as exc:
            return [f"provisioning: {type(exc).__name__}: {exc}"], context.timings

    # A failed target does not prevent another profile from preparing or running.
    for profile_id in {case.profile_id for case in scenarios}:
        prepared[profile_id] = asyncio.create_task(prepare(profile_map[profile_id]))

    cleanup_tasks: set[asyncio.Task[Any] | None] = set()

    async def run_case(case: Scenario) -> ScenarioResult:
        context = contexts[case.case_id]
        admitted_identity = admission_identities[case.case_id]
        evidence: dict[str, Any] = {}
        observation_baselines: list[dict[str, Any]] = []
        retirement_evidence: dict[str, Any] = {}
        cleanup_evidence: dict[str, list[dict[str, Any]]] = {}
        cleanup_changed = False
        collected_normally = False
        errors: list[str] = []
        context_retention_failed = False
        context_observations: dict[int, tuple[Any, dict[str, Any]]] = {}
        status = "COLLECTED"
        residual: list[asyncio.Task[Any]] = []
        lease: Lease | None = None
        cancelled = False

        def record_observations(snapshot: dict[str, Any]) -> set[tuple[str, ...]]:
            if observation_baselines and same_json(observation_baselines[-1], snapshot):
                return set()
            register_claims(case.case_id, _ownership_claims([], snapshot, None, None, case_id=case.case_id))
            order_conflicts: set[tuple[str, ...]] = {
                (name, "sequence")
                for name, order_changed in (
                    ("sdkRequests", _sdk_frames_order_changed),
                    ("events", _event_frames_order_changed),
                )
                if any(order_changed(previous, snapshot) for previous in observation_baselines)
            }
            # Use raw source orders, before delta extraction or inventory merging:
            # merging disjoint fragments must not invent order between them.
            observation_baselines.append(snapshot)
            return order_conflicts

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
                    previous_observation = context_observations.get(id(source))
                    if previous_observation is not None and same_json(previous_observation[1], source):
                        continue
                    snapshot = copy.deepcopy(dict(source))
                    order_conflicts = record_observations(snapshot) if snapshot else set()
                    # Keep the object alive with its last snapshot: an unchanged
                    # supplied object can legitimately predate newer context data.
                    context_observations[id(source)] = (source, snapshot)
                    updates = (
                        snapshot
                        if previous_observation is None
                        else _observation_updates(previous_observation[1], snapshot)
                    )
                    retained, changes = _retain_observations(retained, updates, compare_sdk_order=False)
                    changes.conflicts.update(order_conflicts)
                    physical_conflicts = {path for path in changes.conflicts if path[:2] != ("replay", "sideEffects")}
                    if physical_conflicts:
                        paths = ", ".join(".".join(path) for path in sorted(physical_conflicts))
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
            order_conflicts = record_observations(copy.deepcopy(dict(collected)))
            # Returned observations are newest, so valid sequence enrichment and
            # monotonic completion win after all input-object discoveries survive.
            evidence, changed = _retain_observations(evidence, collected, compare_sdk_order=False)
            context.evidence = copy.deepcopy(evidence)
            changed.conflicts.update(order_conflicts)
            behavior_changes = {
                path for path in changed.omitted | changed.conflicts if path[:2] == ("replay", "sideEffects")
            }
            changed.omitted.difference_update(behavior_changes)
            changed.conflicts.difference_update(behavior_changes)
            collected_normally = True
            ownership_errors = _ownership_errors(context, evidence)
            if changed:
                ownership_errors.append("Collection omitted or changed retained observations")
            if context_retention_failed:
                ownership_errors.append("Context did not retain trustworthy observations")
            errors.extend(ownership_errors)
            if errors:
                status = "INCOMPLETE_EVIDENCE"
            else:
                trusted_observations.add(case.case_id)
            if behavior_changes:
                errors.append("INCOMPLETE: collection omitted or changed retained side-effect observations")
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
                cleanup_tasks.add(asyncio.current_task())
                retired = False
                physical_retirement_verified = False
                cleanup_errors: list[str] = []
                frozen_cleanup_identity: dict[str, Any] = {}
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
                                register_claims(
                                    case.case_id, _ownership_claims([], snapshot, None, None, case_id=case.case_id)
                                )
                                check_cleanup_identity(snapshot, phase)
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
                if not collected_normally and not cancelled and not residual and callable(partial):
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
                    except (Exception, asyncio.CancelledError) as exc:
                        errors.append(f"Partial observation collection: {type(exc).__name__}: {exc}")
                        cleanup_errors.append("Partial collection did not retain trustworthy observations")
                    finally:
                        # Preserve mutations to the supplied object even when
                        # the collector replaced the attribute before returning.
                        try:
                            evidence = retain_context(partial_baseline, partial_input, "partial collection")
                            if partial_captured is not None:
                                partial_order_conflicts = record_observations(partial_captured)
                                evidence, changed = _retain_observations(
                                    evidence, partial_captured, compare_sdk_order=False
                                )
                                changed.conflicts.update(partial_order_conflicts)
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
                    except Exception as exc:
                        errors.append(f"Context observation validation: {type(exc).__name__}: {exc}")
                        cleanup_errors.append("Context did not retain trustworthy observations")
                try:
                    frozen_cleanup_identity = copy.deepcopy(
                        {key: evidence[key] for key in ("runtime", "artifact", "profile") if key in evidence}
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
                    leases.track_residual(lease.resource_id, residual)
                    cleanup_errors.append("A bounded phase was interrupted; residual work requires reconciliation")
                normal_retirement = evidence
                try:
                    if not isinstance(context.evidence, Mapping):
                        raise ValueError("Cleanup observations must remain an evidence object")
                    normal_retirement = (
                        {**evidence, "retirement": copy.deepcopy(context.evidence["retirement"])}
                        if evidence and not cleanup_changed and "retirement" in context.evidence
                        else evidence
                    )
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"Cleanup evidence retention: {type(exc).__name__}: {exc}")
                cleanup_snapshots = [item for snapshots in cleanup_evidence.values() for item in snapshots]
                retirement_claims = any(
                    isinstance(retirement := frame.get("retirement"), Mapping)
                    and any(retirement.get(field) for field in ("resources", "faults", "completions"))
                    for frame in [*observation_baselines, evidence, *cleanup_snapshots]
                )
                owned_work = (
                    retirement_claims
                    or context.attempts
                    or context.retirement_evidence
                    or cleanup_changed
                    or any(
                        snapshot.get(name) for snapshot in [evidence, *cleanup_snapshots] for name in owned_inventories
                    )
                )
                supplied_retirement = context.retirement_evidence
                if not isinstance(supplied_retirement, Mapping):
                    cleanup_errors.append("Independent retirement snapshot must be an object")
                if owned_work and (status != "COLLECTED" or supplied_retirement or cleanup_changed):
                    try:
                        retirement_evidence = (
                            copy.deepcopy(dict(supplied_retirement)) if isinstance(supplied_retirement, Mapping) else {}
                        )
                        if not retirement_evidence and status == "COLLECTED" and cleanup_changed:
                            if not isinstance(context.evidence, Mapping):
                                raise ValueError("Normal cleanup did not retain a complete retirement snapshot")
                            retirement_evidence = copy.deepcopy(dict(context.evidence))
                        if not retirement_evidence:
                            work = "Launched work" if context.attempts else "Owned work"
                            cleanup_errors.append(f"{work} has no independent retirement evidence")
                        else:
                            check_cleanup_identity(retirement_evidence, "retirement snapshot")
                            observed_frames = [
                                *observation_baselines,
                                evidence,
                                *cleanup_snapshots,
                                retirement_evidence,
                            ]
                            native = _native_intervals(observed_frames)
                            ownership_retirement = copy.deepcopy(retirement_evidence)
                            for attempt in ownership_retirement.get("attempts", []):
                                if not isinstance(attempt, dict) or not isinstance(attempt.get("requestId"), str):
                                    continue
                                for field, (measured_at, platform) in native.get(attempt["requestId"], {}).items():
                                    if (
                                        platform
                                        and attempt.get(field) == measured_at
                                        and attempt.get(f"{field}Source") in (None, "fixture")
                                    ):
                                        attempt[f"{field}Source"] = "platform"
                            audit = [
                                error
                                for baseline in [*observation_baselines, evidence, *cleanup_snapshots]
                                for error in _retirement_ownership_errors(
                                    context, baseline, ownership_retirement, compare_order=baseline is not evidence
                                )
                            ]
                            audit.extend(
                                validate_retirement_evidence(
                                    retirement_evidence, allow_no_launches=not context.attempts
                                )
                            )
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
                    retirement_evidence or normal_retirement,
                ):
                    if finding not in cleanup_errors:
                        cleanup_errors.append(finding)
                cleanup_errors.extend(
                    issue
                    for issue in _cleanup_record_errors(
                        [*observation_baselines, evidence, *cleanup_snapshots, retirement_evidence],
                        retirement_evidence or normal_retirement,
                    )
                    if issue.startswith("INCOMPLETE:")
                )
                try:
                    cleanup_errors.extend(
                        _retained_retirement_errors(
                            [*observation_baselines, evidence, *cleanup_snapshots],
                            retirement_evidence or normal_retirement,
                        )
                    )
                except Exception as exc:
                    cleanup_errors.append(f"Retirement observation reconciliation: {type(exc).__name__}: {exc}")
                if owned_work and not physical_retirement_verified:
                    if evidence:
                        audit = _ownership_errors(context, evidence, complete_cohort=False)
                        audit.extend(validate_observed_retirement(normal_retirement))
                        errors.extend(error for error in audit if error not in errors)
                    else:
                        audit = ["Owned work has no captured retirement evidence"]
                        errors.extend(audit)
                    if audit:
                        cleanup_errors.append("Final retirement evidence did not establish quiescence")
                if not retirement_evidence and normal_retirement is not evidence:
                    retirement_evidence = normal_retirement
                if cleanup_errors:
                    if status == "COLLECTED":
                        status = "INFRASTRUCTURE_ERROR"
                    errors.extend(cleanup_errors)
                register_claims(
                    case.case_id,
                    _ownership_claims(
                        context.attempts, evidence, retirement_evidence, cleanup_evidence, case_id=case.case_id
                    ),
                )
                await lease.release(quiescent=retired and not cleanup_errors and case.case_id not in claim_conflicts)
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
                observation_evidence=observation_baselines,
                trusted_observations=case.case_id in trusted_observations,
            )
        )
        if cancelled:
            raise asyncio.CancelledError
        return result

    tasks_by_profile: dict[str, list[asyncio.Task[ScenarioResult]]] = {}
    tasks = []
    for case in scenarios:
        case_task = asyncio.create_task(run_case(case))
        tasks_by_profile.setdefault(case.profile_id, []).append(case_task)
        tasks.append(case_task)
    case_results = asyncio.gather(*tasks)

    finishing_profiles: set[asyncio.Task[Any]] = set()

    async def finish_profile(profile_id: str, case_tasks: list[asyncio.Task[ScenarioResult]]) -> None:
        await asyncio.gather(prepared[profile_id], *case_tasks, return_exceptions=True)
        run_lock = run_locks.get(profile_id)
        if run_lock is None:
            return
        if profile_id not in adapter_preparation_started:
            run_lock.release()
            return
        profile = profile_map[profile_id]
        context = ScenarioContext(run_id, Scenario("finish", "finish", profile_id, "finish"), profile)
        quiet = leases.quarantine_reason(profile.resource_id) is None
        task = asyncio.current_task()
        assert task is not None
        finishing_profiles.add(task)
        residual: list[asyncio.Task[Any]] = []
        try:
            await _bounded(
                adapter.finish(profile, quiescent=quiet), "profileRetirement", limits.retirement, context, residual
            )
        except (Exception, asyncio.CancelledError) as exc:
            await leases.quarantine(profile.resource_id, f"Run ownership release failed: {exc}")
            for result in completed.values():
                if result.profile_id == profile_id:
                    if result.status == "COLLECTED":
                        result.status = "INFRASTRUCTURE_ERROR"
                    result.errors.append(f"Run ownership release failed: {type(exc).__name__}: {exc}")
        finally:
            # The target's case resource remains quarantined independently;
            # a later run can acquire this mutex but cannot deploy or admit.
            finishing_profiles.discard(task)
            leases.track_residual(profile.resource_id, residual)
            leases._resources[profile.resource_id].profile_active = False
            run_lock.release()
            for result in completed.values():
                if result.profile_id == profile_id:
                    result.timings.update(copy.deepcopy(context.timings))
                    emit(result)

    async def drain_and_finish() -> None:
        finished = asyncio.gather(
            *(finish_profile(profile_id, case_tasks) for profile_id, case_tasks in tasks_by_profile.items())
        )
        cancellation: asyncio.CancelledError | None = None
        while not finished.done():
            try:
                await asyncio.shield(finished)
            except asyncio.CancelledError as exc:
                cancellation = exc
                for task in tuple(finishing_profiles):
                    if not task.cancelling():
                        task.cancel()
        finished.result()
        if cancellation is not None:
            raise cancellation

    draining = asyncio.create_task(drain_and_finish(), name="drain_and_finish")
    try:
        outcomes = list(await asyncio.shield(case_results))
    finally:
        for task in (*tasks, *prepared.values()):
            # A case already in bounded cleanup must be allowed to retire.
            if task not in cleanup_tasks and not task.done() and not task.cancelling():
                task.cancel()
        cancellation: asyncio.CancelledError | None = None
        while not draining.done():
            try:
                await asyncio.shield(draining)
            except asyncio.CancelledError as exc:
                cancellation = exc
        if case_results.done() and not case_results.cancelled():
            case_results.exception()
        draining.result()
        if cancellation is not None:
            raise cancellation

    if validator is not None:
        from aws_durable_execution_conformance_tests.lmi.offline import adjudicate_evidence

        for result in outcomes:
            if result.evidence:
                capture_status = result.status
                result.status, result.errors, projected = adjudicate_evidence(
                    capture_status,
                    result.errors,
                    result.evidence,
                    retirement_evidence=result.retirement_evidence,
                    cleanup_evidence=result.cleanup_evidence,
                    observation_evidence=result.observation_evidence,
                    attempts=result.attempts,
                    validator=validator,
                    standard=contexts[result.case_id].scenario.coverage_profile == "standard",
                    trusted_observations=result.trusted_observations,
                )
                result.evidence = projected
                emit(result)
    return outcomes
