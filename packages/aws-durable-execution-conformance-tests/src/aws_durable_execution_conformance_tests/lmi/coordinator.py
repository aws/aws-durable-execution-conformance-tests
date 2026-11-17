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
from typing import Any, Protocol

from aws_durable_execution_conformance_tests.lmi.evidence import (
    HISTORY_STATUSES,
    TERMINAL_STATUSES,
    EvidenceValidator,
    same_json,
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
                for name, label in (("requestId", "invocation request ID"), ("executionArn", "durable execution")):
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
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait((task,), timeout=budget)
        if not done:
            raise PhaseTimeout(f"{name} exceeded its {budget:g}s harness budget")
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
            journal = {item["requestId"]: item for item in context.attempts}
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
        selected = evidence.get("selectedRequests")
        originals = {item["role"]: item.get("requestId") for item in context.attempts if item["attempt"] == 1}
        if (
            not isinstance(selected, Mapping)
            or set(selected) != set(originals)
            or (complete_cohort and set(originals) != set(context.scenario.roles))
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


def _retry_attempt_order(evidence: Mapping[str, Any]) -> tuple[str, ...] | None:
    replay = evidence.get("replay")
    retry = replay.get("retry") if isinstance(replay, Mapping) else None
    attempts = retry.get("attempts") if isinstance(retry, Mapping) else None
    if not isinstance(attempts, list):
        return None
    identifiers = []
    for attempt in attempts:
        identifier = attempt.get("attemptId") if isinstance(attempt, Mapping) else None
        if not isinstance(identifier, str) or not identifier.strip():
            return None
        identifiers.append(identifier)
    # Singleton fragments establish no ordering constraint. The caller diagnoses
    # duplicate identities before using an observed sequence as an ordering.
    if len(identifiers) < 2:
        return None
    return tuple(identifiers)


def _side_effect_observation_errors(evidence: Mapping[str, Any]) -> list[str]:
    replay = evidence.get("replay")
    counts = replay.get("sideEffects") if isinstance(replay, Mapping) else None
    if not isinstance(counts, list):
        return []
    errors = []
    seen: set[str] = set()
    for count in counts:
        if not isinstance(count, Mapping):
            continue
        operation_id = count.get("operationId")
        if isinstance(operation_id, str):
            if operation_id in seen:
                errors.append(f"INCOMPLETE: duplicate side-effect operationId {operation_id}")
            seen.add(operation_id)
        for counter_field in ("count", "expectedCount"):
            if counter_field in count and (type(count[counter_field]) is not int or count[counter_field] < 0):
                errors.append(f"INCOMPLETE: side-effect {counter_field} must be a non-negative integer")
    return errors


def _observation_advanced(previous: Any, current: Any, path: tuple[str, ...]) -> bool:
    if path == ("histories", "status"):
        return previous in ("RUNNING", "PENDING") and current in HISTORY_STATUSES
    if path[:-1] in _OPERATION_RECORD_PATHS and path[-1:] == ("status",):
        return previous in _OPERATION_NONTERMINAL_STATUSES and current in (
            *_OPERATION_NONTERMINAL_STATUSES,
            *_OPERATION_TERMINAL_STATUSES,
        )
    if path == ("replay", "sideEffects", "count"):
        return type(previous) is int and type(current) is int and 0 <= previous <= current
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
            if "sequence" in record and (type(record["sequence"]) is not int or record["sequence"] < 0):
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
    omitted: bool = False
    conflicts: set[tuple[str, ...]] = field(default_factory=set)

    def __bool__(self) -> bool:
        return self.omitted or bool(self.conflicts)

    def merge(self, other: _RetentionChanges) -> None:
        self.omitted |= other.omitted
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
            changes.omitted = True
        elif isinstance(record, Mapping):
            for key, value in record.items():
                if key not in retained[match]:
                    retained[match][key] = copy.deepcopy(value)
                    changes.omitted = True
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
    original: Mapping[str, Any], captured: Mapping[str, Any], path: tuple[str, ...] = ()
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
                    changed.omitted = True
                retained[key] = copy.deepcopy(previous)
            continue
        if key not in retained:
            retained[key] = copy.deepcopy(previous)
            changed.omitted = True
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
                isinstance(record, Mapping) and type(record.get("sequence")) is int
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
                                _owned_record_enriches(record, retained[key][index], event=events)
                                or _owned_record_enriches(retained[key][index], record, event=events)
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
                    changed.omitted = True
                else:
                    remaining.remove(match)
                    if owned_records and isinstance(record, Mapping):
                        for metadata in (
                            ("observedDuringRequestId", "sequence") if events else ("observedDuringRequestId",)
                        ):
                            if metadata == "observedDuringRequestId" and record.get(metadata) is None:
                                continue
                            if metadata in record and (
                                metadata not in retained[key][match]
                                or (metadata == "observedDuringRequestId" and retained[key][match][metadata] is None)
                            ):
                                retained[key][match][metadata] = copy.deepcopy(record[metadata])
                                changed.omitted = True
                    if events and isinstance(record, Mapping):
                        old_sequence, new_sequence = record.get("sequence"), retained[key][match].get("sequence")
                        request_id = record.get("requestId")
                        if isinstance(request_id, str) and type(old_sequence) is int and type(new_sequence) is int:
                            event_order.setdefault(request_id, []).append((old_sequence, new_sequence))
                    if identity is not None and isinstance(record, Mapping):
                        retained[key][match], nested_changed = _retain_observations(
                            record, retained[key][match], (*path, key)
                        )
                        changed.merge(nested_changed)
            for order in event_order.values():
                # Numerical renumbering is harmless; reversing known local
                # event order would erase a lifecycle failure before its audit.
                if any(first[0] < second[0] and first[1] >= second[1] for first, second in pairwise(sorted(order))):
                    changed.conflicts.add((*path, key, "sequence"))
        elif not same_json(previous, retained[key]) and not _observation_advanced(
            previous, retained[key], (*path, key)
        ):
            retained[key] = copy.deepcopy(previous)
            changed.conflicts.add((*path, key))
    return retained, changed


def _retirement_ownership_errors(
    context: ScenarioContext, original: Mapping[str, Any], final: Mapping[str, Any]
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
            elif (key == "startedAtMs" and after > before) or (key == "endedAtMs" and after < before):
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
    for history in original.get("histories", []):
        if not isinstance(history, Mapping):
            continue  # Already reported as an unidentified original inventory row.
        arn = history.get("executionArn")
        if not isinstance(arn, str) or not arn.strip():
            continue
        completed_history = final_histories.get(arn)
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
            if type(sequence) is not int or sequence < 0:
                errors.append("Retirement cannot reconcile a malformed semantic sequence")
                continue
            by_request.setdefault(event.get("requestId"), []).append((sequence, event_identity(event)))
        return by_request

    # Sequence values can change when additional observations are collected.
    # Preserve the known relative order, particularly at equal timestamps;
    # partial events without sequence numbers do not yet establish that order.
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
        match = next((index for index, item in enumerate(remaining) if _owned_record_enriches(request, item)), None)
        if match is None:
            errors.append("Retirement omitted or changed a previously observed SDK request")
        else:
            remaining.pop(match)

    def latest_time(values: Sequence[Any]) -> float:
        return max(
            (
                value
                for value in values
                if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
            ),
            default=0,
        )

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


def _cleanup_retry_errors(
    frozen_replay: Mapping[str, Any],
    sources: Sequence[Mapping[str, Any]],
    execution_arn: Any,
    bound_execution: Callable[[Any], str | None],
    *,
    final: bool,
) -> list[str]:
    """Check retry claims without promoting partial fragments to whole runs."""
    findings: list[str] = []
    frozen_retry = frozen_replay.get("retry")
    if not isinstance(frozen_retry, Mapping) or not isinstance(frozen_retry.get("operationId"), str):
        return findings
    operation_id = frozen_retry["operationId"]
    frozen_attempts = frozen_retry.get("attempts", [])
    frozen_ids = (
        {
            item["attemptId"]
            for item in frozen_attempts
            if isinstance(item, Mapping) and isinstance(item.get("attemptId"), str)
        }
        if isinstance(frozen_attempts, list)
        else set()
    )
    facts: dict[str, dict[str, Any]] = {}
    orders: list[list[str]] = []
    entries: dict[str, list[Mapping[str, Any]]] = {}
    for source in sources:
        replay = source.get("replay")
        if not isinstance(replay, Mapping):
            continue
        observed_entries = replay.get("operationExecutions", [])
        if isinstance(observed_entries, list):
            for entry in observed_entries:
                if (
                    isinstance(entry, Mapping)
                    and entry.get("operationId") == operation_id
                    and isinstance(entry.get("attemptId"), str)
                ):
                    entries.setdefault(entry["attemptId"], []).append(entry)
        if "retry" not in replay:
            continue
        retry = replay["retry"]
        if not isinstance(retry, Mapping):
            if final:
                findings.append("ASSERTION: cleanup retry observations must be an object")
            continue
        same_operation = "operationId" not in retry or same_json(retry["operationId"], operation_id)
        if not same_operation:
            findings.append("ASSERTION: cleanup retry operationId changed the frozen retry operation")
        attempts = retry.get("attempts", [])
        if not isinstance(attempts, list):
            if final:
                findings.append("ASSERTION: cleanup retry attempts must be an array")
            continue
        order = []
        for attempt in attempts:
            if not isinstance(attempt, Mapping):
                if final:
                    findings.append("ASSERTION: cleanup retry attempt must be an object")
                continue
            if "requestId" in attempt:
                bound_execution(attempt["requestId"])
            if not same_operation:
                continue
            attempt_id = attempt.get("attemptId")
            if not isinstance(attempt_id, str) or not attempt_id.strip():
                if final:
                    findings.append("ASSERTION: cleanup retry attempt lacks a non-empty attemptId")
                continue
            order.append(attempt_id)
            known = facts.setdefault(attempt_id, {})
            for key in ("requestId", "outcome"):
                if key not in attempt:
                    continue
                if key in known and not same_json(known[key], attempt[key]):
                    findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} changed its known {key}")
                else:
                    known[key] = attempt[key]
        if len(set(order)) != len(order):
            findings.append("ASSERTION: cleanup retry observations repeat an attemptId")
        elif order:
            orders.append(order)

    # Each source contributes only its observed ordering edges. In particular,
    # separate singleton fragments do not imply a temporal relationship.
    successors: dict[str, set[str]] = {attempt_id: set() for attempt_id in facts}
    indegree = dict.fromkeys(facts, 0)
    for order in orders:
        for before, after in pairwise(order):
            if after not in successors[before]:
                successors[before].add(after)
                indegree[after] += 1
    ready = deque(attempt_id for attempt_id, degree in indegree.items() if degree == 0)
    visited = 0
    while ready:
        attempt_id = ready.popleft()
        visited += 1
        for following in successors[attempt_id]:
            indegree[following] -= 1
            if indegree[following] == 0:
                ready.append(following)
    if visited != len(facts):
        findings.append("ASSERTION: cleanup retry attempt order contradicts retained observations")

    if not final:
        return findings
    for attempt_id, known in facts.items():
        candidates = entries.get(attempt_id, [])
        request_id = known.get("requestId")
        if "requestId" not in known:
            candidate_requests = {
                item["requestId"]
                for item in candidates
                if isinstance(item.get("requestId"), str) and item["requestId"].strip()
            }
            if len(candidate_requests) == 1:
                request_id = next(iter(candidate_requests))
            else:
                if not candidate_requests:
                    bound_execution(None)
                findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} has unresolved native correlation")
                continue
        owner = bound_execution(request_id)
        if owner is None:
            continue
        if owner != execution_arn:
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} did not run on the suspended execution")
        matching = [item for item in candidates if item.get("requestId") == request_id]
        if not matching:
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} lacks a correlated user-code entry")
            continue
        outcomes = [item["outcome"] for item in matching if "outcome" in item]
        if "outcome" not in known and outcomes and all(same_json(item, outcomes[0]) for item in outcomes):
            known["outcome"] = outcomes[0]
        if "outcome" in known and any(not same_json(known["outcome"], item) for item in outcomes):
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} contradicts its user-code outcome")
    for order in orders:
        # A subset is not a new declaration of the full retry's endpoints.
        if (
            frozen_ids
            and frozen_ids <= set(order)
            and (facts[order[0]].get("outcome") != "FAILED" or facts[order[-1]].get("outcome") != "SUCCEEDED")
        ):
            findings.append("ASSERTION: cleanup retry observations do not start with failure and end with success")
    return findings


def _cleanup_checkpoint_errors(
    frozen_replay: Mapping[str, Any], sources: Sequence[Mapping[str, Any]], execution_arn: Any, *, final: bool
) -> tuple[list[str], set[str]]:
    """Reconcile supplied checkpoint facts without replacing partial views."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    frozen_before = {
        item["id"]: item for item in records(frozen_replay.get("before")) if isinstance(item.get("id"), str)
    }
    frozen_after = {item["id"]: item for item in records(frozen_replay.get("after")) if isinstance(item.get("id"), str)}
    before_ids: set[str] = set(frozen_before)
    after_ids: set[str] = set(frozen_after)
    # The accepted original after snapshot was checked against service history.
    history_ids: set[str] = set(frozen_after)
    observations: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    for source in sources:
        replay = source.get("replay")
        if isinstance(replay, Mapping):
            for view in ("before", "after"):
                if view not in replay:
                    continue
                operations = replay[view]
                if not isinstance(operations, list):
                    if final:
                        findings.append(f"ASSERTION: cleanup replay.{view} checkpoints must be an array")
                    continue
                seen: set[str] = set()
                for operation in operations:
                    operation_id = operation.get("id") if isinstance(operation, Mapping) else None
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append(f"ASSERTION: cleanup replay.{view} checkpoint lacks an operation ID")
                        continue
                    if operation_id in seen:
                        findings.append(f"ASSERTION: cleanup replay.{view} repeats checkpoint {operation_id}")
                    seen.add(operation_id)
                    (before_ids if view == "before" else after_ids).add(operation_id)
                    observations.setdefault(operation_id, []).append((view, operation))
                    if (
                        view == "before"
                        and "status" in operation
                        and operation["status"] not in _OPERATION_TERMINAL_STATUSES
                    ):
                        findings.append(f"ASSERTION: cleanup replay.before checkpoint {operation_id} is not completed")
                    prior = frozen_before.get(operation_id, frozen_after.get(operation_id))
                    if prior is not None:
                        path = ("replay", "before" if view == "before" or operation_id in frozen_before else "after")
                        _, changes = _retain_observations(prior, operation, path)
                        if changes.conflicts:
                            findings.append(
                                f"ASSERTION: cleanup replay.{view} changed frozen checkpoint {operation_id}"
                            )
        for history in records(source.get("histories")):
            if history.get("executionArn") != execution_arn:
                continue
            for operation in records(history.get("operations")):
                operation_id = operation.get("id")
                if isinstance(operation_id, str) and operation_id.strip():
                    history_ids.add(operation_id)
                    observations.setdefault(operation_id, []).append(("history", operation))

    for operation_id, observed in observations.items():
        identity: dict[str, Any] = {"id": operation_id}
        terminal: list[Mapping[str, Any]] = []
        for view, operation in observed:
            for key in ("name", "type", "parentId"):
                if key in operation:
                    if key in identity and not same_json(identity[key], operation[key]):
                        findings.append(f"ASSERTION: cleanup checkpoint {operation_id} changed its {key}")
                    else:
                        identity[key] = operation[key]
            if view == "before" or operation.get("status") in _OPERATION_TERMINAL_STATUSES:
                terminal.append(operation)
            if (
                view == "after"
                and operation_id in before_ids
                and "status" in operation
                and operation["status"] not in _OPERATION_TERMINAL_STATUSES
            ):
                findings.append(
                    f"ASSERTION: cleanup after checkpoint {operation_id} is incomplete despite before completion"
                )
        for index, operation in enumerate(terminal):
            for previous in terminal[:index]:
                _, changes = _retain_observations(previous, operation, ("replay", "before"))
                if changes.conflicts:
                    findings.append(
                        f"ASSERTION: cleanup completed checkpoint {operation_id} has contradictory observations"
                    )
            for view, partial in observed:
                if view != "history" and "status" not in partial:
                    _, changes = _retain_observations(operation, partial, ("replay", "before"))
                    if changes.conflicts:
                        findings.append(
                            f"ASSERTION: cleanup partial checkpoint {operation_id} contradicts its completed outcome"
                        )
    retry = frozen_replay.get("retry")
    if isinstance(retry, Mapping) and isinstance(retry.get("operationId"), str) and retry["operationId"] in before_ids:
        findings.append("ASSERTION: cleanup replay.before checkpoint contains the frozen retry operation")
    if final:
        for operation_id in before_ids - after_ids:
            findings.append(
                f"ASSERTION: cleanup before checkpoint {operation_id} lacks an after-checkpoint correlation"
            )
        for operation_id in after_ids - history_ids:
            findings.append(f"ASSERTION: cleanup after checkpoint {operation_id} lacks a service-history correlation")
    return findings, before_ids


def _cleanup_behavior_errors(
    original: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    standard: bool,
    known_snapshots: Sequence[Mapping[str, Any]] = (),
    final: bool = False,
) -> list[str]:
    """Detect specific refutations without replacing frozen business evidence."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    def timestamp(value: Any) -> float | None:
        if type(value) not in (int, float) or value < 0:
            return None
        try:
            return float(value) if math.isfinite(value) else None
        except OverflowError:
            return None

    attempts = {
        item["requestId"]: item for item in records(original.get("attempts")) if isinstance(item.get("requestId"), str)
    }
    events = records(original.get("events"))
    admission_sources = (original, *known_snapshots, snapshot)
    releases: list[float] = []
    # Absence can be completed later; an explicitly malformed release time is
    # invalid evidence even if a later snapshot replaces or omits that field.
    for source in admission_sources:
        fault_collections = [("faultsCreated", source.get("faultsCreated"))]
        retirement = source.get("retirement")
        if isinstance(retirement, Mapping):
            fault_collections.append(("retirement.faults", retirement.get("faults")))
        for location, collection in fault_collections:
            for fault in records(collection):
                if "releasedAtMs" not in fault:
                    continue
                released = timestamp(fault["releasedAtMs"])
                if released is None:
                    findings.append(f"INCOMPLETE: cleanup {location}.releasedAtMs must be a finite non-negative number")
                else:
                    releases.append(released)
    known_requests = {
        attempt["requestId"]
        for source in admission_sources
        for attempt in records(source.get("attempts"))
        if isinstance(attempt.get("requestId"), str)
    }
    closures: dict[str, float] = {}
    # A later observation can reveal an earlier closure. Preserve every known
    # boundary and recheck old activity as well as newly discovered requests.
    for event in (event for source in admission_sources for event in records(source.get("events"))):
        request_id, time = event.get("requestId"), timestamp(event.get("atMs"))
        if (
            isinstance(request_id, str)
            and request_id in known_requests
            and time is not None
            and event.get("kind") in ("wrapper_return", "admission_closed")
        ):
            closures[request_id] = min(closures.get(request_id, time), time)
    for collection in ("sdkRequests", "events"):
        for record in (record for source in admission_sources for record in records(source.get(collection))):
            if collection == "events" and record.get("kind") != "admission_probe":
                continue
            request_id, time = record.get("requestId"), timestamp(record.get("atMs"))
            closed = closures.get(request_id) if isinstance(request_id, str) else None
            if closed is None or time is None or time < closed:
                continue
            if collection == "sdkRequests" and record.get("outcome") == "DISPATCHED":
                finding = f"ASSERTION: request {request_id} dispatched SDK work after admission closed"
            elif (
                collection == "events"
                and record.get("probe") in ("operation", "checkpoint")
                and record.get("outcome") != "REJECTED"
            ):
                finding = f"ASSERTION: request {request_id} admitted a {record['probe']} probe after admission closed"
            else:
                continue
            if finding not in findings:
                findings.append(finding)
    if not standard:
        return findings

    selected = original.get("selectedRequests")
    if not isinstance(selected, Mapping):
        return findings
    current_attempts = {
        item["requestId"]: item for item in records(snapshot.get("attempts")) if isinstance(item.get("requestId"), str)
    }

    def role(role_name: str) -> Mapping[str, Any]:
        request_id = selected.get(role_name)
        return attempts.get(request_id, {}) if isinstance(request_id, str) else {}

    def boundary(role_name: str, kind: str) -> float | None:
        request_id = role(role_name).get("requestId")
        matches = [
            event
            for event in events
            if request_id is not None and event.get("requestId") == request_id and event.get("kind") == kind
        ]
        return timestamp(matches[0].get("atMs")) if len(matches) == 1 else None

    def native_time(role_name: str, key: str) -> float | None:
        previous = role(role_name)
        current = current_attempts.get(previous.get("requestId"), {})
        return timestamp(current[key] if key in current else previous.get(key))

    def changed_time(role_name: str, key: str) -> bool:
        previous = role(role_name)
        current = current_attempts.get(previous.get("requestId"), {})
        return key in current and timestamp(current[key]) is not None and not same_json(current[key], previous.get(key))

    def observed_events(role_name: str, kind: str) -> list[Mapping[str, Any]]:
        owner = role(role_name)
        request_id = owner.get("requestId")
        observed = []
        for source in admission_sources:
            for event in records(source.get("events")):
                if request_id is None or event.get("requestId") != request_id or event.get("kind") != kind:
                    continue
                if any(
                    key in event and not same_json(event[key], expected)
                    for key, expected in (
                        ("ownerRequestId", request_id),
                        ("executionArn", owner.get("executionArn")),
                    )
                ):
                    if final:
                        findings.append(f"INCOMPLETE: cleanup {kind} event has inconsistent native ownership")
                    continue
                if "atMs" in event and timestamp(event["atMs"]) is None and final:
                    findings.append(f"INCOMPLETE: cleanup {kind} event has an invalid timestamp")
                observed.append(event)
        return observed

    def known_interval(role_name: str) -> tuple[float | None, float | None]:
        request_id = role(role_name).get("requestId")
        native = [
            attempt
            for source in admission_sources
            for attempt in records(source.get("attempts"))
            if request_id is not None and attempt.get("requestId") == request_id
        ]
        starts = [time for attempt in native if (time := timestamp(attempt.get("startedAtMs"))) is not None]
        ends = [time for attempt in native if (time := timestamp(attempt.get("endedAtMs"))) is not None]
        return min(starts) if starts else None, max(ends) if ends else None

    requirement = original.get("requirementId")
    if requirement in ("lmi-lifecycle-2", "lmi-lifecycle-7"):
        returned, exited = boundary("victim", "wrapper_return"), boundary("victim", "task_exit")
        for event in observed_events("victim", "root_completed"):
            if "outcome" in event and not same_json(event["outcome"], role("victim").get("outcome")):
                findings.append("ASSERTION: cleanup root completion contradicts the frozen invocation outcome")
            completed_at = timestamp(event.get("atMs"))
            if completed_at is not None and any(end is not None and completed_at > end for end in (returned, exited)):
                findings.append("ASSERTION: cleanup root completion follows the frozen wrapper or task exit")
    if requirement in ("lmi-lifecycle-1", "lmi-lifecycle-4") and final:
        # Native metadata can widen a provisional interval. A barrier claim is
        # disproved only after every supplied native interval is available.
        intervals = [known_interval(name) for name in ("victim", "peer")]
        starts = [start for start, _ in intervals if start is not None]
        ends = [end for _, end in intervals if end is not None]
        if len(starts) == len(ends) == 2:
            for name in ("victim", "peer"):
                for event in observed_events(name, "barrier_arrived"):
                    arrived = timestamp(event.get("atMs"))
                    if arrived is not None and not max(starts) <= arrived < min(ends):
                        findings.append("PLACEMENT: cleanup barrier lies outside the established native overlap")
    if requirement in ("lmi-lifecycle-3", "lmi-lifecycle-4"):
        profile = original.get("profile")
        profile = profile if isinstance(profile, Mapping) else {}
        fixture_budgets = original.get("budgets")
        fixture_budgets = fixture_budgets if isinstance(fixture_budgets, Mapping) else {}
        start, end = known_interval("victim")
        configured_timeout = timestamp(profile.get("timeoutMs"))
        inflight, exited = boundary("victim", "work_in_flight"), boundary("victim", "task_exit")
        timeout_events = observed_events("victim", "platform_timeout")
        for event in timeout_events:
            if "source" in event and event["source"] != "platform":
                findings.append("ASSERTION: cleanup timeout source is not the observed platform deadline")
            timed_out = timestamp(event.get("atMs"))
            if timed_out is None:
                continue
            if final and (
                (start is not None and configured_timeout is not None and timed_out < start + configured_timeout)
                or (end is not None and timed_out > end)
            ):
                findings.append("ASSERTION: cleanup timeout falls outside the configured native deadline")
            if (inflight is not None and timed_out <= inflight) or (exited is not None and timed_out > exited):
                findings.append("ASSERTION: cleanup timeout contradicts the frozen in-flight work or task exit")
            for kind, budget_key in (("admission_closed", "admissionMs"), ("task_exit", "cleanupMs")):
                measured, budget = boundary("victim", kind), timestamp(fixture_budgets.get(budget_key))
                if measured is not None and budget is not None and not timed_out <= measured <= timed_out + budget:
                    findings.append(f"ASSERTION: cleanup timeout contradicts the frozen {budget_key} measurement")
        if requirement == "lmi-lifecycle-4":
            if profile.get("recoveryPolicy") == "replace-worker":
                recovery_id = role("recovery").get("requestId")
                for source in admission_sources:
                    for attempt in records(source.get("attempts")):
                        if (
                            recovery_id is not None
                            and attempt.get("requestId") == recovery_id
                            and "replacesWorkerId" in attempt
                            and not same_json(attempt["replacesWorkerId"], role("victim").get("workerId"))
                        ):
                            findings.append("PLACEMENT: cleanup recovery replacement changed the frozen failed worker")
            recovery_start, _ = known_interval("recovery")
            recovery_budget = timestamp(fixture_budgets.get("recoveryMs"))
            for event in observed_events("recovery", "worker_recovered"):
                recovered = timestamp(event.get("atMs"))
                if recovered is None:
                    continue
                if final and recovery_start is not None and recovered < recovery_start:
                    findings.append("ASSERTION: cleanup worker recovery precedes its native invocation")
                for timeout_event in timeout_events:
                    timed_out = timestamp(timeout_event.get("atMs"))
                    if (
                        timed_out is not None
                        and recovery_budget is not None
                        and not (timed_out <= recovered <= timed_out + recovery_budget)
                    ):
                        findings.append("ASSERTION: cleanup worker recovery exceeds the frozen recovery measurement")
                if final:
                    for peer in selected:
                        if isinstance(peer, str) and peer.startswith("peer"):
                            _, peer_end = known_interval(peer)
                            if peer_end is not None and recovered > peer_end:
                                findings.append("PLACEMENT: cleanup worker recovery outlived established peer capacity")
    if requirement in ("lmi-lifecycle-3", "lmi-lifecycle-4"):
        limits = [boundary("victim", "task_exit")]
        if requirement == "lmi-lifecycle-4":
            limits.extend(timestamp(event.get("atMs")) for event in observed_events("recovery", "worker_recovered"))
        known_limits = [time for time in limits if time is not None]
        # A later release acknowledgment cannot erase an earlier release from
        # either observation records or a provisional/final retirement record.
        if releases and known_limits and min(releases) <= max(known_limits):
            findings.append("ASSERTION: cleanup evidence reports fault release before the asserted cleanup or recovery")
    if requirement == "lmi-lifecycle-6" and (
        changed_time("victim", "endedAtMs") or changed_time("warm", "startedAtMs")
    ):
        start, end, returned = (
            native_time("warm", "startedAtMs"),
            native_time("victim", "endedAtMs"),
            boundary("victim", "wrapper_return"),
        )
        if start is not None and ((end is not None and start < end) or (returned is not None and start <= returned)):
            findings.append("PLACEMENT: cleanup evidence disproves sequential warm reuse")
    if requirement == "lmi-lifecycle-4" and (
        changed_time("victim", "endedAtMs") or changed_time("recovery", "startedAtMs")
    ):
        start, end, timeout = (
            native_time("recovery", "startedAtMs"),
            native_time("victim", "endedAtMs"),
            boundary("victim", "platform_timeout"),
        )
        if start is not None and timeout is not None and start < timeout:
            findings.append("ASSERTION: cleanup evidence places recovery before the victim timeout")
        victim, recovery = role("victim"), role("recovery")
        if (
            original.get("runtime") == "python"
            and victim.get("processId")
            and victim.get("processId") == recovery.get("processId")
            and start is not None
            and end is not None
            and start < end
        ):
            findings.append("PLACEMENT: cleanup evidence places Python recovery before victim native completion")
    if requirement in ("lmi-lifecycle-5", "lmi-lifecycle-6"):
        sources = (original, *known_snapshots, snapshot)
        executions_by_request: dict[str, set[str]] = {}
        for source in sources:
            for attempt in records(source.get("attempts")):
                request_id, execution_arn = attempt.get("requestId"), attempt.get("executionArn")
                if (
                    isinstance(request_id, str)
                    and request_id.strip()
                    and isinstance(execution_arn, str)
                    and execution_arn.strip()
                ):
                    executions_by_request.setdefault(request_id, set()).add(execution_arn)

        def bound_execution(request_id: Any) -> str | None:
            # A new native request may only gain its execution binding during
            # retirement. Do not turn that provisional absence into a failure.
            if not isinstance(request_id, str) or not request_id.strip():
                if final:
                    findings.append("INCOMPLETE: cleanup replay/activity lacks an identified native request")
                return None
            if request_id not in attempts and not final:
                return None
            bindings = executions_by_request.get(request_id, set())
            if len(bindings) != 1:
                if final:
                    findings.append(
                        f"INCOMPLETE: cleanup native request {request_id} has an unresolved or conflicting execution binding"
                    )
                return None
            return next(iter(bindings))

        if requirement == "lmi-lifecycle-5":
            replay = original.get("replay")
            replay = replay if isinstance(replay, Mapping) else {}
            completed = {
                operation["id"]
                for operation in records(replay.get("before"))
                if isinstance(operation.get("id"), str) and operation.get("status") in _OPERATION_TERMINAL_STATUSES
            }
            expected_counts = {
                item["operationId"]: item["expectedCount"]
                for item in records(replay.get("sideEffects"))
                if isinstance(item.get("operationId"), str) and type(item.get("expectedCount")) is int
            }
            victim = role("victim")
            checkpoint_errors, observed_before = _cleanup_checkpoint_errors(
                replay, sources, victim.get("executionArn"), final=final
            )
            findings.extend(checkpoint_errors)
            completed.update(observed_before)
            returned = boundary("victim", "wrapper_return")
            resume_id = role("resume").get("requestId")
            for source in sources:
                for event in records(source.get("events")):
                    if event.get("kind") != "pending" or event.get("requestId") != victim.get("requestId"):
                        continue
                    request_id = event.get("requestId")
                    execution_arn = bound_execution(request_id)
                    if execution_arn is None:
                        continue
                    if any(
                        key in event and event[key] != expected
                        for key, expected in (("ownerRequestId", request_id), ("executionArn", execution_arn))
                    ):
                        if final:
                            findings.append("INCOMPLETE: cleanup PENDING event has inconsistent native ownership")
                        continue
                    if "atMs" not in event:
                        continue  # A partial event may gain its timestamp during collection.
                    pending_at = timestamp(event["atMs"])
                    if pending_at is None:
                        if final:
                            findings.append("INCOMPLETE: cleanup PENDING event has an invalid timestamp")
                    elif returned is not None and pending_at > returned:
                        findings.append("ASSERTION: cleanup PENDING event follows the frozen wrapper return")
                for attempt in records(source.get("attempts")):
                    started = timestamp(attempt.get("startedAtMs"))
                    if (
                        isinstance(resume_id, str)
                        and attempt.get("requestId") == resume_id
                        and started is not None
                        and returned is not None
                        and started <= returned
                    ):
                        findings.append(
                            "ASSERTION: cleanup evidence places resume before the frozen PENDING wrapper return"
                        )
                if "replay" not in source:
                    continue
                observed_replay = source["replay"]
                if not isinstance(observed_replay, Mapping):
                    if final:
                        findings.append("INCOMPLETE: cleanup replay observations must be an object")
                    continue
                entries = observed_replay.get("operationExecutions", [])
                if not isinstance(entries, list):
                    if final:
                        findings.append("INCOMPLETE: cleanup operation executions must be an array")
                    entries = []
                for entry in entries:
                    if not isinstance(entry, Mapping):
                        if final:
                            findings.append("INCOMPLETE: cleanup operation execution must be an object")
                        continue
                    request_id, operation_id = entry.get("requestId"), entry.get("operationId")
                    execution_arn = bound_execution(request_id)
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append("INCOMPLETE: cleanup operation execution lacks an operationId")
                        continue
                    if (
                        execution_arn is not None
                        and execution_arn == victim.get("executionArn")
                        and request_id != victim.get("requestId")
                        and operation_id in completed
                    ):
                        findings.append(
                            f"ASSERTION: cleanup evidence reports repeated completed user code for {operation_id}"
                        )
                if final:
                    findings.extend(_side_effect_observation_errors(source))
                counts = observed_replay.get("sideEffects", [])
                if not isinstance(counts, list):
                    if final:
                        findings.append("INCOMPLETE: cleanup side-effect observations must be an array")
                    counts = []
                for count in counts:
                    if not isinstance(count, Mapping):
                        if final:
                            findings.append("INCOMPLETE: cleanup side-effect observation must be an object")
                        continue
                    operation_id = count.get("operationId")
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append("INCOMPLETE: cleanup side-effect observation lacks an operationId")
                        continue
                    expected = expected_counts.get(operation_id)
                    if expected is None:
                        continue  # Cleanup cannot invent an oracle for an unasserted operation.
                    if "expectedCount" in count and not same_json(count["expectedCount"], expected):
                        findings.append(
                            f"ASSERTION: cleanup side-effect expectation for {operation_id} changed the frozen fixture oracle"
                        )
                    if type(count.get("count")) is int and count["count"] != expected:
                        findings.append(
                            f"ASSERTION: cleanup side-effect count for {operation_id} differs from the frozen fixture oracle"
                        )
            findings.extend(
                _cleanup_retry_errors(replay, sources, victim.get("executionArn"), bound_execution, final=final)
            )
        else:
            victim_id = role("victim").get("requestId")
            exited = boundary("victim", "task_exit")
            for source in sources:
                for event in records(source.get("events")):
                    if event.get("kind") != "late_activity":
                        continue
                    request_id = event.get("requestId")
                    execution_arn = bound_execution(request_id)
                    if execution_arn is None:
                        continue
                    if event.get("ownerRequestId") != request_id or event.get("executionArn") != execution_arn:
                        if final:
                            findings.append("INCOMPLETE: cleanup late activity has inconsistent native ownership")
                        continue
                    during = event.get("observedDuringRequestId")
                    if during is not None and bound_execution(during) is None:
                        continue
                    observed_at = timestamp(event.get("atMs"))
                    if (
                        request_id == victim_id
                        and observed_at is not None
                        and exited is not None
                        and observed_at > exited
                    ):
                        findings.append("ASSERTION: cleanup late activity continued after the frozen victim task exit")
    return findings


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
                                        cleanup_errors if finding.startswith("INCOMPLETE:") else cleanup_behavior_errors
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
                if owned_work and (status != "PASSED" or context.retirement_evidence or cleanup_changed):
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
                                    cleanup_errors if finding.startswith("INCOMPLETE:") else cleanup_behavior_errors
                                )
                                if finding not in destination:
                                    destination.append(finding)
                            audit = [
                                error
                                for baseline in [*observation_baselines, evidence, *cleanup_snapshots]
                                for error in _retirement_ownership_errors(context, baseline, retirement_evidence)
                            ]
                            audit.extend(validate_retirement_evidence(retirement_evidence))
                            if audit:
                                errors.extend(error for error in audit if error not in errors)
                                cleanup_errors.append("Independent retirement evidence did not establish quiescence")
                            else:
                                physical_retirement_verified = True
                    except (Exception, asyncio.CancelledError) as exc:
                        cleanup_errors.append(f"Independent retirement validation: {type(exc).__name__}: {exc}")
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
                        destination = cleanup_errors if finding.startswith("INCOMPLETE:") else cleanup_behavior_errors
                        if finding not in destination:
                            destination.append(finding)
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"Final cleanup behavior validation: {type(exc).__name__}: {exc}")
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
                        final_errors = _ownership_errors(context, evidence) + list(validator(evidence, phase="final"))
                        new_errors = [error for error in final_errors if error not in errors]
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
                        status = "FAILED"
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
