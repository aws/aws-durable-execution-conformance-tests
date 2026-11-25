# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Bounded, resource-scoped orchestration for opt-in LMI scenarios.

Adapters own AWS calls and fixtures. The coordinator owns admission and keeps a
case's lease through retirement, including time spent suspended or collecting
traces. A scheduler is local to one event loop: adapters must also hold a
cross-process deployment/run lock when persistent targets are shared by runs.

The adapter, in production the shared AWS driver, is trusted: the coordinator
records the adapter's own reports, such as launches, inventories and
retirement, without re-auditing them against earlier snapshots, the launch
journal, admission, or other cases. SDK fixture output that the adapter relays
stays untrusted. Adjudication judges its behavior, and before the coordinator
releases a target the retirement snapshot must establish physical retirement
and settle the native owner of every SDK record seen in any snapshot.
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

from aws_durable_execution_conformance_tests.lmi.evidence import (
    RECOVERY_POLICIES,
    is_json_integer,
    same_json,
    validate_observed_retirement,
    validate_retirement_evidence,
)
from aws_durable_execution_conformance_tests.lmi.rules.controls import invoke_rejection_proven
from aws_durable_execution_conformance_tests.lmi.rules.cutoff import deferred_service_requests, requests_after_cutoff
from aws_durable_execution_conformance_tests.lmi.rules.retention import _cleanup_record_errors


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
        # Callers may pass any sequence; adapters receive an immutable tuple.
        object.__setattr__(self, "roles", tuple(self.roles))
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
    queue: deque[_Waiter] = field(default_factory=deque)
    owners: dict[str, _Waiter] = field(default_factory=dict)
    quarantine_reason: str | None = None


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

        The deployment lease is never budgeted: it serializes a target rather
        than running a case's work.
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
            released = resource.owners.pop(lease.owner)
            lease.released = True
            if released.budget is not None:
                self._active[released.budget] -= 1
            resource.condition.notify_all()
        if released.budget is not None:
            await self._wake_other_resources(lease.resource_id)

    @asynccontextmanager
    async def _deployment_lease(self, resource_id: str, owner: str, *, timeout: float):
        lease = await self.acquire(resource_id, owner, exclusive=True, timeout=timeout)
        completed = False
        try:
            yield
            completed = True
        finally:
            await lease.release(quiescent=completed)


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
    _collection_deadline: float | None = field(default=None, init=False, repr=False, compare=False)

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

    def withdraw_attempt(self, attempt_id: str) -> None:
        """Remove a launch whose dispatch provably never happened.

        Only the most recently journaled attempt can be withdrawn, and only
        when it is still a bare driver-invoke launch: it has a launch
        timestamp and none of a request ID, execution ARN or native
        start/end time. Any attempt that may have reached the service must
        stay visible to retirement, so a mismatch raises rather than
        silently dropping it.
        """
        if not self._attempts or self._attempts[-1]["attemptId"] != attempt_id:
            raise ValueError("Only the last journaled attempt can be withdrawn")
        attempt = self._attempts[-1]
        if (
            attempt.get("source") != "driver-invoke"
            or "launchedAtMs" not in attempt
            or any(key in attempt for key in ("requestId", "executionArn", "nativeStartedAtMs", "nativeEndedAtMs"))
        ):
            raise ValueError("Only an undispatched driver launch can be withdrawn")
        removed = self._attempts.pop()
        if self._on_attempt is not None:
            try:
                self._on_attempt(self)
            except BaseException:
                # The durable sink may have already dropped the attempt from
                # its journal before failing (e.g. an fsync after the write),
                # so memory and the durable record can still disagree here.
                # That is safe: Invoke never ran for this attempt, so keeping
                # it visible in memory and failing closed by re-raising never
                # hides a launch that may have dispatched.
                self._attempts.append(removed)
                raise

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
        if self._on_attempt is None:
            return
        try:
            self._on_attempt(self)
        except BaseException:
            # An undispatched launch can be rolled back. An already-running
            # service invocation must remain visible to retirement.
            if timestamp_key == "launchedAtMs":
                self._attempts.pop()
            raise

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
                for name, label in (
                    ("requestId", "invocation request ID"),
                    ("executionArn", "durable execution"),
                    ("nativeStartedAtMs", "service native start"),
                    ("nativeEndedAtMs", "service native completion"),
                ):
                    if name in attempt and diagnostics.get(name, attempt[name]) != attempt[name]:
                        raise ValueError(f"Cannot replace the original {label}")
                if "attempt" in diagnostics:
                    diagnostics["attempt"] = int(diagnostics["attempt"])
                attempt.update(diagnostics)
                if self._on_attempt is not None:
                    self._on_attempt(self)
                return
        raise ValueError(f"Unknown attempt {attempt_id}")


class ScenarioAdapter(Protocol):
    """Trusted fixture bridge; all methods must cooperate with cancellation.

    In production this is the shared AWS driver. The coordinator does not
    re-audit the adapter's own reports, so their defects are caught by the
    adapter's own tests. Fixture observations it relays remain untrusted.

    ``prepare`` must hold an external run/deployment lock if another process can
    use this persistent target. Retain that lock until every case has retired.
    Profiles finish only after every case in the run, so ``prepare`` must not
    wait on a lock that another profile's ``finish`` releases. Acquire a lock
    that several of this run's targets share once for the run. Release it after
    the last ``finish`` only if every target sharing it finished with
    ``quiescent=True``; otherwise keep it. Fail fast, or wait well under the
    provisioning budget, on a lock another run holds.
    ``place`` must positively identify the cohort and release only its barriers.
    ``collect`` returns every observation so far; ``release`` touches only this
    context's controls; ``retire`` writes final ``context.evidence['retirement']``
    and returns True only when all executions, retries, callbacks and late work
    are settled. ``finish`` releases external run ownership only when quiescent
    is True; otherwise it must preserve quarantine until external recovery.
    Every attempted launch is journaled before transport dispatch.

    An optional ``collect_partial(context)`` returns a bounded read-only snapshot of
    every observation so far after a non-cancelled phase failure, before fault
    release; it replaces ``context.evidence``. It must not wait for cleanup or
    invent missing observations. A case that failed before cleanup can release its lease
    only when ``retire`` additionally supplies ``context.retirement_evidence``:
    independently auditable post-cleanup facts. That snapshot never replaces the
    original behavior evidence or makes its result pass.

    Cleanup may discover additional owned work. Retain it on ``context.evidence``
    even when a cleanup method raises. The coordinator preserves each cleanup
    phase's snapshot separately in ``ScenarioResult.cleanup_evidence``. A case
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
    if name in {"collection", "partialCollection"}:
        # Leave the worker time to return its retained evidence before the watchdog.
        context._collection_deadline = monotonic_started + budget - min(0.1, budget / 10)
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait((task,), timeout=budget)
        if not done:
            raise PhaseTimeout(f"{name} exceeded its {budget:g}s harness budget")
        if task.cancelled():
            raise RuntimeError(f"{name} operation was cancelled")
        return task.result()
    finally:
        if name in {"collection", "partialCollection"}:
            context._collection_deadline = None
        context.timings[name] = {
            "startedAtMs": started,
            "finishedAtMs": time.time() * 1000,
            "durationMs": (time.monotonic() - monotonic_started) * 1000,
        }
        if task.cancelled():
            # A phase that ended in cancellation, even its own, may have left
            # external work running, exactly like an interrupted one.
            residual.append(task)
        elif not task.done():
            task.cancel()
            residual.append(task)
            # Do not wait without a bound for a cancellation-resistant adapter.
            task.add_done_callback(lambda finished: None if finished.cancelled() else finished.exception())


async def run_scenarios(
    scenarios: Sequence[Scenario],
    profiles: Sequence[Profile],
    adapter: ScenarioAdapter,
    *,
    run_id: str,
    scenario_concurrency: int = 8,
    budgets: Budgets | None = None,
    scheduler: ResourceScheduler | None = None,
    on_result: Callable[[ScenarioResult], None] | None = None,
    on_attempt: Callable[[ScenarioContext], None] | None = None,
) -> list[ScenarioResult]:
    """Prepare each profile once, then run compatible cases concurrently.

    The coordinator collects evidence and decides physical target reuse; it
    never decides conformance. Results carry collection statuses, never
    ``PASSED``. Verdicts for these captures come only from adjudication, through
    ``lmi.app.collect_and_adjudicate`` or ``lmi.app.adjudicate_collected_evidence``.

    Results preserve pre-release failures. Normal final collection can only add
    the retirement record. Failed cases and cleanup discoveries retain a separate
    audited retirement snapshot, so cleanup cannot rewrite their original behavior
    observations.
    Missing handlers, infrastructure failures and evidence gaps are never passes.
    ``scheduler`` serves this run only; sharing one across runs is not supported.
    The adapter is trusted (see ``ScenarioAdapter``): results hold its newest
    snapshots as reported.
    ``on_attempt`` is a synchronous durable journal sink called before transport
    dispatch. If it raises, an undispatched launch is rolled back and a
    service-discovered attempt stays journaled. ``on_result`` receives case
    retirement and final profile-retirement updates, including cancelled cases;
    sinks should replace records by case ID.
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
    for case in scenarios:
        if case.profile_id not in profile_map or case.slots > profile_map[case.profile_id].capacity:
            raise ValueError(f"Invalid profile or capacity budget for {case.case_id}")
    for profile in profiles:
        leases.register(profile.resource_id, profile.capacity)
    if not callable(getattr(adapter, "finish", None)):
        raise ValueError("Adapter must implement finish(profile, *, quiescent) for cross-run ownership")
    leases.limit_concurrency(run_id, scenario_concurrency)
    completed: dict[str, ScenarioResult] = {}

    def emit(result: ScenarioResult) -> ScenarioResult:
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

    contexts = {
        case.case_id: ScenarioContext(run_id, case, profile_map[case.profile_id], _on_attempt=on_attempt)
        for case in scenarios
    }
    adapter_preparation_started: set[str] = set()
    # Work that a timed-out or cancelled case-phase adapter task started may still
    # be running on any target, even after the task itself finished. A run with
    # one reports no profile quiescent.
    interrupted: list[list[asyncio.Task[Any]]] = []
    prepared: dict[str, asyncio.Task[tuple[list[str], dict[str, dict[str, float]]]]] = {}

    async def prepare(profile: Profile) -> tuple[list[str], dict[str, dict[str, float]]]:
        context = ScenarioContext(
            run_id, Scenario("deployment", "deployment", profile.profile_id, "deployment"), profile
        )
        # A timed-out preparation leaves its own deployment lease non-quiescent,
        # so it is not tracked.
        residual: list[asyncio.Task[Any]] = []

        async def provision() -> None:
            adapter_preparation_started.add(profile.profile_id)
            await adapter.prepare(profile)

        try:
            async with leases._deployment_lease(profile.resource_id, f"{run_id}:deployment", timeout=limits.lease):
                await _bounded(provision(), "provisioning", limits.provisioning, context, residual)
            return [], context.timings
        except Exception as exc:
            return [f"provisioning: {type(exc).__name__}: {exc}"], context.timings

    # A failed target does not prevent another profile from preparing or running.
    for profile_id in {case.profile_id for case in scenarios}:
        prepared[profile_id] = asyncio.create_task(prepare(profile_map[profile_id]))

    cleanup_tasks: set[asyncio.Task[Any] | None] = set()

    async def run_case(case: Scenario) -> ScenarioResult:
        context = contexts[case.case_id]
        residual: list[asyncio.Task[Any]] = []
        interrupted.append(residual)
        evidence: dict[str, Any] = {}
        observation_snapshots: list[dict[str, Any]] = []
        retirement_evidence: dict[str, Any] = {}
        cleanup_evidence: dict[str, list[dict[str, Any]]] = {}
        cleanup_changed = False
        collected_normally = False
        errors: list[str] = []
        status = "COLLECTED"
        lease: Lease | None = None
        cancelled = False

        def observe(snapshot: Mapping[str, Any]) -> dict[str, Any]:
            """Freeze an adapter snapshot; adjudication compares their SDK observations."""
            frozen = copy.deepcopy(dict(snapshot))
            if frozen and not (observation_snapshots and same_json(observation_snapshots[-1], frozen)):
                observation_snapshots.append(frozen)
            return frozen

        def observe_context() -> None:
            if isinstance(context.evidence, Mapping):
                observe(context.evidence)

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
            # Snapshot the context at every phase boundary, even after a failure.
            async def establish() -> None:
                for operation in (adapter.start, adapter.place):
                    try:
                        await operation(context)
                    finally:
                        observe_context()

            try:
                await _bounded(establish(), "placement", limits.placement, context, residual)
            finally:
                observe_context()
            try:
                await _bounded(adapter.execute(context), "execution", limits.execution, context, residual)
            finally:
                observe_context()
            try:
                collected = await _bounded(adapter.collect(context), "collection", limits.collection, context, residual)
            finally:
                observe_context()
            evidence = observe(collected)
            context.evidence = copy.deepcopy(evidence)
            collected_normally = True
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

                def retain_cleanup(phase: str) -> None:
                    nonlocal cleanup_changed
                    try:
                        snapshot = copy.deepcopy(dict(context.evidence))
                        cleanup_evidence[phase] = [snapshot]
                        # The adapter's own collection failures invalidate
                        # cleanup evidence even when no inventory changed.
                        for index, message in enumerate(snapshot.get("collectionErrors", [])):
                            cleanup_errors.append(f"Cleanup {phase}: collectionErrors[{index}]: {message}")
                        for event in snapshot.get("events", []):
                            # Events relay fixture output, which stays untrusted.
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
                    try:
                        evidence = observe(context.evidence)
                    except (Exception, asyncio.CancelledError) as exc:
                        errors.append(f"Context observations: {type(exc).__name__}: {exc}")
                        cleanup_errors.append("Context did not retain trustworthy observations")
                    partial = getattr(adapter, "collect_partial", None)
                    if not cancelled and not residual and callable(partial):
                        try:
                            try:
                                captured = await _bounded(
                                    partial(context), "partialCollection", limits.collection, context, residual
                                )
                            finally:
                                observe_context()
                            evidence = observe(captured)
                        except (Exception, asyncio.CancelledError) as exc:
                            errors.append(f"Partial observation collection: {type(exc).__name__}: {exc}")
                            cleanup_errors.append("Partial collection did not retain trustworthy observations")
                    context.evidence = copy.deepcopy(evidence)
                try:
                    await _bounded(adapter.release(context), "release", limits.release, context, residual)
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"fault release: {type(exc).__name__}: {exc}")
                finally:
                    retain_cleanup("release")
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
                    retain_cleanup("retire")
                # A timed-out adapter can continue sending requests despite
                # task cancellation. Conservatively require explicit recovery.
                if residual:
                    cleanup_errors.append("A bounded phase was interrupted; residual work requires reconciliation")
                normal_retirement = evidence
                try:
                    if evidence and not cleanup_changed and "retirement" in context.evidence:
                        normal_retirement = {**evidence, "retirement": copy.deepcopy(context.evidence["retirement"])}
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"Cleanup evidence retention: {type(exc).__name__}: {exc}")
                cleanup_snapshots = [item for snapshots in cleanup_evidence.values() for item in snapshots]
                retirement_claims = any(
                    isinstance(retirement := frame.get("retirement"), Mapping)
                    and any(retirement.get(field) for field in ("resources", "faults", "completions"))
                    for frame in [*observation_snapshots, evidence, *cleanup_snapshots]
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
                # A failed deferral reconciliation must fail retirement here;
                # raising would skip the non-quiescent release.
                try:
                    journaled_late = requests_after_cutoff(context.attempts, evidence.get("observationEndedAtMs"))
                    if journaled_late:
                        deferred = deferred_service_requests(evidence)
                        deferred.update(
                            {
                                item["requestId"]: item["executionArn"]
                                for item in context.attempts
                                if item.get("requestId") in journaled_late
                            }
                        )
                        evidence.setdefault("collection", {})["deferredServiceRequests"] = [
                            {"requestId": request, "executionArn": arn} for request, arn in sorted(deferred.items())
                        ]
                    full_runtime_audit_required = bool(deferred_service_requests(evidence)) or any(
                        isinstance(item, Mapping) and item.get("activeAtObservationEnd") is True
                        for item in (evidence["attempts"] if isinstance(evidence.get("attempts"), list) else [])
                    )
                except Exception as exc:
                    cleanup_errors.append(f"Deferred service request reconciliation: {type(exc).__name__}: {exc}")
                    full_runtime_audit_required = True
                supplied_retirement = context.retirement_evidence
                if owned_work and (
                    status != "COLLECTED" or supplied_retirement or cleanup_changed or full_runtime_audit_required
                ):
                    try:
                        retirement_evidence = copy.deepcopy(dict(supplied_retirement))
                        if not retirement_evidence and status == "COLLECTED" and cleanup_changed:
                            retirement_evidence = copy.deepcopy(dict(context.evidence))
                        if not retirement_evidence:
                            work = "Launched work" if context.attempts else "Owned work"
                            cleanup_errors.append(f"{work} has no independent retirement evidence")
                        else:
                            audit = validate_retirement_evidence(
                                retirement_evidence, allow_no_launches=not context.attempts
                            )
                            if audit:
                                errors.extend(error for error in audit if error not in errors)
                                cleanup_errors.append("Independent retirement evidence did not establish quiescence")
                            else:
                                physical_retirement_verified = True
                    except (Exception, asyncio.CancelledError) as exc:
                        cleanup_errors.append(f"Independent retirement validation: {type(exc).__name__}: {exc}")
                # Every SDK record's native owner must be settled, including
                # records that only a cleanup snapshot observed.
                cleanup_errors.extend(
                    issue
                    for issue in _cleanup_record_errors(
                        [*observation_snapshots, evidence, *cleanup_snapshots, retirement_evidence],
                        retirement_evidence or normal_retirement,
                    )
                    if issue.startswith("INCOMPLETE:")
                )
                if owned_work and not physical_retirement_verified:
                    if evidence:
                        audit = validate_observed_retirement(normal_retirement)
                        errors.extend(error for error in audit if error not in errors)
                    else:
                        audit = ["Owned work has no captured retirement evidence"]
                        errors.extend(audit)
                    if audit:
                        cleanup_errors.append("Final retirement evidence did not establish quiescence")
                if not retirement_evidence and normal_retirement is not evidence:
                    retirement_evidence = normal_retirement
                # This reads only the coordinator's own journal: a dispatched
                # launch whose invocation was never identified may still run,
                # and no retirement snapshot can show that it ended. A launch
                # the service definitely rejected never started.
                if any(
                    not attempt.get("requestId") and not invoke_rejection_proven(attempt)
                    for attempt in context.attempts
                ):
                    cleanup_errors.append(
                        "Every launched attempt must retain an identified invocation request before retirement"
                    )
                if cleanup_errors:
                    if status == "COLLECTED":
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
                observation_evidence=observation_snapshots,
                trusted_observations=collected_normally,
            )
        )
        if cancelled:
            raise asyncio.CancelledError
        return result

    tasks = [asyncio.create_task(run_case(case)) for case in scenarios]
    case_results = asyncio.gather(*tasks)

    finishing_profiles: set[asyncio.Task[Any]] = set()

    async def finish_profile(profile_id: str) -> None:
        # Profiles finish only after every case in every profile, so a run lock
        # that several targets share is released once (see ScenarioAdapter).
        await asyncio.gather(prepared[profile_id], *tasks, return_exceptions=True)
        if profile_id not in adapter_preparation_started:
            return
        profile = profile_map[profile_id]
        context = ScenarioContext(run_id, Scenario("finish", "finish", profile_id, "finish"), profile)
        # Quiescence must be positive: a case that ended without releasing its
        # lease never completed its retirement audit, so keep the fence.
        stranded = sorted(
            owner for owner in leases._resources[profile.resource_id].owners if owner.startswith(f"{run_id}:")
        )
        if stranded:
            await leases.quarantine(
                profile.resource_id, f"Case ownership was not released after retirement: {', '.join(stranded)}"
            )
        quiet = leases.quarantine_reason(profile.resource_id) is None and not any(interrupted)
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
            finishing_profiles.discard(task)
            for result in completed.values():
                if result.profile_id == profile_id:
                    result.timings.update(copy.deepcopy(context.timings))
                    emit(result)

    async def drain_and_finish() -> None:
        finished = asyncio.gather(
            # The admitted profiles, not the caller's sequence, which it may change.
            *(finish_profile(profile_id) for profile_id in tuple(prepared))
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
    return outcomes
