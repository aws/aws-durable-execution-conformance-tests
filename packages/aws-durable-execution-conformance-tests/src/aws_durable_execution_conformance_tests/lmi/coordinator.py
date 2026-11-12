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
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class Profile:
    """One persistent, unchanged function deployment and its capacity budget."""

    profile_id: str
    resource_id: str
    capacity: int
    native_concurrency: int
    artifact_id: str
    configuration_id: str

    def __post_init__(self) -> None:
        if not all((self.profile_id, self.resource_id, self.artifact_id, self.configuration_id)):
            raise ValueError("Profile identity, artifact and configuration must be nonempty")
        if any(type(value) is not int or value < 1 for value in (self.capacity, self.native_concurrency)):
            raise ValueError("Profile capacity and native_concurrency must be positive integers")


@dataclass(frozen=True)
class Scenario:
    """A cohort admitted as one owner; identifiers never name durable operations."""

    case_id: str
    requirement_id: str
    profile_id: str
    scenario: str
    cohort_id: str = "cohort-1"
    roles: tuple[str, ...] = ("victim",)
    slots: int = 1
    exclusive: bool = False
    otel_view: str | None = None

    def __post_init__(self) -> None:
        if not all((self.case_id, self.requirement_id, self.profile_id, self.scenario, self.cohort_id)):
            raise ValueError("Scenario identities must be nonempty")
        if not self.roles or len(set(self.roles)) != len(self.roles) or not all(self.roles):
            raise ValueError("Scenario roles must be nonempty and unique")
        if type(self.slots) is not int or self.slots < 1:
            raise ValueError("Scenario slots must be a positive integer")
        if self.otel_view not in (None, "invocation", "execution"):
            raise ValueError("otel_view must be invocation or execution")


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
    """

    def __init__(self) -> None:
        self._resources: dict[str, _Resource] = {}

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
        self, resource_id: str, owner: str, *, slots: int = 1, exclusive: bool = False, timeout: float = 300
    ) -> Lease:
        resource = self._resources[resource_id]
        if not owner or type(slots) is not int or not 1 <= slots <= resource.capacity:
            raise ValueError("A lease needs an owner and slots within the resource capacity")
        request = _Waiter(owner, slots, exclusive)
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
                            and (not resource.owners if exclusive else slots <= available)
                        ):
                            resource.queue.popleft()
                            resource.owners[owner] = request
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
            resource.owners.pop(owner)
            resource.condition.notify_all()

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
        """
        resource = self._resources[resource_id]
        async with resource.condition:
            if resource.owners:
                raise QuarantinedError("Cannot recover a resource while an owner is active")
            if resource.quarantine_reason is None:
                return
            async with asyncio.timeout(timeout):
                if await verify() is not True:
                    raise QuarantinedError("Recovery could not prove quiescence")
            resource.quarantine_reason = None
            resource.condition.notify_all()


@dataclass
class ScenarioContext:
    run_id: str
    scenario: Scenario
    profile: Profile
    evidence: dict[str, Any] = field(default_factory=dict)
    attempts: list[dict[str, Any]] = field(default_factory=list)
    timings: dict[str, dict[str, float]] = field(default_factory=dict)
    _lease: Lease | None = field(default=None, repr=False)
    _on_attempt: Callable[[ScenarioContext], None] | None = field(default=None, repr=False)

    async def release_capacity(self) -> None:
        """Keep ownership while positively observed suspended work is idle.

        Call only once the cohort no longer occupies its reserved native slots.
        The service may later resume work; reserve capacity before controlling
        another launch. Exclusive scenarios remain exclusive even while idle.
        """
        if self._lease is None:
            raise RuntimeError("Scenario has no active lease")
        await self._lease.release_capacity()

    async def reserve_capacity(self, *, timeout: float = 300) -> None:
        if self._lease is None:
            raise RuntimeError("Scenario has no active lease")
        await self._lease.reserve_capacity(timeout=timeout)

    def dispatcher(self, role: str) -> dict[str, str]:
        if role not in self.scenario.roles:
            raise ValueError(f"Unknown cohort role {role}")
        payload = {
            "scenario": self.scenario.scenario,
            "runId": self.run_id,
            "caseId": self.scenario.case_id,
            "cohortId": self.scenario.cohort_id,
            "role": role,
        }
        if self.scenario.otel_view is not None:
            payload["otelView"] = self.scenario.otel_view
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
                if (
                    "requestId" in attempt
                    and diagnostics.get("requestId", attempt["requestId"]) != attempt["requestId"]
                ):
                    raise ValueError("Cannot replace the original invocation request ID")
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
    """

    async def prepare(self, profile: Profile) -> None: ...
    async def finish(self, profile: Profile, *, quiescent: bool) -> None: ...
    async def start(self, context: ScenarioContext) -> None: ...
    async def place(self, context: ScenarioContext) -> None: ...
    async def execute(self, context: ScenarioContext) -> None: ...
    async def collect(self, context: ScenarioContext) -> Mapping[str, Any]: ...
    async def release(self, context: ScenarioContext) -> None: ...
    async def retire(self, context: ScenarioContext) -> bool: ...


class EvidenceValidator(Protocol):
    def __call__(self, evidence: Mapping[str, Any], *, phase: str = "final") -> Sequence[str]: ...


@dataclass
class ScenarioResult:
    case_id: str
    requirement_id: str
    profile_id: str
    status: str
    errors: list[str]
    evidence: dict[str, Any]
    attempts: list[dict[str, Any]]
    timings: dict[str, dict[str, float]]


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


def _ownership_errors(context: ScenarioContext, evidence: Mapping[str, Any]) -> list[str]:
    expected = {
        "runId": context.run_id,
        "caseId": context.scenario.case_id,
        "cohortId": context.scenario.cohort_id,
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
        selected = evidence.get("selectedRequests")
        originals = {item["role"]: item.get("requestId") for item in context.attempts if item["attempt"] == 1}
        if (
            not isinstance(selected, Mapping)
            or set(selected) != set(context.scenario.roles)
            or set(originals) != set(context.scenario.roles)
            or any(selected.get(role) != request_id for role, request_id in originals.items())
        ):
            errors.append("Selected requests must be the first journaled invocation for every admitted role")
    return errors


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

    Results preserve pre-release failures. Final collection can only add the
    retirement record, so emergency cleanup cannot rewrite observed recovery.
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
    for case in scenarios:
        if case.profile_id not in profile_map or case.slots > profile_map[case.profile_id].capacity:
            raise ValueError(f"Invalid profile or capacity budget for {case.case_id}")
    for profile in profiles:
        leases.register(profile.resource_id, profile.capacity)
    if not callable(getattr(adapter, "finish", None)):
        raise ValueError("Adapter must implement finish(profile, *, quiescent) for cross-run ownership")
    semaphore = asyncio.Semaphore(scenario_concurrency)
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
        evidence: dict[str, Any] = {}
        errors: list[str] = []
        status = "PASSED"
        residual: list[asyncio.Task[Any]] = []
        lease: Lease | None = None
        acquired_semaphore = False
        cancelled = False
        admitted_at = time.time() * 1000
        context.timings["admission"] = {"startedAtMs": admitted_at}
        preparation_errors, preparation_timings = await asyncio.shield(prepared[case.profile_id])
        context.timings.update(copy.deepcopy(preparation_timings))
        if preparation_errors:
            return emit(
                ScenarioResult(
                    case.case_id,
                    case.requirement_id,
                    case.profile_id,
                    "INFRASTRUCTURE_ERROR",
                    preparation_errors,
                    {},
                    [],
                    context.timings,
                )
            )
        try:
            lease = await _bounded(
                leases.acquire(
                    context.profile.resource_id,
                    f"{run_id}:{case.case_id}",
                    slots=case.slots,
                    exclusive=case.exclusive,
                    timeout=limits.lease,
                ),
                "lease",
                limits.lease,
                context,
                residual,
            )
            context._lease = lease
            # Waiting for a busy target must not consume global slots needed
            # by unrelated profiles. Count active work only after its lease.
            await semaphore.acquire()
            acquired_semaphore = True
            context.timings["admission"]["finishedAtMs"] = time.time() * 1000

            # Start and placement share a single budget: repeated placement
            # attempts cannot reset the clock or silently replace a cohort.
            async def establish() -> None:
                await adapter.start(context)
                await adapter.place(context)

            await _bounded(establish(), "placement", limits.placement, context, residual)
            await _bounded(adapter.execute(context), "execution", limits.execution, context, residual)
            collected = await _bounded(adapter.collect(context), "collection", limits.collection, context, residual)
            if not isinstance(collected, Mapping):
                raise ValueError("Adapter collection must return an evidence object")
            evidence = copy.deepcopy(dict(collected))
            context.evidence = copy.deepcopy(evidence)
            ownership_errors = _ownership_errors(context, evidence)
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
                cleanup_errors: list[str] = []
                try:
                    await _bounded(adapter.release(context), "release", limits.release, context, residual)
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"fault release: {type(exc).__name__}: {exc}")
                try:
                    retired = (
                        await _bounded(adapter.retire(context), "retirement", limits.retirement, context, residual)
                        is True
                    )
                    if not retired:
                        cleanup_errors.append("Retirement did not prove case-owned quiescence")
                except (Exception, asyncio.CancelledError) as exc:
                    cleanup_errors.append(f"retirement: {type(exc).__name__}: {exc}")
                # A timed-out adapter can continue sending requests despite
                # task cancellation. Conservatively require explicit recovery.
                if residual:
                    cleanup_errors.append("A bounded phase was interrupted; residual work requires reconciliation")
                if evidence and "retirement" in context.evidence:
                    evidence["retirement"] = copy.deepcopy(context.evidence["retirement"])
                if evidence:
                    try:
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
                elif context.attempts:
                    cleanup_errors.append("Launched work has no complete evidence for retirement validation")
                if cleanup_errors:
                    status = "INFRASTRUCTURE_ERROR"
                    errors.extend(cleanup_errors)
                await lease.release(quiescent=retired and not cleanup_errors)
            if acquired_semaphore:
                semaphore.release()
        # Preserve partial evidence as well as failed transport launches.
        if not evidence:
            evidence = copy.deepcopy(context.evidence)
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
            if not task.done():
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
