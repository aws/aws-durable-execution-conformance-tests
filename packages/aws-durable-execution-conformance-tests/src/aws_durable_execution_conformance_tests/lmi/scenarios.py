# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Drive the seven shared lifecycle scenarios through real SDK handlers."""

from __future__ import annotations

import hashlib
import time
import uuid
from collections import Counter
from collections.abc import Callable
from typing import Any, Literal

from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.coordinator import ScenarioContext
from aws_durable_execution_conformance_tests.lmi.errors import (
    CollectionBudgetExpired,
    CollectionError,
    PlacementError,
    TransientCollectionError,
)
from aws_durable_execution_conformance_tests.lmi.observations import (
    overlap,
    pending_callback,
    select,
)
from aws_durable_execution_conformance_tests.lmi.service import retryable_read_error
from aws_durable_execution_conformance_tests.lmi.source_evidence import (
    DEADLINE_REQUIREMENTS,
    deadline_cutoff_results,
    ledger_seal_status,
    observation_exclusions,
    owned_observation,
    retirement_activity_after_cutoff,
    terminal_history_pending,
    undispatched_requests,
)
from aws_durable_execution_conformance_tests.validate import inherit_wait_for_callback_event_names


class _CollectionPollExpired(TimeoutError):
    """The driver exhausted its polling budget for a source snapshot."""


def canonical(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        owned_observation(event, event["observation"])
        for event in events
        if event.get("phase") == "CONFORMANCE" and isinstance(event.get("observation"), dict)
    ]


class ScenarioDriver:
    def __init__(
        self,
        context: ScenarioContext,
        cloud: Cloud,
        *,
        runtime: str,
        fixtures: dict[str, str],
        callback_encoder: Callable[[str], bytes] = str.encode,
        suspension_mode: Literal["callback", "wait"] = "callback",
    ) -> None:
        self.context, self.cloud = context, cloud
        self.runtime, self.fixtures = runtime, dict(fixtures)
        self.callback_encoder = callback_encoder
        if suspension_mode not in ("callback", "wait"):
            raise ValueError("Suspension mode must be callback or wait")
        self.suspension_mode = suspension_mode
        self.cloud.suspension_mode = suspension_mode
        self.items: dict[str, dict[str, Any]] = {}
        self.boundary: dict[str, Any] | None = None
        self.peer_boundary: dict[str, Any] | None = None

    @property
    def scenario(self) -> str:
        return self.context.scenario.scenario

    def seconds(self, budget: str, fallback: float) -> float:
        return self.context.scenario.behavior_budgets.get(budget, fallback * 1000) / 1000

    def gate(self, label: str) -> str:
        owner = hashlib.sha256(f"{self.context.run_id}:{self.context.scenario.case_id}".encode()).hexdigest()[:20]
        return f"case-{owner}-{label}"

    def launch(
        self,
        role: str,
        fixture: str,
        *,
        gate: str | None = None,
        preparation_gate: str | None = None,
        resume_roles: tuple[str, ...] = (),
        target_environment: str | None = None,
    ) -> dict[str, Any]:
        if role in self.items:
            raise CollectionError("A later launch cannot replace a selected original")
        item = self.cloud.start(
            fixture,
            gate=gate,
            preparation_gate=preparation_gate,
            target_environment=target_environment,
            role=role,
            resume_roles=resume_roles,
            dispatcher_payload=self.context.dispatcher(role),
            marker=self.gate(role) + "-" + uuid.uuid4().hex[:12],
        )
        self.items[role] = item
        return item

    def start(self) -> None:
        required = [self.scenario]
        if self.scenario == "peer-recovery":
            required.extend(("peer", "recovery"))
        elif self.scenario == "warm-reuse":
            required.append("warm")
        for fixture in required:
            if fixture not in self.fixtures:
                raise NotImplementedError(f"No SDK fixture for {fixture}")
        if self.scenario == "native-overlap":
            for role in self.context.scenario.roles:
                self.launch(role, self.fixtures[self.scenario], gate=self.gate("cohort"))
            return
        self.launch(
            "victim",
            self.fixtures[self.scenario],
            gate=self.gate("fault") if self.scenario in {"deadline-during-step", "peer-recovery"} else None,
            preparation_gate=self.gate("prepare") if self.scenario == "peer-recovery" else None,
            resume_roles=("resume",) if self.scenario == "suspension-retry" else (),
        )

    def wait_overlap(self, roles: tuple[str, ...], environment: str | None = None) -> list[dict[str, Any]]:
        markers = {self.items[role]["marker"] for role in roles}
        replacement = (
            self.scenario == "peer-recovery"
            and roles == ("peer", "recovery")
            and self.runtime == "javascript"
            and self.context.profile.recovery_policy == "replace-worker"
        )

        def established():
            observed = [event for event in self.cloud.refresh() if event["marker"] in markers]
            originals = {}
            for role in roles:
                entries = [
                    event
                    for event in observed
                    if event["phase"] == "WRAPPER_ENTER" and event["marker"] == self.items[role]["marker"]
                ]
                if not entries:
                    return None
                entry = min(entries, key=lambda event: (event["time"], event["sequence"]))
                originals[role] = entry["request"]
                if replacement and role == "recovery":
                    victim_worker = self.boundary.get("worker") if self.boundary is not None else None
                    if not victim_worker or not entry.get("worker") or entry["worker"] == victim_worker:
                        raise PlacementError("Recovery did not enter a replacement worker")
                if time.time() >= entry["deadline"]:
                    raise PlacementError(f"Original {role} invocation expired before overlap was established")
            events = [event for event in observed if event["request"] in originals.values()]
            if any(event["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"} for event in events):
                raise PlacementError("An original cohort invocation already ended")
            if environment is not None and any(
                event["environment"] != environment for event in select(events, "BLOCKED")
            ):
                raise PlacementError("Selected cohort reached a replacement environment")
            try:
                overlap(events, markers, len(roles), environment, runtime=self.runtime, replacement=replacement)
            except PlacementError:
                return None
            return events

        return self.cloud.poll(
            established,
            seconds=self.cloud.manifest.get("driverTimeout", 120),
            category=PlacementError,
            message="Original cohort placement/overlap was not established",
        )

    def place(self) -> None:
        victim = self.items["victim"]
        if self.scenario == "native-overlap":
            self.wait_overlap(("victim", "peer"))
        elif self.scenario in {"deadline-during-step", "peer-recovery"}:
            self.boundary = self.cloud.phase(victim, "BLOCKED")[0]
            if self.scenario == "peer-recovery":
                self._prepare_recovery_peer()
        else:
            self.cloud.phase(victim, "WRAPPER_ENTER")

    def _prepare_recovery_peer(self) -> None:
        if self.boundary is None:
            raise CollectionError("Recovery preparation has no original victim boundary")
        deadline = self.boundary["deadline"]
        lead = self.cloud.manifest["invocationTimeout"] / 2
        self.cloud.poll(
            lambda: time.time() >= deadline - lead,
            seconds=self.cloud.manifest["invocationTimeout"],
            message="Original victim did not reach the peer-admission window",
        )
        if time.time() >= deadline:
            raise PlacementError("Original victim expired before peer admission")
        peer = self.launch(
            "peer",
            self.fixtures["peer"],
            gate=self.gate("peer"),
            target_environment=self.boundary["environment"],
        )
        self.peer_boundary = self.cloud.phase(peer, "BLOCKED")[0]
        required_window = self.seconds("recoveryMs", 15) + self.seconds("observationGraceMs", 1)
        if self.peer_boundary["deadline"] <= deadline + required_window:
            raise PlacementError("Original peer has insufficient time remaining for recovery")
        self.wait_overlap(("victim", "peer"), self.boundary["environment"])
        # Establish the original healthy cohort before admitting the actual fault.
        self.cloud.release(self.gate("prepare"))
        request = self.boundary["request"]
        self.cloud.poll(
            lambda: [
                event
                for event in canonical(self.cloud.for_item(self.items["victim"]))
                if event.get("requestId") == request
                and event.get("kind") == "work_in_flight"
                and event.get("atMs", deadline * 1000) < deadline * 1000
            ],
            seconds=max(0.001, deadline - time.time()),
            message="Original victim never entered the fault before its deadline",
        )

    def _peer_still_active(self) -> float:
        if self.peer_boundary is None:
            raise CollectionError("Recovery lost the original peer boundary")
        if time.time() >= self.peer_boundary["deadline"]:
            raise PlacementError("Original healthy peer expired during recovery")
        if any(
            event["request"] == self.peer_boundary["request"] and event["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}
            for event in self.cloud.for_item(self.items["peer"])
        ):
            raise PlacementError("Original healthy peer ended during recovery")
        return self.peer_boundary["deadline"]

    def _callback(self, item: dict[str, Any]) -> None:
        deadline = time.monotonic() + self.cloud.manifest.get("driverTimeout", 120)
        callback = None
        callback_operation = None
        terminal_without_callback = False
        terminal_seen = False

        def observe() -> bool:
            nonlocal callback, callback_operation, terminal_without_callback, terminal_seen
            ledger = self.cloud.for_item(item)
            history = [dict(event) for event in self.cloud.history(item)]
            inherit_wait_for_callback_event_names(history)
            terminal_seen = terminal_seen or any(
                event.get("EventType")
                in {"ExecutionSucceeded", "ExecutionFailed", "ExecutionTimedOut", "ExecutionStopped"}
                for event in history
            )
            if not terminal_seen:
                callback = pending_callback(ledger, history, "completion")
                if callback is not None:
                    identifiers = [
                        event.get("Id")
                        for event in history
                        if event.get("EventType") == "CallbackStarted"
                        and event.get("CallbackStartedDetails", {}).get("CallbackId") == callback
                    ]
                    if len(identifiers) != 1 or not isinstance(identifiers[0], str):
                        raise CollectionError("Callback token lacks a unique service operation")
                    callback_operation = identifiers[0]
                    return True
                return False
            callback = None
            self.cloud.collect(full_run=False)
            retained = tuple(self.cloud.events.values())
            known, missing, errors = ledger_seal_status(
                retained, self.cloud.raw_histories, self.cloud.invocations, set()
            )
            terminal_without_callback = bool(known and not missing and not errors) and not terminal_history_pending(
                self.cloud, retained, time.time() * 1000, set()
            )
            return terminal_without_callback

        self._poll_collection(
            observe,
            message="Service history does not prove suspension on the outstanding callback",
            required=True,
            seconds=max(0, deadline - time.monotonic()),
        )
        if callback is not None:
            result = self.callback_encoder(item["marker"])
            try:
                item["runner"].send_callback_success(callback, result=result)
            except Exception as error:
                closed_callback = (
                    isinstance(error, ClientError) and error.response["Error"]["Code"] == "CallbackTimeoutException"
                )
                if not closed_callback and not retryable_read_error(error):
                    raise

                def confirmed() -> bool:
                    completed = [
                        event
                        for event in self.cloud.history(item)
                        if event.get("Id") == callback_operation
                        and event.get("EventType") in {"CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"}
                    ]
                    if not completed:
                        return False
                    if len(completed) == 1 and completed[0]["EventType"] == "CallbackSucceeded":
                        value = completed[0].get("CallbackSucceededDetails", {}).get("Result")
                        if (
                            isinstance(value, dict)
                            and value.get("Truncated", False) is False
                            and isinstance(payload := value.get("Payload"), str)
                            and payload.encode() == result
                        ):
                            return True
                    raise CollectionError("Service history does not confirm the submitted callback result")

                self._poll_collection(
                    confirmed,
                    message="Service did not confirm the callback result",
                    required=True,
                    seconds=max(0, deadline - time.monotonic()),
                )
        elif not terminal_without_callback:
            raise CollectionError("No outstanding callback was verified")

    def _deadline(self) -> None:
        if self.boundary is None:
            raise CollectionError("Deadline fixture lost its original placement record")
        victim = self.items["victim"]
        # The fixture cooperatively exits real held I/O after the runtime deadline;
        # no control release is allowed to establish the cleanup assertion.
        self.cloud.poll(
            lambda: time.time() > self.boundary["deadline"],
            seconds=self.cloud.manifest["invocationTimeout"] + 5,
        )
        if self.scenario == "peer-recovery":
            self._peer_still_active()
            recovery = self.launch(
                "recovery",
                self.fixtures["recovery"],
                gate=self.gate("recovery"),
                target_environment=self.boundary["environment"],
            )
            self.wait_overlap(("peer", "recovery"), self.boundary["environment"])
            self.cloud.release(self.gate("recovery"))
            self.cloud.finish(recovery)
            # Logical success and wrapper return can precede the native
            # completion. Keep the peer held until that completion is recorded,
            # for as long as the original peer remains active. Collection later
            # waits for every request's own completion, including the peer's.
            peer_deadline = self._peer_still_active()
            self.cloud.native_completion(
                recovery,
                seconds=max(0.001, peer_deadline - time.time()),
                category=PlacementError,
                message="Original healthy peer expired before the recovery native completion was recorded",
            )
            self._peer_still_active()
            self.cloud.release(self.gate("peer"))
            self.cloud.finish(self.items["peer"])
        self.cloud.platform_timeout(victim, self.boundary["request"])
        self.cloud.poll(
            lambda: (
                time.time()
                > self.boundary["deadline"] + self.seconds("cleanupMs", 5) + self.seconds("observationGraceMs", 1)
            ),
            seconds=self.cloud.manifest["invocationTimeout"] + 30,
        )
        # Preserve the native observation tail. Collection may stop the victim
        # after setting the behavior cutoff without releasing its controls.

    def execute(self) -> None:
        victim = self.items["victim"]
        if self.scenario == "native-overlap":
            self.cloud.release(self.gate("cohort"))
            for item in self.items.values():
                self.cloud.finish(item)
        elif self.scenario in {"deadline-during-step", "peer-recovery"}:
            self._deadline()
        elif self.scenario == "suspension-retry":
            if self.suspension_mode == "callback":
                self._callback(victim)
            self.cloud.finish(victim)
        elif self.scenario == "warm-reuse":
            self.cloud.finish(victim)
            self.cloud.finish(self.launch("warm", self.fixtures["warm"]))
        else:
            self.cloud.finish(victim, "FAILED" if self.scenario == "normal-failure" else "SUCCEEDED")
        self.cloud.observation_ended_at = time.time()

    def _poll_collection(
        self, predicate: Callable[[], bool], *, message: str, required: bool, seconds: float | None = None
    ) -> None:
        last_error: TransientCollectionError | None = None

        def read() -> bool:
            nonlocal last_error
            try:
                ready = predicate()
            except TransientCollectionError as error:
                last_error = error
                return False
            last_error = None
            return ready

        try:
            self.cloud.poll(
                read,
                seconds=self.cloud.manifest.get("driverTimeout", 120) if seconds is None else seconds,
                category=_CollectionPollExpired,
                message=message,
            )
        except _CollectionPollExpired as error:
            if last_error is not None:
                raise CollectionError(f"{message}: {last_error}") from last_error
            if required:
                raise CollectionError(message) from error

    def collect(self) -> None:
        try:
            self.cloud.wait(self.seconds("observationGraceMs", 1))
            self.cloud.observation_ended_at = time.time()
            self._poll_collection(
                self._collect_observation,
                message="Observation is missing service history or native ledger seals",
                required=False,
            )
        except CollectionBudgetExpired:
            # The coordinator still audits any retained observations and retirement.
            pass

    def _collect_observation(self) -> bool:
        if DEADLINE_REQUIREMENTS.get(self.scenario) == self.context.scenario.requirement_id:
            self.cloud.stop_victim_after_cutoff(self.items["victim"])
        return self._observations_ready()

    def _observations_ready(self) -> bool:
        self.cloud.collect(full_run=False)
        cutoff = self.cloud.observation_ended_at
        if cutoff is None:
            return False
        witnesses = deadline_cutoff_results(self.context, self.cloud)
        if any(row.get("collectionError") for row in witnesses.values()):
            # The stop could not be proven. Retain a fresh read for the
            # incomplete behavior result without waiting for this witness.
            return True
        ledger = tuple(self.cloud.events.values())
        excluded = observation_exclusions(self.context, self.cloud, ledger, cutoff * 1000)
        known, missing, errors = ledger_seal_status(ledger, self.cloud.raw_histories, self.cloud.invocations, excluded)
        return bool(known and not missing and not errors) and not terminal_history_pending(
            self.cloud, ledger, cutoff * 1000, excluded, witnesses
        )

    def quiescent(self) -> bool:
        events = self.cloud.refresh(validate_controls=False)
        observations = canonical(events)
        if any(item.get("kind") == "observation_error" for item in observations):
            return False
        if not self.cloud.invocations:
            return True
        known, missing, errors = ledger_seal_status(events, self.cloud.raw_histories, self.cloud.invocations, set())
        # A request the service proves undispatched can never get a ledger seal,
        # so physical retirement exempts it from this requirement alone; it is
        # still reported incomplete everywhere behavior is evaluated.
        missing -= undispatched_requests(events, self.cloud.raw_histories, self.cloud.invocations).keys()
        if not known or missing or errors:
            return False
        if any(not item.get("arn") or not item["native_requests"] for item in self.cloud.invocations):
            return False
        entered = {event["request"] for event in select(events, "WRAPPER_ENTER")}
        ended = {event["request"] for event in events if event["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}}
        if not entered or not entered <= ended:
            return False
        for start, end, identity in (
            ("task_start", "task_exit", "taskId"),
            ("io_start", "io_exit", "taskId"),
            ("checkpoint_call", "checkpoint_exit", "callId"),
            ("poll_call", "poll_exit", "callId"),
        ):
            starts = Counter(
                (item.get("requestId"), item.get(identity)) for item in observations if item.get("kind") == start
            )
            ends = Counter(
                (item.get("requestId"), item.get(identity)) for item in observations if item.get("kind") == end
            )
            if starts - ends:
                return False
        # The public root task hook is a positive witness that task instrumentation
        # ran; absent instrumentation must not look like an empty, settled pool.
        root_ends = {
            item.get("requestId")
            for item in observations
            if item.get("kind") == "task_exit" and item.get("taskId") == "root"
        }
        return entered <= root_ends

    def _retirement_ready(self) -> bool:
        cutoff = time.time()
        self.cloud.observation_ended_at = cutoff
        self.cloud.collect(full_run=False)
        known = {
            (item.get("arn"), request) for item in self.cloud.invocations for request in item.get("native_requests", ())
        }
        if not self.quiescent() or any(
            (item.get("arn"), request) not in known
            for item in self.cloud.invocations
            for request in item.get("native_requests", ())
        ):
            return False
        if not self.cloud.invocations:
            return True
        ledger = tuple(self.cloud.events.values())
        cutoff_ms = cutoff * 1000
        return not terminal_history_pending(
            self.cloud, ledger, cutoff_ms, set()
        ) and not retirement_activity_after_cutoff(self.cloud, cutoff_ms)

    def retire(self) -> bool:
        self.cloud.settle_case()
        self._poll_collection(
            self._retirement_ready,
            message="Case retirement is missing terminal history, quiescent work or native ledger seals",
            required=True,
        )
        self.cloud.delete_controls()
        self.cloud.retired_at = time.time()
        return True
