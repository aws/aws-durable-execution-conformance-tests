# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Version 1 lifecycle evidence contract and fail-closed observation validators.

See ``evidence-schema.md`` next to this module. Times are milliseconds on one
coordinator-normalized observation clock, not uncorrelated process wall clocks.
The schema is an adapter contract, not proof that an untrusted producer is honest.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, Literal

SCHEMA_VERSION = 1
REQUIREMENT_IDS = tuple(f"lmi-lifecycle-{index}" for index in range(1, 9))
RUNTIMES = ("javascript", "java", "python")
HISTORY_STATUSES = ("SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED", "RUNNING", "PENDING")


class _Evidence:
    def __init__(
        self,
        value: Mapping[str, Any],
        requirement_ids: tuple[str, ...] = REQUIREMENT_IDS,
        *,
        standard_behavior: bool = True,
    ) -> None:
        self.value = value
        self.requirement_ids = requirement_ids
        self.standard_behavior = standard_behavior
        self.errors: list[str] = []
        self.attempts: dict[str, Mapping[str, Any]] = {}
        self.selected: dict[str, Mapping[str, Any]] = {}
        self.events: list[Mapping[str, Any]] = []
        self.profile: Mapping[str, Any] = {}

    def error(self, message: str, kind: str = "INCOMPLETE") -> None:
        self.errors.append(f"{kind}: {message}")

    def mapping(self, value: Any, path: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            self.error(f"{path} must be an object")
            return {}
        return value

    def records(self, value: Any, path: str, *, nonempty: bool = False) -> list[Mapping[str, Any]]:
        if not isinstance(value, list):
            self.error(f"{path} must be an array")
            return []
        if nonempty and not value:
            self.error(f"{path} must not be empty")
        return [self.mapping(item, f"{path}[{index}]") for index, item in enumerate(value)]

    def string(self, value: Any, path: str) -> str:
        if not isinstance(value, str) or not value.strip():
            self.error(f"{path} must be a non-empty string")
            return ""
        return value

    def number(self, value: Any, path: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            self.error(f"{path} must be a finite non-negative number")
            return 0
        return float(value)

    def event(self, request: Mapping[str, Any], kind: str) -> Mapping[str, Any]:
        matches = [
            event
            for event in self.events
            if event.get("requestId") == request.get("requestId") and event.get("kind") == kind
        ]
        if len(matches) != 1:
            self.error(f"request {request.get('requestId')} needs exactly one {kind} event")
            return {}
        return matches[0]

    def require_roles(self, *roles: str) -> bool:
        missing = [role for role in roles if role not in self.selected]
        if missing:
            self.error(f"selectedRequests is missing roles: {', '.join(missing)}")
        return not missing

    def common(self) -> None:
        value = self.value
        if type(value.get("schemaVersion")) is not int or value.get("schemaVersion") != SCHEMA_VERSION:
            self.error(f"unsupported schemaVersion; expected {SCHEMA_VERSION}")
        for key in ("runId", "caseId", "cohortId"):
            self.string(value.get(key), key)
        if value.get("runtime") not in RUNTIMES:
            self.error(f"runtime must be one of {RUNTIMES}")
        if value.get("requirementId") not in self.requirement_ids:
            self.error("unknown requirementId")
        artifact = self.mapping(value.get("artifact"), "artifact")
        for key in ("sdk", "conformance"):
            self.string(artifact.get(key), f"artifact.{key}")
        self.profile = self.mapping(value.get("profile"), "profile")
        for key in ("functionArn", "configurationDigest"):
            self.string(self.profile.get(key), f"profile.{key}")
        concurrency = self.profile.get("nativeConcurrency")
        if type(concurrency) is not int or concurrency < 1:
            self.error("profile.nativeConcurrency must be a positive integer")
        if self.number(self.profile.get("timeoutMs"), "profile.timeoutMs") <= 0:
            self.error("profile.timeoutMs must be positive")
        observation_end = self.number(value.get("observationEndedAtMs"), "observationEndedAtMs")
        for attempt in self.records(value.get("attempts"), "attempts", nonempty=True):
            request_id = self.string(attempt.get("requestId"), "attempt.requestId")
            if request_id in self.attempts:
                self.error(f"duplicate requestId {request_id}")
            self.attempts[request_id] = attempt
            for key in ("executionArn", "environmentId", "workerId", "processId", "role"):
                self.string(attempt.get(key), f"attempt {request_id}.{key}")
            if type(attempt.get("attempt")) is not int or attempt["attempt"] < 1:
                self.error(f"attempt {request_id}.attempt must be a positive integer")
            start = self.number(attempt.get("startedAtMs"), f"attempt {request_id}.startedAtMs")
            end = self.number(attempt.get("endedAtMs"), f"attempt {request_id}.endedAtMs")
            if end <= start or end > observation_end:
                self.error(f"attempt {request_id} has an invalid observation interval")
            if attempt.get("outcome") not in ("SUCCEEDED", "FAILED", "PENDING", "TIMED_OUT"):
                self.error(f"attempt {request_id} has no recognized invocation outcome")
        selected = self.mapping(value.get("selectedRequests"), "selectedRequests")
        if not selected:
            self.error("selectedRequests must not be empty")
        seen: set[str] = set()
        for role, request_id in selected.items():
            if not isinstance(request_id, str) or request_id not in self.attempts:
                self.error(f"selectedRequests.{role} references a missing attempt")
                continue
            if request_id in seen:
                self.error("distinct selected roles must bind distinct original requests", "PLACEMENT")
            seen.add(request_id)
            attempt = self.attempts[request_id]
            if attempt.get("role") != role or attempt.get("attempt") != 1:
                self.error(f"selectedRequests.{role} is a retry or a substituted role", "PLACEMENT")
            self.selected[role] = attempt
        self.events = self.records(value.get("events"), "events", nonempty=True)
        for event in self.events:
            self.string(event.get("kind"), "event.kind")
            self.owned_record(event, "event", observation_end)
        for request in self.records(value.get("sdkRequests"), "sdkRequests"):
            self.owned_record(request, "sdkRequest", observation_end)
            self.string(request.get("method"), "sdkRequest.method")
            if request.get("outcome") not in ("DISPATCHED", "REJECTED"):
                self.error("sdkRequest.outcome must be DISPATCHED or REJECTED")
        self.histories()
        collection = self.mapping(value.get("collection"), "collection")
        watermarks = self.mapping(collection.get("watermarks"), "collection.watermarks")
        for source in ("lifecycle", "sdkRequests", "history"):
            if self.number(watermarks.get(source), f"collection.watermarks.{source}") < observation_end:
                self.error(f"{source} collection does not cover the observation window")
        references = collection.get("evidenceUris")
        if (
            not isinstance(references, list)
            or not references
            or any(not isinstance(uri, str) or not uri for uri in references)
        ):
            self.error("collection.evidenceUris must retain the raw evidence references")
        self.no_late_dispatch()

    def owned_record(self, record: Mapping[str, Any], kind: str, end: float) -> None:
        request_id = record.get("requestId")
        if not isinstance(request_id, str) or request_id not in self.attempts:
            self.error(f"{kind} references an unrecorded request")
            return
        if record.get("ownerRequestId") != request_id:
            self.error(f"{kind} is attributed to a different invocation", "ASSERTION")
        if record.get("executionArn") != self.attempts[request_id].get("executionArn"):
            self.error(f"{kind} is attributed to a different execution", "ASSERTION")
        time = self.number(record.get("atMs"), f"{kind}.atMs")
        if time > end or time < self.attempts[request_id].get("startedAtMs", 0):
            self.error(f"{kind} lies outside its observation window")
        observed_during = record.get("observedDuringRequestId")
        if observed_during is not None and observed_during not in self.attempts:
            self.error(f"{kind}.observedDuringRequestId references an unrecorded request")

    def histories(self) -> None:
        expected = self.mapping(self.value.get("expectedHistories"), "expectedHistories")
        actual: dict[str, Mapping[str, Any]] = {}
        for history in self.records(self.value.get("histories"), "histories", nonempty=True):
            arn = self.string(history.get("executionArn"), "history.executionArn")
            if arn in actual:
                self.error(f"duplicate service history {arn}")
            actual[arn] = history
            if history.get("status") not in HISTORY_STATUSES:
                self.error(f"history {arn}.status must be a recognized execution status")
            if history.get("ownerCaseId") != self.value.get("caseId"):
                self.error(f"history {arn} belongs to a different case", "ASSERTION")
            operations = self.records(history.get("operations"), "history.operations")
            ids: set[str] = set()
            for operation in operations:
                for key in ("id", "type", "status"):
                    self.string(operation.get(key), f"history operation.{key}")
                if "name" not in operation:
                    self.error("history operation.name is required (null for an unnamed operation)")
                elif operation["name"] is not None:
                    self.string(operation["name"], "history operation.name")
                if "parentId" not in operation:
                    self.error("history operation.parentId is required (null for the root)")
                operation_id = operation.get("id")
                if operation_id in ids:
                    self.error("duplicate operation identity in service history")
                if isinstance(operation_id, str):
                    ids.add(operation_id)
            expectation = self.mapping(expected.get(arn), f"expectedHistories.{arn}")
            allow_running = False
            if ("status" in expectation) == ("allowedStatuses" in expectation):
                self.error(f"expectedHistories.{arn} must declare exactly one of status or allowedStatuses")
            if "allowedStatuses" in expectation:
                allowed = expectation["allowedStatuses"]
                if (
                    not isinstance(allowed, list)
                    or not allowed
                    or any(not isinstance(status, str) or status not in HISTORY_STATUSES for status in allowed)
                    or len(set(allowed)) != len(allowed)
                ):
                    self.error(
                        f"expectedHistories.{arn}.allowedStatuses must contain distinct recognized execution statuses"
                    )
                else:
                    allow_running = "RUNNING" in allowed
                    if history.get("status") not in allowed:
                        self.error(f"service history {arn}.status is outside the declared allowedStatuses", "ASSERTION")
            elif expectation.get("status") not in HISTORY_STATUSES:
                self.error(f"expectedHistories.{arn}.status must be a recognized execution status")
            if not allow_running and "result" not in expectation and "error" not in expectation:
                self.error(f"history {arn} needs an independently declared expected result/error")
            for key, item in expectation.items():
                if key != "allowedStatuses" and history.get(key) != item:
                    self.error(f"service history {arn}.{key} differs from the fixture expectation", "ASSERTION")
        arns = {attempt.get("executionArn") for attempt in self.attempts.values()}
        if set(actual) != arns or set(expected) != arns:
            self.error("histories and expectedHistories must cover every launched execution exactly")

    def no_late_dispatch(self) -> None:
        for request_id in self.attempts:
            closures = [
                event["atMs"]
                for event in self.events
                if event.get("requestId") == request_id
                and event.get("kind") in ("wrapper_return", "admission_closed")
                and isinstance(event.get("atMs"), (float, int))
            ]
            if not closures:
                continue
            closed = min(closures)
            for request in self.value.get("sdkRequests", []):
                if (
                    isinstance(request, Mapping)
                    and request.get("requestId") == request_id
                    and request.get("outcome") == "DISPATCHED"
                    and isinstance(request.get("atMs"), (float, int))
                    and request["atMs"] >= closed
                ):
                    self.error(f"request {request_id} dispatched SDK work after admission closed", "ASSERTION")

    def overlap(self) -> None:
        if not self.require_roles("victim", "peer"):
            return
        victim, peer = self.selected["victim"], self.selected["peer"]
        if victim.get("executionArn") == peer.get("executionArn"):
            self.error("overlap needs distinct durable executions", "PLACEMENT")
        if victim.get("environmentId") != peer.get("environmentId"):
            self.error("selected requests did not overlap in the same environment", "PLACEMENT")
        runtime = self.value.get("runtime")
        if runtime == "javascript" and any(victim.get(key) != peer.get(key) for key in ("workerId", "processId")):
            self.error("JavaScript overlap requires the same Node.js worker and process", "PLACEMENT")
        if runtime == "java" and victim.get("processId") != peer.get("processId"):
            self.error("Java overlap requires the same JVM process", "PLACEMENT")
        if runtime == "python" and victim.get("processId") == peer.get("processId"):
            self.error("Python native overlap requires different processes", "PLACEMENT")
        if self.profile.get("nativeConcurrency", 0) < 2:
            self.error("native overlap requires a deployed concurrency of at least two", "PLACEMENT")
        start = max(victim.get("startedAtMs", 0), peer.get("startedAtMs", 0))
        end = min(victim.get("endedAtMs", 0), peer.get("endedAtMs", 0))
        if start >= end:
            self.error("selected invocation intervals do not overlap", "PLACEMENT")
        for attempt in (victim, peer):
            barrier = self.event(attempt, "barrier_arrived")
            if not start <= barrier.get("atMs", -1) < end:
                self.error("cohort barrier was not established inside actual native overlap", "PLACEMENT")

    def disposal(self) -> None:
        if not self.require_roles("victim"):
            return
        victim = self.selected["victim"]
        root = self.event(victim, "root_completed")
        wrapper = self.event(victim, "wrapper_return")
        if root.get("outcome") not in ("SUCCEEDED", "FAILED") or root.get("outcome") != victim.get("outcome"):
            self.error("root completion and actual invocation outcome disagree", "ASSERTION")
        if root.get("atMs", 0) > wrapper.get("atMs", 0):
            self.error("wrapper return precedes root completion", "ASSERTION")
        self.rejected_probe(victim, wrapper.get("atMs", 0))
        exited = self.event(victim, "task_exit")
        if exited.get("atMs", -1) < root.get("atMs", 0):
            self.error("task exit preceded root completion in the disposal fixture", "ASSERTION")

    def rejected_probe(self, victim: Mapping[str, Any], closed: float) -> None:
        for probe in ("operation", "checkpoint"):
            observations = [
                event
                for event in self.events
                if event.get("requestId") == victim.get("requestId")
                and event.get("kind") == "admission_probe"
                and event.get("probe") == probe
                and event.get("atMs", -1) >= closed
            ]
            if not observations:
                self.error(f"missing post-closure {probe} admission probe")
            if any(event.get("outcome") != "REJECTED" for event in observations):
                self.error(f"closed invocation admitted a {probe} probe", "ASSERTION")

    def deadline(self) -> None:
        if not self.require_roles("victim"):
            return
        victim = self.selected["victim"]
        timeout = self.event(victim, "platform_timeout")
        inflight = self.event(victim, "work_in_flight")
        closed = self.event(victim, "admission_closed")
        exited = self.event(victim, "task_exit")
        time = timeout.get("atMs", 0)
        if time < victim.get("startedAtMs", 0) + self.profile.get("timeoutMs", 0):
            self.error("platform timeout did not cross the configured invocation deadline", "ASSERTION")
        if victim.get("outcome") != "TIMED_OUT" or timeout.get("source") != "platform":
            self.error("deadline requires an observed platform timeout, not a simulated cancellation", "ASSERTION")
        if not inflight.get("atMs", time) < time <= exited.get("atMs", 0):
            self.error("controlled work was not in flight across the real deadline", "ASSERTION")
        budgets = self.mapping(self.value.get("budgets"), "budgets")
        for field, event in (("admissionMs", closed), ("cleanupMs", exited)):
            budget = self.number(budgets.get(field), f"budgets.{field}")
            if not time <= event.get("atMs", -1) <= time + budget:
                self.error(f"deadline {field} exceeds the declared fixture budget", "ASSERTION")
        self.rejected_probe(victim, closed.get("atMs", 0))
        for fault in self.records(self.value.get("faultsCreated"), "faultsCreated"):
            release = fault.get("releasedAtMs")
            if isinstance(release, (int, float)) and release <= exited.get("atMs", 0):
                self.error("fault release supplied the cleanup being asserted", "ASSERTION")

    def recovery(self) -> None:
        self.overlap()
        self.deadline()
        if not self.require_roles("victim", "peer", "recovery"):
            return
        victim, recovery = self.selected["victim"], self.selected["recovery"]
        timeout = self.event(victim, "platform_timeout").get("atMs", 0)
        recovered = self.event(recovery, "worker_recovered").get("atMs", 0)
        if recovery.get("environmentId") != victim.get("environmentId"):
            self.error("replacement environment cannot establish worker recovery", "PLACEMENT")
        policy = self.profile.get("recoveryPolicy")
        if policy == "reuse-worker":
            if any(recovery.get(key) != victim.get(key) for key in ("workerId", "processId")):
                self.error("recovery substituted a different worker/process", "PLACEMENT")
        elif policy == "replace-worker":
            if recovery.get("replacesWorkerId") != victim.get("workerId") or recovery.get("workerId") == victim.get(
                "workerId"
            ):
                self.error("replacement must identify the original failed worker capacity", "PLACEMENT")
        else:
            self.error("profile.recoveryPolicy must explicitly select reuse-worker or replace-worker")
        if recovery.get("outcome") != "SUCCEEDED":
            self.error("recovered capacity did not complete the selected recovery request", "ASSERTION")
        if recovery.get("executionArn") in (victim.get("executionArn"), self.selected["peer"].get("executionArn")):
            self.error("recovery must be an independent request, not a service retry", "PLACEMENT")
        if recovery.get("startedAtMs", 0) < timeout or recovered < recovery.get("startedAtMs", 0):
            self.error("recovery preceded the selected victim's timeout", "ASSERTION")
        peers = [attempt for role, attempt in self.selected.items() if role.startswith("peer")]
        if len(peers) != self.profile.get("nativeConcurrency", 0) - 1:
            self.error("recovery must occupy every other native capacity slot", "PLACEMENT")
        for peer in peers:
            if peer.get("environmentId") != victim.get("environmentId") or not peer.get(
                "startedAtMs", 0
            ) < timeout < recovered <= peer.get("endedAtMs", 0):
                self.error("established peer capacity was not held through recovery", "PLACEMENT")
            progress = [
                event.get("atMs", 0)
                for event in self.events
                if event.get("kind") == "peer_progress" and event.get("requestId") == peer.get("requestId")
            ]
            if not any(time < timeout for time in progress) or not any(
                timeout < time <= recovered for time in progress
            ):
                self.error("healthy peer lacks independent progress across the timeout", "ASSERTION")
        for fault in self.records(self.value.get("faultsCreated"), "faultsCreated"):
            if isinstance(fault.get("releasedAtMs"), (int, float)) and fault["releasedAtMs"] <= recovered:
                self.error("fault release cannot satisfy worker recovery", "ASSERTION")

    def replay(self) -> None:
        if not self.require_roles("victim", "resume"):
            return
        victim, resume = self.selected["victim"], self.selected["resume"]
        pending = self.event(victim, "pending")
        wrapper = self.event(victim, "wrapper_return")
        if victim.get("outcome") != "PENDING" or victim.get("executionArn") != resume.get("executionArn"):
            self.error("resume must continue the original suspended execution", "ASSERTION")
        if not pending.get("atMs", 0) <= wrapper.get("atMs", 0) < resume.get("startedAtMs", 0):
            self.error("resume did not follow a real PENDING wrapper return", "ASSERTION")
        replay = self.mapping(self.value.get("replay"), "replay")
        before = self.records(replay.get("before"), "replay.before", nonempty=True)
        after = self.records(replay.get("after"), "replay.after", nonempty=True)
        if len(after) < len(before) or after[: len(before)] != before:
            self.error("replay changed completed operation identity, result, or failure", "ASSERTION")
        ids: set[str] = set()
        for operation in before:
            for field in ("id", "name", "type", "status"):
                self.string(operation.get(field), f"replay.before.{field}")
            if "parentId" not in operation:
                self.error("replay.before requires stable parentId")
            operation_id = operation.get("id")
            if operation_id in ids:
                self.error("replay.before repeats an operation identity")
            if isinstance(operation_id, str):
                ids.add(operation_id)
            outcome_key = "result" if operation.get("status") == "SUCCEEDED" else "error"
            if outcome_key not in operation:
                self.error(f"replay.before must preserve the stored {outcome_key}")
        final_history = next(
            history for history in self.value["histories"] if history["executionArn"] == victim["executionArn"]
        )
        if final_history.get("operations") != after:
            self.error("replay.after differs from the collected service history", "ASSERTION")
        statuses = {operation.get("status") for operation in before}
        if not {"SUCCEEDED", "FAILED"} <= statuses:
            self.error("replay must preserve both a completed success and a completed failure")
        completed = {operation.get("id") for operation in before}
        executions = self.records(replay.get("operationExecutions"), "replay.operationExecutions", nonempty=True)
        for execution in executions:
            if execution.get("requestId") not in self.attempts:
                self.error("operationExecutions references an unrecorded request")
            if execution.get("requestId") == resume.get("requestId") and execution.get("operationId") in completed:
                self.error("resume repeated completed user code", "ASSERTION")
        retry = self.mapping(replay.get("retry"), "replay.retry")
        retry_id = self.string(retry.get("operationId"), "replay.retry.operationId")
        retry_attempts = self.records(retry.get("attempts"), "replay.retry.attempts", nonempty=True)
        retry_attempt_ids = [self.string(item.get("attemptId"), "retry.attemptId") for item in retry_attempts]
        if len(retry_attempt_ids) < 2 or len(set(retry_attempt_ids)) != len(retry_attempt_ids):
            self.error("retry requires distinct observed attempts")
        if retry_attempts and (
            retry_attempts[0].get("outcome") != "FAILED" or retry_attempts[-1].get("outcome") != "SUCCEEDED"
        ):
            self.error("retry must demonstrate a failed attempt followed by success", "ASSERTION")
        for retry_attempt in retry_attempts:
            if retry_attempt.get("requestId") not in self.attempts:
                self.error("retry references an unrecorded request")
            if not any(
                item.get("operationId") == retry_id
                and item.get("attemptId") == retry_attempt.get("attemptId")
                and item.get("requestId") == retry_attempt.get("requestId")
                for item in executions
            ):
                self.error("retry attempt lacks observed user-code execution")
        if retry_id in completed:
            self.error("retry must not repeat an already completed operation", "ASSERTION")
        counts = self.records(replay.get("sideEffects"), "replay.sideEffects", nonempty=True)
        if not completed | {retry_id} <= {item.get("operationId") for item in counts}:
            self.error("side-effect counts must cover stored successes, failures, and retries")
        for count in counts:
            actual = count.get("count")
            expected = count.get("expectedCount")
            if type(actual) is not int or type(expected) is not int or actual < 0 or expected < 0:
                self.error("side-effect counts must be non-negative integers")
            elif actual != expected:
                self.error("side-effect count differs from the fixture's retry policy", "ASSERTION")

    def warm_reuse(self) -> None:
        if not self.require_roles("victim", "warm"):
            return
        victim, warm = self.selected["victim"], self.selected["warm"]
        closed = self.event(victim, "wrapper_return")
        if any(victim.get(key) != warm.get(key) for key in ("environmentId", "workerId", "processId")):
            self.error("warm reuse must identify the original process and worker", "PLACEMENT")
        if warm.get("startedAtMs", 0) <= closed.get("atMs", 0):
            self.error("warm invocation was not sequential reuse", "PLACEMENT")
        late = self.event(victim, "late_activity")
        if late.get("observedDuringRequestId") != warm.get("requestId") or not warm.get("startedAtMs", 0) <= late.get(
            "atMs", -1
        ) <= warm.get("endedAtMs", 0):
            self.error("late activity was not observed during the selected warm invocation", "ASSERTION")
        exited = self.event(victim, "task_exit")
        if exited.get("atMs", 0) < late.get("atMs", 0):
            self.error("late activity continued after the recorded task exit", "ASSERTION")

    def retirement(self) -> None:
        retirement = self.mapping(self.value.get("retirement"), "retirement")
        end = self.number(retirement.get("retiredAtMs"), "retirement.retiredAtMs")
        if end < self.value.get("observationEndedAtMs", 0):
            self.error("retirement precedes completion of assertions")
        created_resources = self.records(self.value.get("resourcesCreated"), "resourcesCreated")
        identities = {(item.get("kind"), item.get("id")) for item in created_resources}
        required = {("invocation", item["requestId"]) for item in self.attempts.values()}
        required.update(("execution", item["executionArn"]) for item in self.attempts.values())
        if not required <= identities:
            self.error("resourcesCreated must retain ownership of every launched invocation and execution")
        for created_key, settled_key, time_key in (
            ("resourcesCreated", "resources", "settledAtMs"),
            ("faultsCreated", "faults", "releasedAtMs"),
        ):
            created = self.records(self.value.get(created_key), created_key)
            settled = self.records(retirement.get(settled_key), f"retirement.{settled_key}")
            created_ids = [self.string(item.get("id"), f"{created_key}.id") for item in created]
            settled_ids = [self.string(item.get("id"), f"retirement.{settled_key}.id") for item in settled]
            if (
                len(set(created_ids)) != len(created_ids)
                or len(set(settled_ids)) != len(settled_ids)
                or set(created_ids) != set(settled_ids)
            ):
                self.error(f"retirement must settle every owned {settled_key} exactly once")
            for item in (*created, *settled):
                if item.get("ownerCaseId") != self.value.get("caseId"):
                    self.error("cleanup attempted to change another case's resources", "ASSERTION")
            for item in settled:
                time = self.number(item.get(time_key), f"retirement.{settled_key}.{time_key}")
                if time > end:
                    self.error("retirement ended before owned work settled")
                if settled_key == "resources":
                    self.string(item.get("evidenceUri"), "retirement.resources.evidenceUri")
                    kind = self.string(item.get("kind"), "retirement.resources.kind")
                    original = next((resource for resource in created if resource.get("id") == item.get("id")), {})
                    if original.get("kind") != kind:
                        self.error("retirement changed an owned resource's kind")
                    if kind == "execution" and item.get("outcome") not in (
                        "SUCCEEDED",
                        "FAILED",
                        "TIMED_OUT",
                        "STOPPED",
                    ):
                        self.error("an execution may still resume after retirement")
                    if kind == "invocation" and item.get("outcome") != "QUIESCENT":
                        self.error("invocation quiescence is not established")
                if settled_key == "faults":
                    if item.get("mode") not in ("normal", "emergency"):
                        self.error("fault release must record normal or emergency mode")
                    if self.standard_behavior and self.value.get("requirementId") in (
                        "lmi-lifecycle-3",
                        "lmi-lifecycle-4",
                    ):
                        asserted = [
                            event.get("atMs", 0)
                            for event in self.events
                            if event.get("kind") in ("task_exit", "worker_recovered")
                        ]
                        if asserted and time <= max(asserted):
                            self.error("fault release cannot retroactively establish cleanup or recovery", "ASSERTION")


def validate_evidence(evidence: Mapping[str, Any], *, phase: Literal["observations", "final"] = "final") -> list[str]:
    """Validate all observations, then optionally case-owned retirement.

    The coordinator calls ``observations`` before releasing faults and retains
    those errors even if emergency release later succeeds. ``final`` additionally
    requires complete retirement evidence. Neither phase validates OTel spans;
    requirements 7/8 also require the existing OTel extension's LMI validator.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: evidence must be an object"]
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    validator = _Evidence(evidence)
    try:
        validator.common()
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed evidence: {error}")
    # Do not interpret malformed structures as behavior observations. Retirement
    # is still checked independently: pre-existing observation errors cannot
    # suppress quiescence checks before the scheduler grants another lease.
    if not validator.errors:
        try:
            _validate_observations(validator, evidence)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed evidence: {error}")
    if phase == "final":
        try:
            validator.retirement()
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed retirement evidence: {error}")
    return validator.errors


def validate_common_evidence(
    evidence: Mapping[str, Any],
    *,
    phase: Literal["observations", "final"] = "final",
    requirement_ids: tuple[str, ...] = REQUIREMENT_IDS,
) -> list[str]:
    """Validate shared ownership and retirement for an explicit scenario catalog.

    Callers must also invoke their catalog's behavior validator. This helper does
    not validate a scenario merely because its ownership envelope is complete.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: evidence must be an object"]
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    validator = _Evidence(evidence, requirement_ids, standard_behavior=False)
    try:
        validator.common()
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed evidence: {error}")
    if phase == "final":
        try:
            validator.retirement()
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed retirement evidence: {error}")
    return validator.errors


def _validate_observations(validator: _Evidence, evidence: Mapping[str, Any]) -> None:
    requirement = evidence.get("requirementId")
    checks = {
        "lmi-lifecycle-1": validator.overlap,
        "lmi-lifecycle-2": validator.disposal,
        "lmi-lifecycle-3": validator.deadline,
        "lmi-lifecycle-4": validator.recovery,
        "lmi-lifecycle-5": validator.replay,
        "lmi-lifecycle-6": validator.warm_reuse,
        "lmi-lifecycle-7": validator.overlap,
    }
    if requirement in checks:
        checks[requirement]()
    if requirement == "lmi-lifecycle-7":
        validator.warm_reuse()
