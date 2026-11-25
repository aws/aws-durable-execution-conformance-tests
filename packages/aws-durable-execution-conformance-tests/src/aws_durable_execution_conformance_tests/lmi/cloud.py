# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Bounded real-service driver using the repository's cloud runner."""

import hashlib
import json
import math
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Event, Lock
from typing import Any

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_conformance_tests.lmi.artifacts import (
    case_key,
    case_output_directory,
    save,
    saved_case_plans,
    sdk_artifact_directory,
)
from aws_durable_execution_conformance_tests.lmi.catalog import COVERAGE_PROFILES
from aws_durable_execution_conformance_tests.lmi.errors import (
    CollectionBudgetExpired,
    CollectionError,
    ProvisioningError,
    TransientCollectionError,
)
from aws_durable_execution_conformance_tests.lmi.observations import (
    check_controls,
    is_platform_timeout,
    select,
    service_native_intervals,
)
from aws_durable_execution_conformance_tests.lmi.rules.controls import invoke_rejection_proven
from aws_durable_execution_conformance_tests.lmi.runtime import CONCURRENCIES, QUALIFIER
from aws_durable_execution_conformance_tests.lmi.service import (
    DurableFunctionRunner,
    client,
    definitive_s3_write_rejection,
    release_control,
    retryable_read_error,
)


def require_execution_name(arn: str, target: str, execution_name: str) -> None:
    """Check both the target and the transport name against the launch journal."""
    prefix = f"{target}/durable-execution/{execution_name}/"
    if (
        not isinstance(execution_name, str)
        or not execution_name
        or "/" in execution_name
        or not isinstance(arn, str)
        or not arn.startswith(prefix)
        or not arn[len(prefix) :]
        or "/" in arn[len(prefix) :]
    ):
        raise CollectionError("Execution ARN does not match the journaled launch")


def require_ledger_launch(event, payload) -> None:
    for field in ("run", "caseId", "marker"):
        if event.get(field) != payload.get(field):
            raise CollectionError(f"Ledger invocation does not match the journaled {field}")


def find_execution_by_name(service, target: str, name: str, payload: dict[str, Any]) -> str | None:
    """Find a unique service-identified execution for the qualified launch."""
    function, qualifier = target.rsplit(":", 1)
    request = {"FunctionName": function, "Qualifier": qualifier, "DurableExecutionName": name}
    found = set()
    while True:
        page = service.list_durable_executions_by_function(**request)
        for item in page.get("DurableExecutions", []):
            if item.get("DurableExecutionName") == name:
                arn = item.get("DurableExecutionArn")
                require_execution_name(arn, target, name)
                found.add(arn)
        if not page.get("NextMarker"):
            break
        request["Marker"] = page["NextMarker"]
    if len(found) > 1:
        raise CollectionError("Multiple service execution ARNs matched one qualified launch")
    arn = next(iter(found), None)
    if arn is None:
        return None
    result = service.get_durable_execution(DurableExecutionArn=arn, IncludeExecutionData=True)
    try:
        actual = json.loads(result["InputPayload"])
    except (KeyError, TypeError, ValueError) as error:
        raise CollectionError("Service did not return the submitted invocation input") from error
    if json.dumps(actual, sort_keys=True) != json.dumps(payload, sort_keys=True):
        raise CollectionError("Service execution input does not match the journaled launch")
    return arn


class RunnerClient:
    """Share paced history reads with the public runner; other APIs are unchanged."""

    def __init__(self, cloud, *, before_call=None, before_invoke=None):
        self.cloud = cloud
        self.before_call = before_call
        self.before_invoke = before_invoke

    def invoke(self, **kwargs):
        if self.before_call is not None:
            self.before_call()
        if self.before_invoke is not None:
            self.before_invoke()
        return self.cloud.lam.invoke(**kwargs)

    def __getattr__(self, name):
        operation = getattr(self.cloud.lam, name)
        if not callable(operation):
            return operation

        def guarded(**kwargs):
            if self.before_call is not None:
                self.before_call()
            return operation(**kwargs)

        return guarded

    def get_durable_execution_history(self, **kwargs):
        return self.cloud.history_page(**kwargs)


class Cloud:
    def __init__(
        self,
        manifest,
        configuration=None,
        *,
        artifacts_dir,
        verify_profile=None,
        case_context=None,
        client_factory=None,
    ):
        self.manifest = manifest
        self.configuration = configuration
        factory = client_factory or client
        self.lam, self.s3, self.logs = (factory(service, manifest["region"]) for service in ("lambda", "s3", "logs"))
        self.single_attempt_fences = client_factory is None
        # A missing explicit verifier defaults to the shared profile checks,
        # bound to the runtime recorded in this deployment's own manifest.
        self.verify_profile = verify_profile if verify_profile is not None else self._default_verify_profile
        self.events = {}
        # Set whenever a refresh caches new ledger records; cleared only after
        # a successful bind while no marker is incomplete (binding from a
        # partial read could bind a retry as the original).
        self.binding_deferred = False
        # Markers whose most recent covering refresh left their listing or
        # reads incomplete. A marker is removed once a refresh that covers it
        # reads everything; until then binding skips it, even in refreshes of
        # other markers (see `refresh`/`_bind_requests`).
        self._incomplete_markers: set[str] = set()
        self.invocations = []
        self.rejected_markers: set[str] = set()
        self.gates = set()
        self.released_gates: set[str] = set()
        self._next_history_read = 0.0
        self.artifacts_dir = Path(artifacts_dir)
        self.case_context = case_context
        self.cancel_event = Event()
        self._history_lock = Lock()
        self.request_roles: dict[str, str] = {}
        self.raw_histories: dict[str, list[dict[str, Any]]] = {}
        self.terminal_histories: set[str] = set()
        self.execution_results: dict[str, dict[str, Any]] = {}
        self.cutoff_results: dict[str, dict[str, Any]] = {}
        self.control_records: list[dict[str, Any]] = []
        self.source_uris: set[str] = set()
        self.control_prefix = f"runs/{manifest['run']}/control/"
        if case_context is not None:
            owner = case_key(case_context.scenario.case_id)
            self.control_prefix = f"runs/{manifest['run']}/cases/{owner}/control/"

    def _default_verify_profile(self, config, scaling, manifest, key) -> None:
        from aws_durable_execution_conformance_tests.lmi.deployment import verify_profile
        from aws_durable_execution_conformance_tests.lmi.runtime import RuntimeDefinition

        verify_profile(RuntimeDefinition.from_manifest(manifest), config, scaling, manifest, key)

    def _remaining_collection_time(self) -> float | None:
        deadline = getattr(self.case_context, "_collection_deadline", None)
        if isinstance(deadline, (float, int)) and not isinstance(deadline, bool):
            return deadline - time.monotonic()
        return None

    def _check_read_budget(self, transport) -> None:
        self.check_active()
        remaining = self._remaining_collection_time()
        config = getattr(getattr(transport, "meta", None), "config", None)
        if remaining is None or not isinstance(config, Config):
            return
        retries = config.retries or {}
        attempts = retries.get("total_max_attempts")
        if type(attempts) is not int or attempts < 1 or retries.get("mode") == "adaptive":
            return
        # Reserve each configured connect/read attempt and standard retry
        # backoff. Socket inactivity limits aren't total operation deadlines.
        timeouts = (config.connect_timeout, config.read_timeout)
        if any(type(timeout) not in {int, float} or not 0 < timeout < math.inf for timeout in timeouts):
            return
        allowance = attempts * sum(timeouts) + 20 * (attempts - 1)
        if remaining <= allowance:
            raise CollectionBudgetExpired("Insufficient collection time to start another AWS read")

    def _read_transport(self, service: str, ordinary):
        self.check_active()
        remaining = self._remaining_collection_time()
        if remaining is None or not getattr(self, "single_attempt_fences", False):
            self._check_read_budget(ordinary)
            return ordinary
        # Default AWS clients can use a separate single-attempt read transport.
        # Socket inactivity limits do not replace the outer worker watchdog.
        reserve = 1.0
        if remaining <= reserve:
            raise CollectionBudgetExpired("Observation collection time reserved for retained evidence")
        budget = (remaining - reserve) / 3
        transport = client(
            service,
            self.manifest["region"],
            single_attempt=True,
            connect_timeout=min(3, budget),
            read_timeout=min(10, budget),
        )
        self._check_read_budget(transport)
        return transport

    def check_active(self) -> None:
        if self.cancel_event.is_set():
            raise CollectionError("Case driver was cancelled")
        remaining = self._remaining_collection_time()
        if remaining is not None and remaining <= 0:
            raise CollectionBudgetExpired("Observation collection budget expired")

    def cancel(self) -> None:
        """Request cooperative stop before joining this case's blocking worker."""
        self.cancel_event.set()

    def wait(self, seconds: float) -> None:
        self.check_active()
        remaining = self._remaining_collection_time()
        if remaining is not None:
            seconds = min(seconds, remaining)
        if self.case_context is None:
            time.sleep(max(0, seconds))
        elif self.cancel_event.wait(max(0, seconds)) or remaining is not None:
            self.check_active()

    def _save(self, relative: str, data: Any, *, exclusive=False) -> None:
        root = self.artifacts_dir.resolve()
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or path == root:
            raise CollectionError("Artifact path is outside the run directory")
        save(path, data, exclusive=exclusive)
        self.source_uris.add(path.as_uri())

    def _require_execution_target(self, arn: str) -> None:
        if self.case_context is not None and not arn.startswith(
            self.manifest["functions"][self.configuration] + "/durable-execution/"
        ):
            raise CollectionError("Execution is outside the case-owned function")

    def _bind_requests(self, events: list[dict[str, Any]], incomplete: set[str] | None = None) -> None:
        if self.case_context is None:
            return
        entries: dict[str, list[dict[str, Any]]] = defaultdict(list)
        markers_by_request: dict[str, set[str]] = defaultdict(set)
        errors = []
        unknown_marker = False
        for event in events:
            if event.get("phase") != "WRAPPER_ENTER":
                continue
            marker, request = event.get("marker"), event.get("request")
            if not isinstance(marker, str) or not marker:
                unknown_marker = True
                errors.append("Wrapper entry has no identified launch marker")
                continue
            entries[marker].append(event)
            if isinstance(request, str) and request:
                markers_by_request[request].add(marker)
        by_marker = {item["marker"]: item for item in self.invocations}
        for marker, item in by_marker.items():
            observed = entries.pop(marker, [])
            if item.get("launch_conflict"):
                continue
            acknowledged = item.get("invoke_acknowledged", False) or item.get("service_identity_verified", False)
            if unknown_marker or (incomplete is not None and marker in incomplete):
                if not acknowledged:
                    item["arn"] = None
                continue
            if not observed:
                continue
            try:
                if any(
                    not isinstance(event.get(field), str) or not event[field]
                    for event in observed
                    for field in ("request", "execution")
                ):
                    raise CollectionError("Wrapper entry has a malformed native request or execution ARN")
                if any(len(markers_by_request[event["request"]]) > 1 for event in observed):
                    raise CollectionError("Native invocation appears under more than one launch")
                arns = {event["execution"] for event in observed}
                if acknowledged and arns != {item["arn"]}:
                    raise CollectionError("An invocation changed its original durable execution")
                if not acknowledged:
                    for event in observed:
                        require_ledger_launch(event, item["payload"])
                        require_execution_name(
                            event["execution"], self.manifest["functions"][self.configuration], item["execution_name"]
                        )
                    if len(arns) != 1:
                        raise CollectionError("Multiple execution ARNs matched one journaled launch")
                    arn = next(iter(arns))
                    if item.get("arn") and item["arn"] != arn:
                        raise CollectionError("An invocation changed its original durable execution")
                    item["arn"] = arn
            except CollectionError as error:
                if not acknowledged:
                    item["arn"] = None
                errors.append(str(error))
                continue
            try:
                if any(
                    not isinstance(event.get(field), (int, float))
                    for event in observed
                    for field in ("time", "sequence")
                ):
                    raise CollectionError("Wrapper entry has malformed timing or sequence")
                self._bind_launch_requests(item, observed)
            except CollectionError as error:
                # Bad chronology cannot retract separately proven ownership.
                errors.append(str(error))
        if entries:
            errors.append("Observed an invocation outside this case's launch journal")
        if errors:
            raise CollectionError("; ".join(errors))

    def _bind_launch_requests(self, item, events):
        for event in sorted(events, key=lambda row: (row["time"], row["request"], row["sequence"])):
            if event["request"] in self.request_roles:
                continue
            # Ledger publication may lag a later retry. Once a request has
            # been bound to a launch, never silently relabel an earlier stream
            # as a retry on another refresh.
            prior_entries = [
                entry
                for entry in events
                if entry["phase"] == "WRAPPER_ENTER" and entry["request"] in item["native_requests"]
            ]
            if prior_entries and event["time"] <= max(entry["time"] for entry in prior_entries):
                raise CollectionError("Native invocation chronology changed after launch binding")
            ordinal = len(item["native_requests"])
            role = item["role"]
            if ordinal and item["resume_roles"]:
                role = item["resume_roles"][min(ordinal - 1, len(item["resume_roles"]) - 1)]
            launch_id = item["launch_id"] if ordinal == 0 else f"{item['launch_id']}:native:{event['request']}"
            if ordinal == 0:
                self.case_context.update_attempt(
                    launch_id,
                    requestId=event["request"],
                    executionArn=event["execution"],
                )
            else:
                self.case_context.record_attempt(
                    launch_id,
                    role=role,
                    requestId=event["request"],
                    executionArn=event["execution"],
                    source="service-observed",
                )
            item["native_requests"].append(event["request"])
            self.request_roles[event["request"]] = role

    def verify(self):
        if self.manifest.get("concurrencies") != CONCURRENCIES or set(self.manifest["functions"]) != set(CONCURRENCIES):
            raise ProvisioningError("One deployment must contain both c1 and c2 functions")
        keys = (self.configuration,) if self.configuration is not None else self.manifest["functions"]
        for key in keys:
            arn = self.manifest["functions"][key]
            config = self.lam.get_function_configuration(FunctionName=arn)
            scaling = self.lam.get_function_scaling_config(FunctionName=arn.rsplit(":", 1)[0], Qualifier=QUALIFIER)
            self.verify_profile(config, scaling, self.manifest, key)

    @property
    def concurrency(self):
        return self.manifest["concurrencies"][self.configuration]

    def start(
        self,
        scenario,
        gate=None,
        target_environment=None,
        *,
        preparation_gate=None,
        role=None,
        resume_roles=(),
        dispatcher_payload=None,
        marker=None,
    ):
        self.check_active()
        if self.configuration not in self.manifest["functions"]:
            raise ProvisioningError("Select a deployed concurrency configuration before Invoke")
        marker = marker or scenario + "-" + uuid.uuid4().hex[:12]
        if not isinstance(marker, str) or "/" in marker or "\\" in marker or marker in {".", ".."}:
            raise CollectionError("Invocation marker must be a single path segment")
        # Markers can exceed Lambda's 64-character execution-name limit. Keep
        # their run/case scope while deriving a stable, bounded transport name.
        identity = [
            self.manifest["run"],
            self.configuration,
            self.case_context.scenario.case_id if self.case_context is not None else None,
            marker,
        ]
        execution_name = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
        payload = dict(dispatcher_payload or {})
        if dispatcher_payload:
            payload["sharedScenario"] = dispatcher_payload["scenario"]
        payload.update(scenario=scenario, marker=marker, run=self.manifest["run"])
        if self.case_context is not None:
            payload["coverageProfile"] = self.case_context.scenario.coverage_profile
            payload["behaviorBudgets"] = dict(self.case_context.scenario.behavior_budgets)
            payload["controlPrefix"] = self.control_prefix
            if role not in self.case_context.scenario.roles:
                raise ValueError("Launch role is not part of the admitted scenario")
            if any(resume not in self.case_context.scenario.roles for resume in resume_roles):
                raise ValueError("Resume role is not part of the admitted scenario")
        if target_environment is not None:
            payload["target_environment"] = target_environment
        if gate:
            payload["gate"] = gate
        if preparation_gate:
            payload["preparation_gate"] = preparation_gate
        function = self.manifest["functions"][self.configuration]
        relative = f"invocations/{marker}.json"
        journal = {
            "payload": payload,
            "function": function,
            "launched": time.time(),
            "durableExecutionName": execution_name,
            "dispatchState": "PREPARED",
        }

        def before_invoke():
            context = self.case_context
            recorded = False
            try:
                if context is not None:
                    context.record_attempt(
                        marker,
                        role=role,
                        marker=marker,
                        functionArn=function,
                        durableExecutionName=execution_name,
                        source="driver-invoke",
                    )
                    recorded = True
                self._save(relative, {**journal, "dispatchState": "SUBMITTED"})
            except BaseException as error:
                if context is not None and recorded:
                    try:
                        context.withdraw_attempt(marker)
                    except BaseException as withdraw_error:
                        # withdraw_attempt restores its own in-memory removal if the
                        # durable sink rejects it, so the attempt stays journaled
                        # either way; fail closed on the original error and note
                        # the secondary failure rather than swallowing it.
                        error.add_note(f"Failed to withdraw the undispatched attempt: {withdraw_error}")
                raise
            journal["dispatchState"] = "SUBMITTED"
            self.invocations.append(item)

        runner = DurableFunctionRunner(
            function,
            region=self.manifest["region"],
            lambda_client=RunnerClient(self, before_call=self.check_active, before_invoke=before_invoke),
        )
        item = {
            "marker": marker,
            "arn": None,
            "configuration": self.configuration,
            "runner": runner,
            "started": journal["launched"],
            "payload": payload,
            "role": role,
            "resume_roles": tuple(resume_roles),
            "native_requests": [],
            "launch_id": marker,
            "execution_name": execution_name,
        }
        try:
            self._save(relative, journal, exclusive=True)
        except FileExistsError as error:
            raise CollectionError(f"Invocation marker already has a journal: {marker}") from error
        if self.case_context is not None and "release-all" not in self.gates:
            self.hold("release-all")
        if gate:
            self.hold(gate)
        if preparation_gate:
            self.hold(preparation_gate)
        try:
            arn = runner.run_async(input=payload, execution_name=execution_name)
            require_execution_name(arn, function, execution_name)
        except Exception as error:
            if item in self.invocations and isinstance(error, ClientError):
                rejection = {
                    "dispatchState": "REJECTED",
                    "retryAttempts": error.response.get("ResponseMetadata", {}).get("RetryAttempts"),
                    "rejectionCode": error.response.get("Error", {}).get("Code"),
                }
                if invoke_rejection_proven(rejection):
                    journal.update(rejection)
                    self.invocations.remove(item)
                    self.rejected_markers.add(marker)
                    if self.case_context is not None:
                        self.case_context.update_attempt(marker, **rejection)
                elif rejection["rejectionCode"] == "DurableExecutionAlreadyStartedException":
                    journal.update({**rejection, "dispatchState": "CONFLICT"})
                    item["launch_conflict"] = True
            self._save(relative, {**journal, "error": str(error)})
            raise CollectionError(f"{marker}: invocation API failed") from error
        item["arn"] = arn
        item["invoke_acknowledged"] = True
        print(f"LMI {scenario}: marker={marker}, execution={arn}", flush=True)
        self._save(relative, {**journal, "arn": arn})
        return item

    def hold(self, gate):
        self.check_active()
        if gate in self.gates:
            for record in self.control_records:
                if record["gate"] == gate and record["action"] == "hold" and record.get("state") == "PENDING":
                    self._confirm_hold(gate)
            return  # another invocation in this case uses the same barrier
        intent = {"gate": gate, "action": "hold", "state": "PENDING", "started": time.time()}
        self.control_records.append(intent)
        self._save("control-records.json", self.control_records)
        self.gates.add(gate)
        try:
            response = self.s3.put_object(
                Bucket=self.manifest["bucket"],
                Key=f"{self.control_prefix}{gate}",
                Body=b"hold",
                IfNoneMatch="*",
            )
        except (BotoCoreError, ClientError) as error:
            if definitive_s3_write_rejection(error):
                intent.update(state="REJECTED", time=time.time())
                self.gates.remove(gate)
                self._save("control-records.json", self.control_records)
                raise
            self._confirm_hold(gate)
        else:
            retries = response.get("ResponseMetadata", {}).get("RetryAttempts") if isinstance(response, dict) else None
            if type(retries) is int and retries == 0:
                intent.update(time=time.time(), state="ACKNOWLEDGED")
                self._save("control-records.json", self.control_records)

    def _confirm_hold(self, gate):
        result = self.s3.get_object(Bucket=self.manifest["bucket"], Key=f"{self.control_prefix}{gate}")
        with result["Body"] as stream:
            if stream.read() != b"hold":
                raise CollectionError(f"Cannot prove control {gate} is held")
        # Seeing a hold permits the fixture to proceed, but cannot settle a
        # delayed conditional write. Keep its intent pending through cleanup.

    def release(self, gate):
        self.check_active()
        if gate in self.released_gates:
            return
        if self.case_context is not None and gate not in self.gates:
            raise CollectionError("Refusing to release a control not owned by this case")
        intent = {
            "gate": gate,
            "action": "release",
            "started": time.time(),
            "state": "PENDING",
        }
        self.control_records.append(intent)
        self._save("control-records.json", self.control_records)
        pending_hold = any(
            record["action"] == "hold" and record["gate"] == gate and record.get("state") in {"PENDING", "SUPERSEDED"}
            for record in self.control_records
        )
        release_control(self.s3, self.manifest["bucket"], f"{self.control_prefix}{gate}", allow_absent=pending_hold)
        intent.update(time=time.time(), state="ACKNOWLEDGED")
        self.released_gates.add(gate)
        self._save("control-records.json", self.control_records)

    def refresh(self, markers=None, *, validate_controls=True):
        self.check_active()
        if self.case_context is not None and markers == []:
            raise CollectionError("A shared case cannot collect another case's run prefix")
        # A new pytest fixture must not scan every earlier case before it can
        # observe its first event. Explicit [] requests the complete run for collection.
        active_markers = [item["marker"] for item in self.invocations]
        owned_markers = {*active_markers, *self.rejected_markers}
        if markers is None:
            markers = [*active_markers, *sorted(self.rejected_markers)]
        if self.case_context is not None:
            if not markers:
                return []
            if not set(markers) <= owned_markers:
                raise CollectionError("A shared case cannot read another case's marker")
        root = f"runs/{self.manifest['run']}/events/"
        prefixes = {f"{root}{marker}/": marker for marker in markers} if markers else {root: ""}
        in_scope_markers = set(prefixes.values())
        missing: dict[str, str] = {}
        failures: list[tuple[str, Exception]] = []
        for prefix, marker in prefixes.items():
            try:
                transport = self._read_transport("s3", self.s3)
                pages = iter(
                    transport.get_paginator("list_objects_v2").paginate(Bucket=self.manifest["bucket"], Prefix=prefix)
                )
                while True:
                    self._check_read_budget(transport)
                    page = next(pages, None)
                    if page is None:
                        break
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        if not key.startswith(prefix):
                            raise CollectionError("Ledger listing escaped the requested prefix")
                        if key not in self.events:
                            missing[key] = marker
                    if page.get("IsTruncated") is False:
                        break
            except Exception as error:
                failures.append((marker, error))

        if missing:
            # Any newly discovered record can end up cached (self.events)
            # before a later failure, of any kind, stops this refresh short
            # of a complete bind. Defer as soon as there is something new to
            # bind rather than only on the specific budget-expiry branch
            # below; the deferral is cleared only once a complete,
            # failure-free bind actually runs.
            self.binding_deferred = True

        def read(key):
            self._check_read_budget(read_transport)
            response = read_transport.get_object(Bucket=self.manifest["bucket"], Key=key)
            with response["Body"] as stream:
                event = json.loads(stream.read())
            if not isinstance(event, dict):
                raise CollectionError("Ledger record is not an object")
            if event.get("run") != self.manifest["run"] or event.get("commit") != self.manifest["commit"]:
                raise CollectionError("Event came from another build/run")
            if missing[key] and event.get("marker") != missing[key]:
                raise CollectionError("Ledger record does not match its launch prefix")
            if any(
                not isinstance(event.get(field), str) or not event[field] for field in ("request", "phase", "marker")
            ):
                raise CollectionError("Ledger record has malformed request, phase or launch marker")
            if any(
                type(event.get(field)) not in {int, float} or not math.isfinite(event[field])
                for field in ("time", "sequence")
            ):
                raise CollectionError("Ledger record has malformed timing or sequence")
            self.check_active()
            return event

        if missing:
            keys = list(missing)
            with ThreadPoolExecutor(max_workers=8) as readers:
                for start in range(0, len(keys), 8):
                    try:
                        read_transport = self._read_transport("s3", self.s3)
                    except CollectionBudgetExpired as error:
                        # Every marker with an unread key is incomplete, so
                        # binding cannot treat its partial read as complete.
                        failures.extend(
                            (marker, error) for marker in dict.fromkeys(missing[key] for key in keys[start:])
                        )
                        break
                    pending = {key: readers.submit(read, key) for key in keys[start : start + 8]}
                    for key, future in pending.items():
                        try:
                            self.events[key] = future.result()
                        except Exception as error:
                            failures.append((missing[key], error))
        # A marker is incomplete for binding purposes whenever this call's own
        # listing or any of its reads failed or went unread (including the
        # budget-expiry attribution above and the all-budget early raise
        # below). It is only cleared once its own listing and every one of
        # its reads succeed in a call that actually covers it; a marker left
        # out of this call's scope keeps whatever status it already had.
        failed_markers = {marker for marker, _ in failures} & in_scope_markers
        self._incomplete_markers |= failed_markers
        self._incomplete_markers -= in_scope_markers - failed_markers
        events = list(self.events.values())
        budget_errors = [error for _, error in failures if isinstance(error, CollectionBudgetExpired)]
        if budget_errors and all(isinstance(error, CollectionBudgetExpired) for _, error in failures):
            self.source_uris.update(f"s3://{self.manifest['bucket']}/{key}" for key in self.events)
            self._save("events.json", events)
            raise budget_errors[0]
        self.check_active()
        try:
            # Bind over the whole event cache, but always skip every marker
            # this or any earlier call left incomplete: a cached retry for a
            # still-incomplete marker must never be bound as its original
            # request just because an unrelated marker's refresh succeeded.
            self._bind_requests(events, self._incomplete_markers)
            if not self._incomplete_markers:
                self.binding_deferred = False
        except CollectionError as error:
            failures.append(("", error))
        if failures:
            text = "; ".join(f"{marker}: {error}" if marker else str(error) for marker, error in failures)
            if all(retryable_read_error(error) for _, error in failures):
                raise TransientCollectionError(text)
            raise CollectionError(text)
        events.sort(key=lambda e: (e["time"], e["request"], e["sequence"]))
        self.source_uris.update(f"s3://{self.manifest['bucket']}/{key}" for key in self.events)
        self._save("events.json", events)
        selected = [e for e in events if not markers or e["marker"] in markers]
        if markers and validate_controls:
            check_controls(selected)
        return selected

    def for_item(self, item):
        return self.refresh(markers=[item["marker"]])

    def poll(
        self,
        predicate,
        seconds=30,
        category=CollectionError,
        message="Evidence deadline exceeded",
    ):
        end = time.monotonic() + seconds
        last_transient: Exception | None = None
        while True:
            self.check_active()
            try:
                value = predicate()
            except Exception as error:
                if not isinstance(error, TransientCollectionError) and not retryable_read_error(error):
                    raise
                last_transient = error
            else:
                if value:
                    return value
            if time.monotonic() >= end:
                raise category(message) from last_transient
            self.wait(min(0.5, max(0, end - time.monotonic())))

    def phase(self, item, phase, **kwargs):
        return self.poll(lambda: select(self.for_item(item), phase), **kwargs)

    def finish(self, item, status="SUCCEEDED"):
        try:
            result = item["runner"].wait_for_result(item["arn"], timeout=self.manifest["driverTimeout"])
        except TimeoutError as error:
            raise CollectionError("Driver timeout, not Lambda invocation timeout evidence") from error
        self.execution_results[item["arn"]] = result
        self._save(f"executions/{item['marker']}.json", result)
        if self.case_context is None:
            assert result["Status"] == status, f"{item['marker']}: {result['Status']} (expected {status})"
            if status == "SUCCEEDED":
                assert json.loads(result["Result"]) == item["marker"], "Result crossed execution boundaries"
            self.phase(item, "WRAPPER_RETURN")
        else:
            # Outcome verification happens later, once collection builds evidence from this wrapper event.
            self.poll(
                lambda: any(event["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"} for event in self.for_item(item))
            )
        return self.poll(
            lambda: self.history(item),
            seconds=self.manifest["driverTimeout"],
            message="Final execution history did not become available",
        )

    def history_page(self, **request):
        """Serialize paced reads within one case, without shared-case throttling state."""
        with self._history_lock:
            return self._history_page(**request)

    def _history_page(self, **request):
        """Bound read-only throttling retries without replaying invocation writes."""
        deadline = time.monotonic() + 20
        last_error = None
        for attempt in range(5):
            self.check_active()
            delay = max(0, self._next_history_read - time.monotonic())
            if time.monotonic() + delay >= deadline:
                break
            if delay:
                self.wait(delay)
            transport = self._read_transport("lambda", self.lam)
            try:
                return transport.get_durable_execution_history(**request)
            except Exception as error:
                if not retryable_read_error(error):
                    raise
                last_error = error
            finally:
                self._next_history_read = time.monotonic() + 1
            self._next_history_read = time.monotonic() + min(2**attempt, 4)
        raise TransientCollectionError("Execution-history API throttle budget exhausted") from last_error

    def history(self, item):
        arn = item["arn"]
        retained = self.raw_histories.setdefault(arn, [])
        identified: dict[int, dict[str, Any]] = {}
        for event in retained:
            identity = event.get("EventId")
            if type(identity) is int:
                if identity in identified and identified[identity] != event:
                    raise CollectionError(f"Conflicting service history for event {identity}")
                identified[identity] = event
        marker = None
        while True:
            self.check_active()
            request = {"DurableExecutionArn": arn, "IncludeExecutionData": True}
            if marker:
                request["Marker"] = marker
            result = self.history_page(**request)
            for event in result.get("Events", []):
                identity = event.get("EventId")
                if type(identity) is int:
                    if identity in identified:
                        if identified[identity] != event:
                            retained.append(event)
                            self._save(f"histories/{item['marker']}.json", retained)
                            raise CollectionError(f"Conflicting service history for event {identity}")
                        continue
                    identified[identity] = event
                elif event in retained:
                    continue
                retained.append(event)
            marker = result.get("NextMarker")
            if not marker:
                break
        events = sorted(retained, key=lambda event: event["EventId"] if type(event.get("EventId")) is int else math.inf)
        self.raw_histories[arn] = events
        self._save(f"histories/{item['marker']}.json", events)
        if self.case_context is not None:
            intervals = service_native_intervals(events)
            owned = set(item.get("native_requests", ()))
            for attempt in self.case_context.attempts:
                if (
                    attempt.get("requestId") in owned
                    and attempt.get("executionArn") == item["arn"]
                    and (interval := intervals.get(attempt["requestId"])) is not None
                    and (attempt.get("nativeStartedAtMs"), attempt.get("nativeEndedAtMs")) != interval
                ):
                    start, end = interval
                    self.case_context.update_attempt(attempt["attemptId"], nativeStartedAtMs=start, nativeEndedAtMs=end)
        return events

    def platform_timeout(self, item, request):
        # A TIMED_OUT logical execution or a client poll limit is insufficient.
        # Require a real invocation-completed service event for this request.
        def observed():
            for event in self.history(item):
                details = event.get("InvocationCompletedDetails", {})
                if details.get("RequestId") == request and is_platform_timeout(details):
                    return event
            return None

        return self.poll(
            observed,
            seconds=self.manifest["invocationTimeout"] + 40,
            message="No service invocation timeout evidence for " + request,
        )

    def collect(self, *, full_run=True, shared_output: Path | None = None):
        self.check_active()
        if self.case_context is not None and full_run:
            raise CollectionError("A shared case may collect only its own invocations")
        errors: list[Exception] = []
        self.terminal_histories.clear()
        if not full_run and not (self.invocations or self.rejected_markers):
            return
        items = {}
        ledger_identities: dict[str, dict[str, Any]] = {}
        conflicting: set[str] = set()
        captured: set[str] = set()
        events = []
        try:
            events = self.refresh(
                markers=[]
                if full_run
                else [*(item["marker"] for item in self.invocations), *sorted(self.rejected_markers)],
                validate_controls=False,
            )
            if full_run:
                targets = tuple(arn + "/durable-execution/" for arn in self.manifest["functions"].values())
                # Collection without journals is read-only, and artifact names
                # do not come from the fixture's marker.
                for event in events:
                    arn = event["execution"]
                    if not arn.startswith(targets):
                        errors.append(CollectionError("Observed an execution outside this deployment"))
                        continue
                    identity = {
                        "run": self.manifest["run"],
                        "marker": event.get("marker"),
                        "caseId": event.get("caseId"),
                    }
                    if not isinstance(identity["marker"], str) or not identity["marker"]:
                        errors.append(CollectionError("Observed execution has no fixture marker"))
                        continue
                    known = ledger_identities.setdefault(arn, identity)
                    if known != identity:
                        conflicting.add(arn)
                        errors.append(CollectionError("Observed execution has conflicting fixture identities"))
                    items[arn] = {"arn": arn, "marker": hashlib.sha256(arn.encode()).hexdigest()}
        except CollectionBudgetExpired:
            raise
        except Exception as error:
            errors.append(error)
        if full_run:
            journals, journal_errors = self.run_invocations(include_shared=True, shared_output=shared_output)
            errors.extend(CollectionError(error) for error in journal_errors)
            for arn in conflicting:
                items.pop(arn, None)
            verified_journals = {item["arn"]: item for item in journals}
            verified = set(verified_journals)
            for item in journals:
                arn = item["arn"]
                items[arn] = {"arn": arn, "marker": hashlib.sha256(arn.encode()).hexdigest()}
        if not full_run:
            items = {i["arn"]: i for i in self.invocations if i.get("arn")}
        for item in items.values():
            try:
                transport = self._read_transport("lambda", self.lam)
                kwargs = {"DurableExecutionArn": item["arn"]}
                ledger_only = full_run and item["arn"] not in verified
                if ledger_only:
                    kwargs["IncludeExecutionData"] = True
                result = transport.get_durable_execution(**kwargs)
                if ledger_only:
                    payload = json.loads(result.get("InputPayload", "null"))
                    if not isinstance(payload, dict):
                        raise CollectionError("Service did not return the fixture execution input")
                    require_ledger_launch(ledger_identities[item["arn"]], payload)
                self.execution_results[item["arn"]] = result
                self._save(f"executions/{item['marker']}.json", result)
                self.history(item)
                captured.add(item["arn"])
                if result.get("Status") in {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}:
                    self.terminal_histories.add(item["arn"])
            except CollectionBudgetExpired:
                raise
            except Exception as error:
                errors.append(error)
        if full_run:
            for key, target in self.manifest["functions"].items():
                try:
                    owned = [
                        history
                        for arn, history in self.raw_histories.items()
                        if arn in captured and arn.startswith(target + "/durable-execution/")
                    ]
                    ledger = [
                        event
                        for event in events
                        if (journal := verified_journals.get(event.get("execution"))) is not None
                        and event["execution"].startswith(target + "/durable-execution/")
                        and event.get("marker") == journal["marker"]
                        and event.get("caseId") == journal.get("caseId")
                    ]
                    self._collect_logs(key, target, owned, ledger)
                except Exception as error:
                    errors.append(error)
        if errors:
            messages = [str(error) for error in errors]
            self._save("collection-errors.json", messages)
            if all(isinstance(error, TransientCollectionError) or retryable_read_error(error) for error in errors):
                raise TransientCollectionError("; ".join(messages))
            raise CollectionError("; ".join(messages))

    def _collect_logs(self, key, target, histories, ledger):
        windows = {}
        for history in histories:
            for row in history:
                details = row.get("InvocationCompletedDetails", {})
                request = details.get("RequestId")
                start, end = details.get("StartTimestamp"), details.get("EndTimestamp")
                if isinstance(request, str) and isinstance(start, datetime) and isinstance(end, datetime):
                    # Platform start/report can straddle the service timestamps.
                    windows[request] = (int(start.timestamp() * 1000) - 2000, int(end.timestamp() * 1000) + 2000)
        ledger_windows: dict[str, tuple[int, int]] = {}
        for event in ledger:
            request, timestamp = event.get("request"), event.get("time")
            if (
                not isinstance(request, str)
                or not isinstance(timestamp, (int, float))
                or not math.isfinite(timestamp)
                or timestamp < 0
            ):
                continue
            moment = int(timestamp * 1000)
            lower, upper = ledger_windows.get(request, (moment, moment))
            ledger_windows[request] = (min(lower, moment), max(upper, moment))
        for request, (lower, upper) in ledger_windows.items():
            windows.setdefault(request, (lower - 2000, upper + 2000))
        if not windows:
            return
        start = max(0, min(window[0] for window in windows.values()))
        end = max(window[1] for window in windows.values())
        config = self.lam.get_function_configuration(FunctionName=target)
        logs = []
        for page in self.logs.get_paginator("filter_log_events").paginate(
            logGroupName=config["LoggingConfig"]["LogGroup"], startTime=start, endTime=end + 1
        ):
            for event in page.get("events", []):
                if not start <= event["timestamp"] <= end:
                    continue
                try:
                    message = json.loads(event["message"])
                except TypeError, ValueError:
                    continue
                if not isinstance(message, dict):
                    continue
                record = message.get("record")
                request = record.get("requestId") if isinstance(record, dict) else message.get("requestId")
                if isinstance(request, str) and request in windows:
                    lower, upper = windows[request]
                    if lower <= event["timestamp"] <= upper:
                        logs.append(event)
        self._save(f"logs/{key}.json", logs)

    def release_all(self):
        errors = []
        for gate in sorted(self.gates - self.released_gates, key=lambda value: value != "release-all"):
            try:
                self.release(gate)
            except Exception as error:
                errors.append(f"{gate}: {error}")
        self.gates.difference_update(self.released_gates)
        if errors:
            raise CollectionError("; ".join(errors))

    def delete_controls(self):
        """Delete owned barriers only after their runtime users are quiescent."""
        gates = {
            record["gate"]
            for record in self.control_records
            if record["action"] == "hold" and record.get("state") != "REJECTED"
        }
        deleted = {
            record["gate"]
            for record in self.control_records
            if record["action"] == "delete" and record.get("state") == "ACKNOWLEDGED"
        }
        ambiguous = {
            record["gate"]
            for record in self.control_records
            if record["action"] == "hold" and record.get("state") in {"PENDING", "SUPERSEDED"}
        }
        for gate in sorted(gates - deleted - ambiguous):
            self.check_active()
            if gate not in self.released_gates:
                raise CollectionError("Cannot delete an unreleased case control")
            intent = {"gate": gate, "action": "delete", "started": time.time(), "state": "PENDING"}
            self.control_records.append(intent)
            self._save("control-records.json", self.control_records)
            request = {"Bucket": self.manifest["bucket"], "Key": f"{self.control_prefix}{gate}"}
            self.s3.delete_object(**request)
            try:
                self.s3.head_object(**request)
            except ClientError as error:
                if error.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}:
                    raise CollectionError("Cannot verify owned control deletion") from error
            else:
                raise CollectionError("Deleted case control is still present")
            intent.update(time=time.time(), state="ACKNOWLEDGED")
            self._save("control-records.json", self.control_records)
        if ambiguous:
            raise CollectionError(
                "Retained release controls for unresolved hold writes: " + ", ".join(sorted(ambiguous))
            )

    def run_invocations(self, *, include_shared=False, shared_output: Path | None = None):
        """Recover each run-owned execution independently of other journals."""
        targets = self.manifest["functions"].values()
        items = []
        errors = []
        paths = list((self.artifacts_dir / "invocations").glob("*.json"))
        if include_shared:
            output = shared_output or case_output_directory(self.artifacts_dir, self.manifest["run"])
            try:
                plans = saved_case_plans(output, self.manifest["run"])
            except (OSError, ValueError) as error:
                plans = []
                errors.append(str(error))
            if not plans and not errors:
                plans = [(output / coverage, {}) for coverage in COVERAGE_PROFILES]
            for coverage_dir, plan in plans:
                try:
                    root = sdk_artifact_directory(coverage_dir, plan) / "cases"
                    paths.extend(root.glob("*/invocations/*.json"))
                except (OSError, ValueError) as error:
                    errors.append(f"{coverage_dir}: {error}")
        for path in paths:
            try:
                record = json.loads(path.read_text())
                payload = record["payload"]
                if payload["run"] != self.manifest["run"]:
                    continue
                marker = payload["marker"]
                target = record["function"]
                if target not in targets:
                    raise CollectionError("Refusing to stop an execution outside this deployment")
                name = record["durableExecutionName"]
                prepared = record.get("dispatchState") == "PREPARED" and not any(
                    key in record for key in ("arn", "executionArn", "requestId")
                )
                if prepared or invoke_rejection_proven(record):
                    if path.name != f"{marker}.json" or not isinstance(name, str) or not name:
                        raise CollectionError("Undispatched invocation journal has inconsistent identity")
                    continue
                if record.get("dispatchState") == "CONFLICT":
                    raise CollectionError(f"Invoke name conflicts with another launch: {marker}")
                arn = record.get("arn")
                if not arn:
                    recovered = set()
                    try:
                        for event in self.refresh(markers=[marker], validate_controls=False):
                            if event["marker"] != marker or event["phase"] != "WRAPPER_ENTER":
                                continue
                            require_ledger_launch(event, payload)
                            require_execution_name(event["execution"], target, name)
                            recovered.add(event["execution"])
                    except Exception as error:
                        recovered.clear()
                        errors.append(f"{path.name}: {error}")
                    if len(recovered) > 1:
                        raise CollectionError(f"Conflicting recovered executions: {marker}")
                    arn = next(iter(recovered), None) or find_execution_by_name(self.lam, target, name, payload)
                    if arn is None:
                        raise CollectionError(f"Unreconciled Invoke: {marker}")
                require_execution_name(arn, target, name)
                item = {"arn": arn, "marker": marker}
                if include_shared:
                    item["caseId"] = payload.get("caseId")
                items.append(item)
            except Exception as error:
                errors.append(f"{path.name}: {error}")
        return items, errors

    def stop_running_execution(self, arn: str) -> bool:
        """Stop an execution, or confirm that completion won the race."""
        transport = self._read_transport("lambda", self.lam)
        try:
            transport.stop_durable_execution(DurableExecutionArn=arn)
        except ClientError as error:
            if error.response["Error"]["Code"] != "InvalidParameterValueException":
                raise
            transport = self._read_transport("lambda", self.lam)
            current = transport.get_durable_execution(DurableExecutionArn=arn)["Status"]
            if current == "PAUSING":
                raise TransientCollectionError(f"Execution is still pausing: {arn}") from error
            if current not in {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}:
                raise
            return False
        return True

    def stop_victim_after_cutoff(self, item: dict[str, Any]) -> None:
        """Retain the pre-stop result and request a bounded observation stop."""
        self.check_active()
        arn = item.get("arn")
        if item.get("role") != "victim" or not any(item is candidate for candidate in self.invocations):
            raise CollectionError("Observation stop requires the admitted victim")
        if not isinstance(arn, str) or not arn:
            raise CollectionError("Observation stop requires a verified execution ARN")
        self._require_execution_target(arn)
        witness = self.cutoff_results.setdefault(arn, {})
        if witness.get("collectionError") or witness.get("stopResolved"):
            return
        try:
            transport = self._read_transport("lambda", self.lam)
            current = transport.get_durable_execution(DurableExecutionArn=arn)["Status"]
            if "Status" not in witness:
                witness.update(Status=current, observedAtMs=time.time() * 1000)
                self._save("cutoff-executions.json", self.cutoff_results)
            if current == "PAUSING":
                raise TransientCollectionError(f"Victim execution is still pausing: {arn}")
            if current in {"RUNNING", "PAUSED"}:
                self.stop_running_execution(arn)
            elif current not in {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}:
                raise CollectionError(f"Victim execution has unsupported stop status: {current}")
            witness["stopResolved"] = True
        except (BotoCoreError, ClientError) as error:
            if retryable_read_error(error):
                raise TransientCollectionError(f"Could not reconcile observation stop for {arn}: {error}") from error
            witness["collectionError"] = str(error)
        self._save("cutoff-executions.json", self.cutoff_results)

    def settle_case(self):
        """Release fault work and drain this case before reusing its function."""
        errors = []
        try:
            self.release_all()
        except Exception as error:
            errors.append(error)
        owned: set[str] = set()
        unconfirmed: set[str] = set()
        stopped: set[str] = set()
        terminal: set[str] = set()
        ledger_error: Exception | None = None
        read_errors: dict[str, Exception] = {}
        invalid_ledger = False

        def reconcile():
            nonlocal ledger_error, invalid_ledger
            if self.case_context is not None:
                try:
                    self.refresh(validate_controls=False)
                except TransientCollectionError as error:
                    ledger_error = error
                except CollectionError as error:
                    ledger_error = None
                    if not invalid_ledger:
                        errors.append(error)
                    invalid_ledger = True
                else:
                    ledger_error = None
                for launch in self.invocations:
                    name = launch.get("execution_name")
                    if launch.get("arn") or launch.get("launch_conflict") or not name:
                        continue
                    try:
                        arn = find_execution_by_name(
                            self.lam, self.manifest["functions"][self.configuration], name, launch["payload"]
                        )
                    except (BotoCoreError, ClientError, CollectionError) as error:
                        if retryable_read_error(error):
                            ledger_error = error
                        elif str(error) not in {str(item) for item in errors}:
                            errors.append(error)
                    else:
                        if arn is not None:
                            launch.update(arn=arn, service_identity_verified=True)
            current = {item["arn"] for item in self.invocations if item.get("arn")}
            unconfirmed.intersection_update(current)
            for arn in read_errors.keys() - current:
                del read_errors[arn]
            unconfirmed.update(current - terminal - owned)
            owned.clear()
            owned.update(current)

        def drain():
            for arn in sorted(unconfirmed):
                read_errors.pop(arn, None)
                try:
                    status = self.lam.get_durable_execution(DurableExecutionArn=arn)["Status"]
                    if status == "PAUSING":
                        continue
                    if status in {"RUNNING", "PAUSED"}:
                        if arn in stopped:
                            continue
                        if self.stop_running_execution(arn):
                            stopped.add(arn)
                            continue
                except Exception as error:
                    if isinstance(error, TransientCollectionError) or retryable_read_error(error):
                        read_errors[arn] = error
                        continue
                    errors.append(error)
                terminal.add(arn)
                unconfirmed.remove(arn)

        def settled():
            reconcile()
            drain()
            unresolved = self.case_context is not None and any(not item.get("arn") for item in self.invocations)
            if unconfirmed or unresolved or ledger_error is not None:
                return False
            if invalid_ledger:
                return True  # Still report the error after draining owned work.
            events = self.refresh(validate_controls=False) if self.case_context is None else self.events.values()
            if self.case_context is not None:
                events = [event for event in events if event["execution"] in owned]
            entered = {e["request"] for e in select(events, "WRAPPER_ENTER")}
            exited = {e["request"] for e in events if e["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}}
            return entered <= exited

        reconcile()
        if self.invocations or self.rejected_markers:
            drain()
            try:
                self.poll(
                    settled,
                    seconds=self.manifest.get("driverTimeout", 30),
                    message="Released case still owns work or has an unresolved Invoke; function is not ready for reuse",
                )
            except Exception as error:
                if ledger_error is not None:
                    error.add_note(f"Last transient ledger read: {ledger_error}")
                for arn, read_error in read_errors.items():
                    error.add_note(f"Last transient execution read for {arn}: {read_error}")
                errors.append(error)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise CollectionError("; ".join(str(error) for error in errors))
