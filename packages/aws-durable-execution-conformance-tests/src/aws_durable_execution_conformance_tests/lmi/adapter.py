# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The shared AWS driver's bridge to the LMI scenario coordinator, which trusts what it reports."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol

from aws_durable_execution_conformance_tests.lmi.artifacts import case_key, owner_lock, save
from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.configuration import configuration_identity
from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    ScenarioContext,
    _await_without_cancelling,
)
from aws_durable_execution_conformance_tests.lmi.errors import (
    CollectionBudgetExpired,
    CollectionError,
    ProvisioningError,
)
from aws_durable_execution_conformance_tests.lmi.runtime import FixtureBinding, RuntimeDefinition
from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver
from aws_durable_execution_conformance_tests.lmi.source_evidence import binding_evidence_builder

# "marker": UTF-8 marker bytes (today's str.encode). "json-marker": the marker
# string JSON-encoded before being sent as the callback payload.
_CALLBACK_ENCODERS: dict[str, Callable[[str], bytes]] = {
    "marker": str.encode,
    "json-marker": lambda marker: json.dumps(marker).encode(),
}


def _binding_driver_factory(binding: FixtureBinding, runtime: str) -> Callable[..., ScenarioDriver]:
    """Build a ``driver_factory(context, cloud, **options)`` driven by a FixtureBinding."""

    def factory(context: ScenarioContext, cloud: Cloud, **options: Any) -> ScenarioDriver:
        options.setdefault("callback_encoder", _CALLBACK_ENCODERS[binding.callback_payload])
        options.setdefault("suspension_mode", binding.suspension)
        return ScenarioDriver(context, cloud, runtime=runtime, fixtures=dict(binding.fixtures), **options)

    return factory


def _only_undispatched_journals(cloud: Cloud, context: ScenarioContext) -> bool:
    try:
        for path in (cloud.artifacts_dir / "invocations").glob("*.json"):
            journal = json.loads(path.read_text())
            if not isinstance(journal, dict) or not isinstance(payload := journal.get("payload"), dict):
                return False
            marker = payload["marker"]
            if (
                journal.get("dispatchState") != "PREPARED"
                or any(field in journal for field in ("arn", "executionArn", "requestId"))
                or payload["run"] != context.run_id
                or payload["caseId"] != context.scenario.case_id
                or journal["function"] != context.profile.resource_id
                or not isinstance(marker, str)
                or not marker
                or path.name != f"{marker}.json"
                or not isinstance(name := journal["durableExecutionName"], str)
                or not name
            ):
                return False
    except (OSError, TypeError, KeyError, ValueError):
        return False
    return True


class EvidenceBuilder(Protocol):
    def __call__(self, context: ScenarioContext, cloud: Cloud, *, phase: str) -> dict[str, Any]: ...


class _CoordinatorLoopContext:
    """A ScenarioContext view for driver worker threads.

    The driver runs blocking service calls in worker threads, but a
    ScenarioContext and its journal sink belong to the coordinator's event
    loop. This view runs journal calls on that loop and waits for them, so a
    journal failure still stops the dispatch that follows.
    Every other attribute is the context's own.
    """

    _LOOP_METHODS = frozenset({"record_attempt", "record_service_attempt", "update_attempt", "withdraw_attempt"})

    def __init__(self, context: ScenarioContext, loop: asyncio.AbstractEventLoop) -> None:
        object.__setattr__(self, "_context", context)
        object.__setattr__(self, "_loop", loop)

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._context, name)
        if name not in self._LOOP_METHODS:
            return value

        def call(*args: Any, **kwargs: Any) -> Any:
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is self._loop:
                return value(*args, **kwargs)

            async def invoke() -> Any:
                return value(*args, **kwargs)

            return asyncio.run_coroutine_threadsafe(invoke(), self._loop).result()

        return call

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._context, name, value)


class _Case:
    def __init__(self, driver: ScenarioDriver) -> None:
        self.driver = driver
        self.worker: asyncio.Task[Any] | None = None

    async def call(self, operation: Callable[[], Any]) -> Any:
        if self.worker is not None and not self.worker.done():
            raise CollectionError("A previous case worker still owns blocking service calls")
        self.worker = asyncio.create_task(asyncio.to_thread(operation))
        try:
            # Keep the worker task alive on outer cancellation so retirement can
            # join the actual thread before issuing cleanup writes.
            return await _await_without_cancelling(self.worker)
        except asyncio.CancelledError:
            self.driver.cloud.cancel()
            raise

    async def prepare_retirement(self) -> None:
        if self.worker is not None:
            try:
                await _await_without_cancelling(self.worker)
            except Exception:
                # The coordinator retains the original phase error; this only
                # confirms its service-call worker has really stopped.
                pass
            finally:
                if self.worker.done():
                    self.worker = None
        self.driver.cloud.begin_retirement()


class SharedAdapter:
    """Each shared case has one Cloud, worker, control namespace and evidence sink.

    ``cloud_factory``, ``driver_factory`` and ``evidence_builder`` are internal test
    seams only: production code always builds them from the plan's own
    ``sdk.deployment`` runtime/fixture binding, never from an SDK-supplied override.
    """

    def __init__(
        self,
        manifest: Mapping[str, Any],
        *,
        evidence_builder: EvidenceBuilder | None = None,
        cloud_factory: Callable[..., Cloud] | None = None,
        driver_factory: Callable[..., ScenarioDriver] | None = None,
    ) -> None:
        self.manifest = deepcopy(dict(manifest))
        self.sdk = self.manifest["sdk"]
        self.deployment = self.sdk["deployment"]
        self.artifacts_dir = Path(self.sdk["artifacts_dir"])
        self.cloud_factory = cloud_factory or Cloud
        # Only derive the binding (and parse the deployment's runtime) when a
        # built-in default is actually needed; test seams that already supply
        # both factories never require a real, binding-carrying deployment.
        if evidence_builder is None or driver_factory is None:
            binding = RuntimeDefinition.from_manifest(self.deployment).binding
            runtime_name = self.manifest["runtime"]
            evidence_builder = evidence_builder or binding_evidence_builder(binding, runtime_name)
            driver_factory = driver_factory or _binding_driver_factory(binding, runtime_name)
        self.evidence_builder = evidence_builder
        self.driver_factory = driver_factory
        self.cases: dict[str, _Case] = {}
        self.prepared_clouds: dict[str, tuple[Cloud, str]] = {}
        self.preparation_workers: dict[str, asyncio.Task[tuple[Cloud, str]]] = {}
        # Cases proven to have done no work before `start` failed and deleted
        # them; `collect_partial` still gets called for these during cleanup.
        self.no_work_cases: set[str] = set()

    async def prepare(self, profile: Profile) -> None:
        def verify() -> tuple[Cloud, str]:
            if profile.resource_id not in self.deployment["functions"].values():
                raise ProvisioningError("Shared profile is not part of the owned SDK deployment")
            key = next(key for key, arn in self.deployment["functions"].items() if arn == profile.resource_id)
            if self.deployment["concurrencies"][key] != profile.native_concurrency:
                raise ProvisioningError("Native concurrency differs from the shared profile")
            if profile.configuration_id != configuration_identity(self.deployment, key):
                raise ProvisioningError("Shared configuration identity differs from the deployed fixture")
            cloud = self.cloud_factory(self.deployment, key, artifacts_dir=self.artifacts_dir / "configuration")
            from .quarantine import FenceRetryUncertain, acquire_fence, release_fence

            token = uuid.uuid4().hex
            journal = self.artifacts_dir / "configuration" / f"{case_key(profile.resource_id)}-fence.json"
            from .service import definitely_not_sent, definitive_s3_write_rejection

            with owner_lock(journal) as owner:
                if journal.exists():
                    raise ProvisioningError("Existing profile fence journal requires reconciliation")

                identity = {
                    "run": self.deployment["run"],
                    "commit": self.deployment["commit"],
                    "resourceId": profile.resource_id,
                    "token": token,
                    "singleAttempt": getattr(cloud, "single_attempt_fences", False) is True,
                    "owner": owner,
                }

                reserved = False

                def before_put() -> None:
                    nonlocal reserved
                    try:
                        save(journal, {**identity, "state": "ACQUIRING"}, exclusive=True)
                    except FileExistsError as error:
                        raise ProvisioningError("Existing profile fence journal requires reconciliation") from error
                    reserved = True

                try:
                    token = acquire_fence(
                        getattr(cloud, "fence_storage", cloud.s3),
                        self.deployment,
                        profile.resource_id,
                        token=token,
                        before_put=before_put,
                        single_attempt=identity["singleAttempt"],
                    )
                except FenceRetryUncertain:
                    if reserved:
                        save(journal, {**identity, "state": "ACQUIRING", "singleAttempt": False})
                    raise
                except Exception as error:
                    # A rejected or never-sent PUT created no fence, so the
                    # ACQUIRING journal would otherwise block recovery forever.
                    if reserved and (
                        definitive_s3_write_rejection(error, precondition=True)
                        or definitely_not_sent(error, single_attempt=identity["singleAttempt"])
                    ):
                        journal.unlink(missing_ok=True)
                    raise
                save(journal, {**identity, "token": token, "state": "ACQUIRED"})
                try:
                    cloud.verify()
                except Exception as error:
                    try:
                        save(journal, {**identity, "token": token, "state": "NOT_ADMITTED"})
                        release_fence(cloud.s3, self.deployment, profile.resource_id, token)
                        journal.unlink(missing_ok=True)
                    except Exception as cleanup_error:
                        raise ProvisioningError(
                            f"Profile verification failed: {error}. Could not release its fence: {cleanup_error}"
                        ) from error
                    raise
                return cloud, token

        resource = profile.resource_id
        if resource in self.preparation_workers:
            raise ProvisioningError("A profile preparation worker is still active")
        worker = asyncio.create_task(asyncio.to_thread(verify))
        self.preparation_workers[resource] = worker
        try:
            self.prepared_clouds[resource] = await _await_without_cancelling(worker)
        except asyncio.CancelledError:
            # Finish must still join the thread and recover a fence acquired
            # after the coordinator cancelled the provisioning phase.
            raise
        except Exception:
            del self.preparation_workers[resource]
            raise
        else:
            del self.preparation_workers[resource]

    def _case(self, context: ScenarioContext) -> _Case:
        try:
            return self.cases[context.scenario.case_id]
        except KeyError as error:
            raise CollectionError("Case was not initialized by the shared adapter") from error

    async def start(self, context: ScenarioContext) -> None:
        if context.scenario.case_id in self.cases:
            raise CollectionError("A later scenario cannot replace an admitted case")
        key = next(
            (key for key, arn in self.deployment["functions"].items() if arn == context.profile.resource_id),
            None,
        )
        if key is None:
            raise ProvisioningError("Case target is outside the owned deployment")
        owner = case_key(context.scenario.case_id)
        threaded = _CoordinatorLoopContext(context, asyncio.get_running_loop())
        try:
            cloud = await asyncio.to_thread(
                self.cloud_factory,
                self.deployment,
                key,
                artifacts_dir=self.artifacts_dir / "cases" / owner,
                case_context=threaded,
            )
            driver = self.driver_factory(threaded, cloud)
        except Exception:
            # Either Cloud itself failed to construct, or it did but the
            # driver failed before this case was ever registered. Either way
            # no case work (no service I/O, no driver.start) could have run
            # yet, so collect_partial must skip this case too.
            self.no_work_cases.add(context.scenario.case_id)
            raise
        case = self.cases[context.scenario.case_id] = _Case(driver)
        try:
            await case.call(driver.start)
        except Exception:
            # The worker has joined. Keep even prepared launch intents and
            # pending control writes for reconciliation.
            if not (
                context.attempts
                or cloud.invocations
                or any(record.get("state") != "REJECTED" for record in cloud.control_records)
                or getattr(cloud, "events", {})
                or getattr(cloud, "raw_histories", {})
                or not _only_undispatched_journals(cloud, context)
            ):
                del self.cases[context.scenario.case_id]
                self.no_work_cases.add(context.scenario.case_id)
            raise

    async def place(self, context: ScenarioContext) -> None:
        case = self._case(context)
        await case.call(case.driver.place)

    async def execute(self, context: ScenarioContext) -> None:
        case = self._case(context)
        await case.call(case.driver.execute)

    async def collect(self, context: ScenarioContext) -> dict[str, Any]:
        case = self._case(context)

        def collect() -> dict[str, Any]:
            case.driver.collect()
            return self.evidence_builder(context, case.driver.cloud, phase="observations")

        return await case.call(collect)

    async def collect_partial(self, context: ScenarioContext) -> dict[str, Any]:
        """Retain a failed phase's facts before release, without awaiting cleanup."""
        case_id = context.scenario.case_id
        if case_id in self.no_work_cases and case_id not in self.cases:
            # `start` already proved this case never did any work and deleted
            # it; there is nothing to collect and no service call to make.
            return {}
        case = self._case(context)

        def capture() -> dict[str, Any]:
            cloud = case.driver.cloud
            read_error = None
            if cloud.observation_ended_at is None:
                cloud.observation_ended_at = time.time()
            try:
                cloud.collect(full_run=False)
            except CollectionBudgetExpired:
                pass
            except Exception as error:
                read_error = f"Partial source collection: {type(error).__name__}: {error}"
            partial = self.evidence_builder(context, cloud, phase="partial")
            partial.setdefault("collection", {})["watermarks"] = {}
            if read_error is not None:
                partial.setdefault("collectionErrors", []).append(read_error)
            cloud._save("shared-partial-evidence.json", partial)
            return partial

        return await case.call(capture)

    async def release(self, context: ScenarioContext) -> None:
        case = self.cases.get(context.scenario.case_id)
        if case is None:
            return
        await case.prepare_retirement()
        await case.call(case.driver.cloud.release_all)

    async def retire(self, context: ScenarioContext) -> bool:
        case = self.cases.get(context.scenario.case_id)
        if case is None:
            return True
        await case.prepare_retirement()

        def retire() -> bool:
            cloud = case.driver.cloud
            controls = getattr(cloud, "control_records", [])
            if (
                controls
                and all(record.get("action") == "hold" and record.get("state") == "REJECTED" for record in controls)
                and not (
                    context.attempts
                    or cloud.invocations
                    or getattr(cloud, "events", {})
                    or getattr(cloud, "raw_histories", {})
                    or not _only_undispatched_journals(cloud, context)
                )
            ):
                return True
            try:
                complete = case.driver.retire()
                final = self.evidence_builder(context, cloud, phase="final")
            except Exception as error:
                try:
                    cloud.observation_ended_at = time.time()
                    partial = self.evidence_builder(context, cloud, phase="retirement-partial")
                    partial.pop("retirement", None)
                    partial.setdefault("collection", {})["watermarks"] = {}
                    context.retirement_evidence = partial
                    cloud._save("shared-partial-retirement-evidence.json", partial)
                except Exception as capture_error:
                    error.add_note(f"Partial retirement evidence could not be saved: {capture_error}")
                raise
            # The coordinator freezes observation evidence before fault release.
            # Only actual retirement facts may be appended by this phase.
            retirement = deepcopy(final.get("retirement", {}))
            resource_ids = {
                (resource["kind"], resource["id"]) for resource in context.evidence.get("resourcesCreated", [])
            }
            fault_ids = {fault["id"] for fault in context.evidence.get("faultsCreated", [])}
            if "resources" in retirement:
                retirement["resources"] = [
                    item for item in retirement["resources"] if (item["kind"], item["id"]) in resource_ids
                ]
            if "faults" in retirement:
                retirement["faults"] = [item for item in retirement["faults"] if item["id"] in fault_ids]
            context.evidence["retirement"] = retirement
            # Fresh completion facts prove physical retirement separately; they
            # must never replace the case's pre-release behavior observations.
            context.retirement_evidence = final
            return complete

        return await case.call(retire)

    async def finish(self, profile: Profile, *, quiescent: bool) -> None:
        # Even a terminal execution can leave invocation-owned runtime work.
        # Retain the pre-admission fence on failed or uncertain retirement.
        resource = profile.resource_id
        worker = self.preparation_workers.get(resource)
        if worker is not None:
            # The provisioning phase already reports failures from this worker.
            with suppress(Exception):
                self.prepared_clouds[resource] = await _await_without_cancelling(worker)
            del self.preparation_workers[resource]
        prepared = self.prepared_clouds.get(resource)
        if prepared is None or not quiescent:
            return
        from .quarantine import release_fence

        cloud, token = prepared
        journal = self.artifacts_dir / "configuration" / f"{case_key(resource)}-fence.json"

        def release() -> None:
            with owner_lock(journal):
                save(
                    journal,
                    {
                        "run": self.deployment["run"],
                        "commit": self.deployment["commit"],
                        "resourceId": resource,
                        "token": token,
                        "state": "RETIRED",
                    },
                )
                release_fence(cloud.s3, self.deployment, resource, token)
                journal.unlink(missing_ok=True)

        await asyncio.to_thread(release)
        del self.prepared_clouds[resource]
