# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Extension contract and validation-hook tests with a patched retriever."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, ClassVar

import pytest

from aws_durable_execution_conformance_tests.cloudwatch import CloudWatchLogError
from aws_durable_execution_conformance_tests.extensions import ValidationContext
from aws_durable_execution_conformance_tests_telemetry import extension as extension_module
from aws_durable_execution_conformance_tests_telemetry.extension import TelemetryExtension

ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:conformance-tests-abc-telemetry-1:$LATEST"
    "/durable-execution/8f1c2d3e-0000-4000-8000-000000000001/inv-1"
)
REQUIREMENT: dict[str, Any] = {
    "TelemetryAssertions": {
        "record": {
            "recordType": "DurableExecutionTelemetry",
            "schemaVersion": "1.0",
            "status": "SUCCEEDED",
            "executionArn": "${EXECUTION_ARN}",
            "input": {"value": 41},
            "output": {"value": 42},
        },
        "forbid_fields": ["error"],
        "operations_by_name": {
            "increment": {"type": "STEP", "status": "SUCCEEDED", "count": 1, "failedCount": 0},
        },
    },
}


def _record() -> dict[str, Any]:
    return {
        "recordType": "DurableExecutionTelemetry",
        "schemaVersion": "1.0",
        "executionArn": ARN,
        "functionName": "conformance-tests-abc-telemetry-1",
        "functionQualifier": "$LATEST",
        "region": "us-west-2",
        "accountId": "123456789012",
        "status": "SUCCEEDED",
        "input": {"value": 41},
        "output": {"value": 42},
        "operationsByName": {
            "increment": {"type": "STEP", "subType": "Step", "count": 1, "failedCount": 0, "status": "SUCCEEDED"},
        },
    }


def _event(payload: Any, timestamp: int = 1) -> dict[str, Any]:
    return {"timestamp": timestamp, "message": json.dumps(payload)}


class _FakeRetriever:
    events: ClassVar[list[dict[str, Any]] | Exception] = []
    calls: ClassVar[list[dict[str, Any]]] = []

    def __init__(self, cfn_client: Any, logs_client: Any) -> None:
        self.clients = (cfn_client, logs_client)

    def get_execution_log_events(self, **kwargs: Any) -> list[dict[str, Any]]:
        type(self).calls.append(kwargs)
        if isinstance(self.events, Exception):
            raise self.events
        return self.events


@pytest.fixture
def retriever(monkeypatch: pytest.MonkeyPatch) -> type[_FakeRetriever]:
    _FakeRetriever.events = []
    _FakeRetriever.calls = []
    monkeypatch.setattr(extension_module, "CloudWatchLogRetriever", _FakeRetriever)
    return _FakeRetriever


def _args(**overrides: Any) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="abc")
    TelemetryExtension().add_arguments(parser)
    args = parser.parse_args([])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _context(
    tmp_path: Path,
    *,
    requirement: dict[str, Any] | None = None,
    region: str = "us-west-2",
    **options: Any,
) -> ValidationContext:
    return ValidationContext(
        description_id="telemetry-1",
        function_name="conformance-tests-abc-telemetry-1",
        execution_arn=ARN,
        invocation_started_at_ms=1_700_000_000_000,
        invocation_finished_at_ms=1_700_000_001_000,
        region=region,
        language="js",
        requirement=requirement if requirement is not None else REQUIREMENT,
        execution_history={"Events": [{"EventId": 1}]},
        output_dir=tmp_path / "output",
        placeholders={"EXECUTION_ARN": ARN, "ID1": "step-id"},
        options={"name": "abc", "telemetry_log_group": None, "telemetry_wait_seconds": 4, **options},
        aws_clients={"cloudformation": object(), "logs": object()},
    )


def _artifact(tmp_path: Path) -> dict[str, Any]:
    return json.loads((tmp_path / "output" / "telemetry-1-telemetry.json").read_text())


def test_requirement_suite_is_rooted_at_source_directory() -> None:
    (suite,) = TelemetryExtension().requirement_suites()
    assert suite.name == "telemetry"
    assert suite.provider == "telemetry"
    assert suite.validation_hook is not None
    expected_root = Path(extension_module.__file__).resolve().parent.parent.parent / "test-requirements" / "telemetry"
    assert suite.root == expected_root
    assert (suite.root / "telemetry-1.yaml").is_file()


def test_deployment_parameters_default_log_group() -> None:
    extension = TelemetryExtension()
    args = _args()
    extension.validate_configuration(args)
    assert extension.deployment_parameters(args) == {
        "TelemetryLogGroupName": "/durable-execution-conformance/conformance-tests-abc/durable-execution-telemetry",
    }
    assert extension.deployment_secrets(args) == {}
    assert extension.validation_client_services(args) == ()


def test_deployment_parameters_override_log_group() -> None:
    args = _args(telemetry_log_group="/custom/group")
    assert TelemetryExtension().deployment_parameters(args) == {"TelemetryLogGroupName": "/custom/group"}


def test_validate_configuration_rejects_negative_wait() -> None:
    with pytest.raises(ValueError, match="telemetry-wait-seconds"):
        TelemetryExtension().validate_configuration(_args(telemetry_wait_seconds=-1))


def test_validate_configuration_rejects_empty_log_group() -> None:
    with pytest.raises(ValueError, match="telemetry-log-group"):
        TelemetryExtension().validate_configuration(_args(telemetry_log_group="  "))


def test_add_arguments_has_no_write_artifact_flag() -> None:
    # The artifact location comes from the core --history-dir option, like
    # every other suite. A suite-specific toggle must not come back.
    args = _args()
    assert "telemetry_write_artifact" not in vars(args)
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="abc")
    TelemetryExtension().add_arguments(parser)
    with pytest.raises(SystemExit):
        parser.parse_args(["--telemetry-write-artifact"])


def test_validate_telemetry_passes_and_writes_artifact(tmp_path: Path, retriever: type[_FakeRetriever]) -> None:
    retriever.events = [_event(_record())]
    errors = TelemetryExtension().validate_telemetry(_context(tmp_path))
    assert errors == []
    artifact = _artifact(tmp_path)
    assert artifact["errors"] == []
    assert artifact["records"] == [_record()]
    assert artifact["execution_history"] == {"Events": [{"EventId": 1}]}
    assert retriever.calls == [
        {
            "log_group_name": "/durable-execution-conformance/conformance-tests-abc/durable-execution-telemetry",
            "execution_arn": ARN,
            "start_time_ms": 1_700_000_000_000 - 30_000,
            "wait_seconds": 4,
        }
    ]


def test_validate_telemetry_uses_log_group_override(tmp_path: Path, retriever: type[_FakeRetriever]) -> None:
    retriever.events = [_event(_record())]
    TelemetryExtension().validate_telemetry(_context(tmp_path, telemetry_log_group="/custom/group"))
    assert retriever.calls[0]["log_group_name"] == "/custom/group"


def test_validate_telemetry_no_records(tmp_path: Path, retriever: type[_FakeRetriever]) -> None:
    retriever.events = [{"timestamp": 1, "message": "not json"}]
    errors = TelemetryExtension().validate_telemetry(_context(tmp_path))
    assert errors == [
        f"Durable Execution Telemetry: no DurableExecutionTelemetry record for {ARN} in "
        "/durable-execution-conformance/conformance-tests-abc/durable-execution-telemetry within the retrieval window "
        "(1 raw events, 1 malformed)"
    ]
    artifact = _artifact(tmp_path)
    assert artifact["malformed"] == ["not json"]
    assert artifact["records"] == []
    assert artifact["errors"] == errors


def test_validate_telemetry_access_failure(tmp_path: Path, retriever: type[_FakeRetriever]) -> None:
    retriever.events = CloudWatchLogError("/g", "filter-log-events failed: AccessDeniedException")
    errors = TelemetryExtension().validate_telemetry(_context(tmp_path))
    assert len(errors) == 1
    assert errors[0].startswith("Durable Execution Telemetry: access failure: ")
    assert "AccessDeniedException" in errors[0]


def test_validate_telemetry_assertion_failure(tmp_path: Path, retriever: type[_FakeRetriever]) -> None:
    bad = _record()
    bad["status"] = "FAILED"
    retriever.events = [_event(bad)]
    errors = TelemetryExtension().validate_telemetry(_context(tmp_path))
    assert errors == [
        "Durable Execution Telemetry: status: expected 'SUCCEEDED', got 'FAILED'",
        "Durable Execution Telemetry: 1 record(s) retrieved, none satisfied every assertion",
    ]
    artifact = _artifact(tmp_path)
    assert artifact["errors"] == errors
    assert artifact["log_group"] == "/durable-execution-conformance/conformance-tests-abc/durable-execution-telemetry"


def test_validate_telemetry_accepts_complete_record_among_partials(
    tmp_path: Path, retriever: type[_FakeRetriever]
) -> None:
    running = _record()
    running["status"] = "RUNNING"
    del running["output"]
    retriever.events = [_event(running, 1), _event(_record(), 2)]
    assert TelemetryExtension().validate_telemetry(_context(tmp_path)) == []


def test_validate_telemetry_region_mismatch(tmp_path: Path, retriever: type[_FakeRetriever]) -> None:
    errors = TelemetryExtension().validate_telemetry(_context(tmp_path, region="us-east-1"))
    assert errors == [
        "Durable Execution Telemetry: execution ARN region 'us-west-2' differs from run region 'us-east-1'"
    ]
    assert retriever.calls == []


def test_validate_telemetry_rejects_non_mapping_assertions(tmp_path: Path, retriever: type[_FakeRetriever]) -> None:
    errors = TelemetryExtension().validate_telemetry(_context(tmp_path, requirement={"TelemetryAssertions": []}))
    assert errors == ["TelemetryAssertions must be a mapping"]
    assert retriever.calls == []
