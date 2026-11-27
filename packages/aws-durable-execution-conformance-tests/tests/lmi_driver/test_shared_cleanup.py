# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Run-scoped cleanup needs no distributed lock and never touches foreign work."""

import hashlib
import io
import json
from typing import Any
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, ReadTimeoutError

from aws_durable_execution_conformance_tests.lmi import artifacts as artifacts_module
from aws_durable_execution_conformance_tests.lmi import runner as shared
from aws_durable_execution_conformance_tests.lmi.errors import CollectionError

from .test_shared_adapter import deployment as _not_implemented_deployment


@pytest.fixture(autouse=True)
def _stable_boot_identity(monkeypatch):
    """Pin owner_lock's boot identity so these tests do not depend on the real host.

    Individual tests that exercise an unavailable identity still override
    this with their own, more specific monkeypatch.
    """
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: "test-host-boot-id")


def test_no_recorded_cases_skip_aws_cleanup(monkeypatch, tmp_path):
    client = Mock(side_effect=AssertionError("No case work was recorded"))
    monkeypatch.setattr(shared, "client", client)
    assert shared.cleanup({"run": "run", "functions": {}}, tmp_path) == 0
    assert json.loads((tmp_path / "cleanup.json").read_text())["status"] == "NO_CASES"
    client.assert_not_called()


@pytest.mark.parametrize("foreign", [True, False])
def test_case_identity_is_checked_before_control_release(monkeypatch, tmp_path, foreign):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    owned = target + "/durable-execution/name/id"
    deployment = {
        "run": "run",
        "bucket": "private",
        "region": "us-west-2",
        "functions": {"c1": target},
    }
    case = {"case_id": "case", "profile_id": "c1"}
    plan = {
        "runId": "run",
        "profiles": [{"profile_id": "c1", "resource_id": target}],
        "scenarios": [case],
    }
    output = tmp_path / "standard"
    output.mkdir()
    (output / "manifest.json").write_text(json.dumps(plan))
    identity = hashlib.sha256(b"case").hexdigest()[:20]
    invocations = output / "source/cases" / identity / "invocations"
    invocations.mkdir(parents=True)
    (invocations / "intent.json").write_text(
        json.dumps(
            {
                "payload": {"run": "run", "caseId": "case"},
                "durableExecutionName": "name",
                "arn": "arn:foreign" if foreign else owned,
            }
        )
    )
    storage, service = Mock(), Mock()
    prefix = f"runs/run/cases/{identity}/control/"
    storage.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": prefix + "release-all"}]}]
    storage.get_object.side_effect = lambda **kwargs: {"Body": io.BytesIO(b"hold"), "ETag": '"hold"'}
    service.get_durable_execution.return_value = {"Status": "RUNNING"}
    monkeypatch.setattr(shared, "client", lambda name, region: storage if name == "s3" else service)
    assert shared.cleanup(deployment, tmp_path) == int(foreign)
    report = json.loads((tmp_path / "cleanup.json").read_text())
    assert report["status"] == ("FAILED" if foreign else "CLEANUP_REQUESTED")
    if foreign:
        storage.put_object.assert_not_called()
        service.stop_durable_execution.assert_not_called()
    else:
        storage.put_object.assert_called_once_with(
            Bucket="private", Key=prefix + "release-all", Body=b"release", IfMatch='"hold"'
        )
        service.stop_durable_execution.assert_called_once_with(DurableExecutionArn=owned)
    assert all(call.kwargs["Key"].startswith(prefix) for call in storage.get_object.call_args_list)


@pytest.mark.parametrize("wrong", ["run", "target"])
def test_cleanup_rejects_foreign_manifest_before_aws_calls(monkeypatch, tmp_path, wrong):
    deployment = {"run": "run", "functions": {"c1": "arn:owned"}}
    output = tmp_path / "standard"
    output.mkdir()
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "other" if wrong == "run" else "run",
                "profiles": [
                    {
                        "profile_id": "c1",
                        "resource_id": "arn:foreign" if wrong == "target" else "arn:owned",
                    }
                ],
                "scenarios": [{"profile_id": "c1", "case_id": "case"}],
            }
        )
    )
    client = Mock(side_effect=AssertionError("Foreign targets must not be touched"))
    monkeypatch.setattr(shared, "client", client)
    with pytest.raises(CollectionError):
        shared.cleanup(deployment, tmp_path)
    client.assert_not_called()


def test_one_identified_execution_does_not_hide_another_unreconciled_invoke(monkeypatch, tmp_path):
    from pathlib import Path

    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    execution = target + "/durable-execution/name/id"
    deployment = {
        "run": "run",
        "bucket": "private",
        "region": "us-west-2",
        "driverTimeout": 0,
        "functions": {"c1": target},
    }
    case = {"case_id": "case", "profile_id": "c1"}
    output = tmp_path / "standard"
    output.mkdir()
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [case],
            }
        )
    )
    owner = hashlib.sha256(b"case").hexdigest()[:20]
    intents = output / "source/cases" / owner / "invocations"
    intents.mkdir(parents=True)
    known, missing = intents / "known.json", intents / "missing.json"
    payload = {"run": "run", "caseId": "case"}
    known.write_text(
        json.dumps({"payload": {**payload, "marker": "known"}, "durableExecutionName": "name", "arn": execution})
    )
    missing.write_text(
        json.dumps({"payload": {**payload, "marker": "missing"}, "durableExecutionName": "missing", "arn": None})
    )
    original_glob = Path.glob
    monkeypatch.setattr(
        Path,
        "glob",
        lambda path, pattern: (iter([known, missing]) if path == intents else original_glob(path, pattern)),
    )
    storage, service = Mock(), Mock()
    storage.get_paginator.return_value.paginate.return_value = []
    service.list_durable_executions_by_function.return_value = {"DurableExecutions": []}
    service.get_durable_execution.return_value = {"Status": "SUCCEEDED"}
    monkeypatch.setattr(shared, "client", lambda name, region: storage if name == "s3" else service)
    assert shared.cleanup(deployment, tmp_path) == 1
    errors = json.loads((tmp_path / "cleanup.json").read_text())["errors"]
    assert errors == ["Unreconciled Invoke: case/missing"]


def test_cleanup_rejects_a_crossed_arn_on_the_same_target(tmp_path):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    deployment = {"run": "run", "bucket": "private", "region": "us-west-2", "functions": {"c1": target}}
    output = tmp_path / "standard"
    output.mkdir()
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [{"case_id": "case", "profile_id": "c1"}],
            }
        )
    )
    intents = output / "source/cases" / shared.case_key("case") / "invocations"
    intents.mkdir(parents=True)
    payload = {"run": "run", "caseId": "case", "marker": "marker"}
    (intents / "intent.json").write_text(json.dumps({"payload": payload, "durableExecutionName": "name"}))
    storage, service = Mock(), Mock()
    event = {**payload, "phase": "WRAPPER_ENTER", "execution": target + "/durable-execution/other/id"}
    storage.get_paginator.return_value.paginate.side_effect = [
        [],
        [{"Contents": [{"Key": "runs/run/events/marker/0000.json"}]}],
    ]
    storage.get_object.return_value = {"Body": io.BytesIO(json.dumps(event).encode())}
    assert (
        shared.cleanup(deployment, tmp_path, client_factory=lambda name, _: storage if name == "s3" else service) == 1
    )
    service.stop_durable_execution.assert_not_called()


def test_cleanup_releases_verified_control_even_when_ledger_read_fails(tmp_path):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    deployment = {"run": "run", "bucket": "private", "region": "us-west-2", "functions": {"c1": target}}
    output = tmp_path / "standard"
    output.mkdir()
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [{"case_id": "case", "profile_id": "c1"}],
            }
        )
    )
    owner = shared.case_key("case")
    intents = output / "source/cases" / owner / "invocations"
    intents.mkdir(parents=True)
    payload = {"run": "run", "caseId": "case", "marker": "fixture"}
    (intents / "intent.json").write_text(json.dumps({"payload": payload, "durableExecutionName": "fixture"}))
    storage, service = Mock(), Mock()
    key = f"runs/run/cases/{owner}/control/release-all"
    storage.get_paginator.return_value.paginate.side_effect = [
        [{"Contents": [{"Key": key}]}],
        ClientError({"Error": {"Code": "SlowDown"}}, "ListObjectsV2"),
    ]
    storage.get_object.return_value = {"Body": io.BytesIO(b"hold"), "ETag": '"hold"'}
    assert shared.cleanup(deployment, tmp_path, client_factory=lambda name, _: storage if name == "s3" else service)
    storage.put_object.assert_called_once_with(Bucket="private", Key=key, Body=b"release", IfMatch='"hold"')
    service.stop_durable_execution.assert_not_called()


def test_default_cleanup_uses_saved_relative_case_output(tmp_path):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    deployment = {
        "run": "run",
        "bucket": "private",
        "region": "us-west-2",
        "functions": {"c1": target},
        "deploymentArtifactDirectory": str(tmp_path),
    }
    output = tmp_path / "custom" / "standard"
    output.mkdir(parents=True)
    (tmp_path / "case-output.json").write_text(json.dumps({"runId": "run", "directory": "custom"}))
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [{"case_id": "case", "profile_id": "c1"}],
            }
        )
    )
    storage = Mock()
    key = f"runs/run/cases/{shared.case_key('case')}/control/release-all"
    storage.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": key}]}]
    storage.get_object.return_value = {"Body": io.BytesIO(b"hold"), "ETag": '"hold"'}
    factory = Mock(return_value=storage)
    assert shared.cleanup(deployment, client_factory=factory) == 0
    storage.put_object.assert_called_once_with(Bucket="private", Key=key, Body=b"release", IfMatch='"hold"')
    assert json.loads((output.parent / "cleanup.json").read_text())["status"] == "CLEANUP_REQUESTED"
    assert shared.cleanup(deployment, tmp_path / "explicit", client_factory=factory) == 0
    assert json.loads((tmp_path / "explicit" / "cleanup.json").read_text())["status"] == "NO_CASES"


@pytest.mark.parametrize("hold_race", [None, "conflict", "lost-response"])
@pytest.mark.parametrize("custom_source", [False, True])
def test_cleanup_protects_pending_control_absent_from_listing(tmp_path, hold_race, custom_source):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    deployment = {"run": "run", "bucket": "private", "region": "us-west-2", "functions": {"c1": target}}
    coverage = tmp_path / "standard"
    source = tmp_path / "custom-source" if custom_source else coverage / "source"
    case_dir = source / "cases" / shared.case_key("case")
    case_dir.mkdir(parents=True)
    coverage.mkdir(exist_ok=True)
    (coverage / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                **({"sdk": {"artifacts_dir": str(source)}} if custom_source else {}),
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [{"case_id": "case", "profile_id": "c1"}],
            }
        )
    )
    (case_dir / "control-records.json").write_text(
        json.dumps(
            [
                {"gate": "unseen", "action": "hold", "state": "PENDING"},
            ]
        )
    )
    storage = Mock()
    storage.get_paginator.return_value.paginate.return_value = [{"Contents": []}]
    missing = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    storage.get_object.side_effect = (
        [missing, {"Body": io.BytesIO(b"hold"), "ETag": '"held"'}] if hold_race else missing
    )
    if hold_race:
        failure = (
            ReadTimeoutError(endpoint_url="test")
            if hold_race == "lost-response"
            else ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        )
        storage.put_object.side_effect = [failure, {}]
    assert shared.cleanup(deployment, tmp_path, client_factory=lambda *_: storage) == 0
    key = f"runs/run/cases/{shared.case_key('case')}/control/unseen"
    assert storage.put_object.call_args_list[0].kwargs == {
        "Bucket": "private",
        "Key": key,
        "Body": b"release",
        "IfNoneMatch": "*",
    }
    if hold_race:
        assert storage.put_object.call_args_list[1].kwargs == {
            "Bucket": "private",
            "Key": key,
            "Body": b"release",
            "IfMatch": '"held"',
        }
    assert storage.put_object.call_count == (2 if hold_race else 1)
    storage.delete_object.assert_not_called()


@pytest.mark.parametrize(
    "state,single_attempt,released",
    [
        ("ACQUIRING", False, False),
        ("ACQUIRING", True, True),
        ("NOT_ADMITTED", False, True),
        ("RETIRED", False, True),
        ("ACQUIRED", True, False),
    ],
)
def test_offline_profile_recovery_honors_custom_root_and_receipt(tmp_path, state, single_attempt, released):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    deployment = {
        "run": "run",
        "commit": "commit",
        "bucket": "private",
        "region": "us-west-2",
        "functions": {"c1": target},
    }
    coverage = tmp_path / "standard"
    source = tmp_path / "custom-profile-source"
    journals = source / "configuration"
    journals.mkdir(parents=True)
    coverage.mkdir()
    (coverage / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "sdk": {"artifacts_dir": str(source)},
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [],
            }
        )
    )
    journal = journals / f"{shared.case_key(target)}-fence.json"
    record = {
        "resourceId": target,
        "token": "owned",
        "run": "run",
        "commit": "commit",
        "singleAttempt": single_attempt,
        "state": state,
    }
    with shared.owner_lock(journal) as owner:
        journal.write_text(json.dumps({**record, "owner": owner}))
    storage = Mock()
    storage.get_object.side_effect = lambda **_: {
        "Body": io.BytesIO(json.dumps({**record, "runId": "run"}).encode()),
        "ETag": '"own"',
    }
    expected_error = state == "ACQUIRING" and not single_attempt
    assert shared.cleanup(deployment, tmp_path, client_factory=lambda *_: storage) == int(expected_error)
    assert journal.exists() is not released
    assert storage.delete_object.call_count == int(released)


def test_run_persists_relative_binding_source_as_absolute(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import app

    working_directory = tmp_path / "original-cwd"
    working_directory.mkdir()
    monkeypatch.chdir(working_directory)
    binding_source = working_directory / "journals"
    output = tmp_path / "run-output"

    def factory(*_):
        return {"runId": "run", "sdk": {"deployment": {"conformanceRevision": "commit"}, "artifacts_dir": "journals"}}

    def stop_before_runtime(path):
        assert json.loads(path.read_text())["sdk"]["artifacts_dir"] == str(binding_source)
        assert binding_source.is_dir()
        raise RuntimeError("stop before runtime")

    monkeypatch.setattr(app, "load_manifest", stop_before_runtime)
    with pytest.raises(RuntimeError, match="stop before runtime"):
        shared.run({"run": "run"}, output, manifest_factory=factory)
    monkeypatch.chdir(tmp_path)
    plan = json.loads((output / "standard/manifest.json").read_text())
    assert shared.sdk_artifact_directory(output / "standard", plan) == binding_source


def _profile_recovery_fixture(tmp_path, record):
    """Build the custom-root layout test_offline_profile_recovery_* needs, then write `record`."""
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    deployment = {
        "run": "run",
        "commit": "commit",
        "bucket": "private",
        "region": "us-west-2",
        "functions": {"c1": target},
    }
    coverage = tmp_path / "standard"
    source = tmp_path / "custom-profile-source"
    journals = source / "configuration"
    journals.mkdir(parents=True)
    coverage.mkdir()
    (coverage / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "sdk": {"artifacts_dir": str(source)},
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [],
            }
        )
    )
    journal = journals / f"{shared.case_key(target)}-fence.json"
    with shared.owner_lock(journal) as owner:
        journal.write_text(json.dumps({**record, "owner": {**owner, **record.get("owner", {})}}))
    storage = Mock()
    storage.get_object.side_effect = lambda **_: {
        "Body": io.BytesIO(json.dumps({**record, "runId": "run"}).encode()),
        "ETag": '"own"',
    }
    return deployment, journal, storage


@pytest.mark.parametrize("state", ["NOT_ADMITTED", "RETIRED"])
def test_offline_profile_recovery_recovers_without_boot_identity_when_not_acquiring(tmp_path, monkeypatch, state):
    """A NOT_ADMITTED/RETIRED fence journal needs only the flock, not an owner proof."""
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    record = {
        "resourceId": target,
        "token": "owned",
        "run": "run",
        "commit": "commit",
        "singleAttempt": False,
        "state": state,
    }
    deployment, journal, storage = _profile_recovery_fixture(tmp_path, record)
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: None)
    assert shared.cleanup(deployment, tmp_path, client_factory=lambda *_: storage) == 0
    assert not journal.exists()
    storage.delete_object.assert_called_once()


def test_offline_profile_recovery_refuses_acquiring_without_boot_identity(tmp_path, monkeypatch):
    """An ACQUIRING fence journal is still uncertain: it needs same_owner_lock's falsy check."""
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    record = {
        "resourceId": target,
        "token": "owned",
        "run": "run",
        "commit": "commit",
        "singleAttempt": True,
        "state": "ACQUIRING",
    }
    deployment, journal, storage = _profile_recovery_fixture(tmp_path, record)
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: None)
    assert shared.cleanup(deployment, tmp_path, client_factory=lambda *_: storage) == 1
    assert journal.exists()
    storage.delete_object.assert_not_called()


@pytest.mark.parametrize(
    "differing_field,replacement",
    [("bootId", "a-different-hosts-boot-id"), ("inode", 999999999)],
)
def test_offline_profile_recovery_refuses_a_foreign_owner(tmp_path, differing_field, replacement):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    record = {
        "resourceId": target,
        "token": "owned",
        "run": "run",
        "commit": "commit",
        "singleAttempt": True,
        "state": "ACQUIRING",
        "owner": {differing_field: replacement},
    }
    deployment, journal, storage = _profile_recovery_fixture(tmp_path, record)
    assert shared.cleanup(deployment, tmp_path, client_factory=lambda *_: storage) == 1
    assert journal.exists()
    storage.delete_object.assert_not_called()


def test_cleanup_uses_relocated_direct_cli_manifest(tmp_path):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    arn = target + "/durable-execution/name/id"
    deployment = {"run": "run", "bucket": "private", "region": "us-west-2", "functions": {"c1": target}}
    root = tmp_path / "download"
    source = root / "copied-source"
    journal = source / "cases" / shared.case_key("case") / "invocations" / "marker.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(
        json.dumps(
            {
                "payload": {"run": "run", "caseId": "case", "marker": "marker"},
                "function": target,
                "arn": arn,
                "durableExecutionName": "name",
            }
        )
    )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "sdk": {"artifacts_dir": "/old/workspace/source", "artifacts_dir_relative": "copied-source"},
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [{"case_id": "case", "profile_id": "c1"}],
            }
        )
    )
    storage, service = Mock(), Mock()
    storage.get_paginator.return_value.paginate.return_value = []
    service.get_durable_execution.return_value = {"Status": "RUNNING"}
    assert shared.cleanup(deployment, root, client_factory=lambda name, _: storage if name == "s3" else service) == 0
    service.stop_durable_execution.assert_called_once_with(DurableExecutionArn=arn)


def test_rebased_active_deployment_discovers_default_case_output(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.deployment import load_cleanup_manifest

    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    arn = target + "/durable-execution/name/id"
    deployment = {
        "run": "run",
        "bucket": "private",
        "region": "us-west-2",
        "functions": {"c1": target},
        "deploymentArtifactDirectory": "/old/workspace",
    }
    (tmp_path / "manifest.json").write_text(json.dumps(deployment))
    run_root = tmp_path / "shared"
    source = run_root / "source"
    journal = source / "cases" / shared.case_key("case") / "invocations" / "mark.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(
        json.dumps(
            {
                "payload": {"run": "run", "caseId": "case", "marker": "mark"},
                "function": target,
                "arn": arn,
                "durableExecutionName": "name",
            }
        )
    )
    (run_root / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "sdk": {"artifacts_dir_relative": "source"},
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [{"case_id": "case", "profile_id": "c1"}],
            }
        )
    )
    storage, service = Mock(), Mock()
    storage.get_paginator.return_value.paginate.return_value = []
    service.get_durable_execution.return_value = {"Status": "RUNNING"}
    actual = load_cleanup_manifest(tmp_path, "run")
    assert actual["deploymentArtifactDirectory"] == str(tmp_path)
    assert shared.cleanup(actual, client_factory=lambda name, _: storage if name == "s3" else service) == 0
    service.stop_durable_execution.assert_called_once_with(DurableExecutionArn=arn)


def test_unavailable_recorded_output_is_failed_without_creating_destination(tmp_path):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    destination = tmp_path / "unmounted" / "run"
    (artifacts / "case-output.json").write_text(json.dumps({"runId": "run", "directory": str(destination)}))
    deployment = {"run": "run", "deploymentArtifactDirectory": str(artifacts)}
    factory = Mock(side_effect=AssertionError("No cloud calls are needed"))
    assert shared.cleanup(deployment, client_factory=factory) == 1
    saved = artifacts / "cleanup" / shared.case_key("run") / "cleanup.json"
    assert json.loads(saved.read_text())["status"] == "FAILED"
    assert str(destination) in saved.read_text()
    assert not destination.exists()
    factory.assert_not_called()


def _declare_every_case_not_implemented(deployment, coverage, output):
    """A ``manifest_factory`` shaped like ``make_manifest``'s, but with every
    scenario declared ``not_implemented``.

    This is the real-world trigger for the missing-journal-root defect:
    with no active scenario, ``_collect_and_adjudicate`` returns before an
    adapter is ever constructed, so nothing is written under the SDK
    artifact directory ``sdk_plan_with_artifact_directory`` creates.
    """
    plan = shared.make_manifest(
        deployment, coverage, output, artifact_id="sha256:test-artifact", conformance_revision=deployment["commit"]
    )
    plan["scenarios"] = [dict(case, not_implemented="fixture skip") for case in plan["scenarios"]]
    return plan


def _copy_files_only(source, destination):
    """Simulate a files-only artifact upload/download (e.g. ``actions/upload-artifact``).

    That action uploads only files -- a directory holding no files is
    dropped entirely -- and skips hidden files by default, so parent
    directories are recreated only for the files actually copied.
    """
    for path in source.rglob("*"):
        relative = path.relative_to(source)
        if path.is_file() and not any(part.startswith(".") for part in relative.parts):
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(path.read_bytes())


def test_cleanup_survives_a_files_only_upload_when_every_case_is_not_implemented(tmp_path):
    """The in-tree SDK journal root's marker keeps it visible to a files-only
    artifact upload/download even though no adapter ever ran, so cleanup on
    the downloaded copy does not fail closed on a missing directory.
    """
    deployment = _not_implemented_deployment()
    output = tmp_path / "run-output"
    # NOT_IMPLEMENTED is strict-blocking for build_report, not itself a defect.
    assert shared.run(deployment, output, manifest_factory=_declare_every_case_not_implemented) == 1

    source_root = output / "standard" / "source"
    # A hidden marker would be dropped by the upload, as `_copy_files_only` models.
    assert not artifacts_module.SDK_ARTIFACT_DIRECTORY_MARKER.startswith(".")
    marker = source_root / artifacts_module.SDK_ARTIFACT_DIRECTORY_MARKER
    assert marker.is_file()
    assert json.loads(marker.read_text()) == {"kind": "lmi-sdk-artifacts"}
    assert not any(p for p in source_root.iterdir() if p.name != artifacts_module.SDK_ARTIFACT_DIRECTORY_MARKER)

    downloaded = tmp_path / "downloaded"
    _copy_files_only(output, downloaded)
    # The files-only copy still preserves the (otherwise-empty) journal root.
    assert (downloaded / "standard" / "source" / artifacts_module.SDK_ARTIFACT_DIRECTORY_MARKER).is_file()

    storage, service = Mock(), Mock()
    storage.get_paginator.return_value.paginate.return_value = [{"Contents": []}]
    assert (
        shared.cleanup(deployment, downloaded, client_factory=lambda name, _: storage if name == "s3" else service) == 0
    )
    report = json.loads((downloaded / "cleanup.json").read_text())
    assert report["status"] == "CLEANUP_REQUESTED"
    assert report["errors"] == []


def test_cleanup_still_fails_closed_when_the_journal_root_is_genuinely_missing(tmp_path):
    """Without the marker (or any other file), a files-only copy drops the
    empty journal root entirely, and cleanup must keep refusing that case.
    """
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    deployment = {"run": "run", "bucket": "private", "region": "us-west-2", "functions": {"c1": target}}
    root = tmp_path / "downloaded"
    (root / "standard").mkdir(parents=True)
    (root / "standard" / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "sdk": {"artifacts_dir_relative": "source"},
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [],
            }
        )
    )
    storage, service = Mock(), Mock()
    storage.get_paginator.return_value.paginate.return_value = [{"Contents": []}]
    assert shared.cleanup(deployment, root, client_factory=lambda name, _: storage if name == "s3" else service) == 1
    report = json.loads((root / "cleanup.json").read_text())
    assert report["status"] == "FAILED"
    assert any("unavailable" in error for error in report["errors"])
    service.stop_durable_execution.assert_not_called()


def test_sdk_plan_with_artifact_directory_marker_is_idempotent(tmp_path):
    """A second call on the same in-tree root leaves exactly one identical marker."""
    plan: dict[str, Any] = {"sdk": {}}
    first = artifacts_module.sdk_plan_with_artifact_directory(tmp_path, plan)
    root = tmp_path / first["sdk"]["artifacts_dir_relative"]
    marker = root / artifacts_module.SDK_ARTIFACT_DIRECTORY_MARKER
    assert marker.is_file()
    original = marker.read_text()

    second = artifacts_module.sdk_plan_with_artifact_directory(tmp_path, first)
    assert second["sdk"]["artifacts_dir_relative"] == first["sdk"]["artifacts_dir_relative"]
    assert sorted(p.name for p in root.iterdir()) == [artifacts_module.SDK_ARTIFACT_DIRECTORY_MARKER]
    assert marker.read_text() == original


def test_sdk_plan_with_artifact_directory_does_not_mark_an_external_root(tmp_path):
    """An external (absolute, out-of-tree) SDK artifact directory belongs to the
    SDK, not this runner, and must not be written into.
    """
    external = tmp_path / "external-sdk-root"
    plan = {"sdk": {"artifacts_dir": str(external)}}
    directory = tmp_path / "coverage"
    directory.mkdir()
    result = artifacts_module.sdk_plan_with_artifact_directory(directory, plan)
    assert "artifacts_dir_relative" not in result["sdk"]
    assert external.is_dir()
    assert list(external.iterdir()) == []


def test_cleanup_reports_no_cases_after_a_pre_output_run_failure(tmp_path):
    """A manifest_factory failure on the very first coverage profile leaves the
    shared run-output directory holding only an empty per-coverage
    subdirectory. Its own keep-alive marker -- independent of the per-coverage
    SDK journal root's -- must keep that directory visible to a files-only
    upload, so cleanup on the downloaded copy reports NO_CASES just like it
    does on the original host, instead of failing on an unavailable output
    directory.
    """
    artifacts_dir = tmp_path / "artifacts"
    deployment = {
        "run": "run-pre-output-failure",
        "commit": "commit-sha",
        "bucket": "bucket",
        "region": "us-west-2",
        "functions": {"c1": "arn:aws:lambda:us-west-2:123:function:fn1:$LATEST.PUBLISHED"},
        "deploymentArtifactDirectory": str(artifacts_dir),
    }
    output_root = artifacts_dir / "shared"

    def failing_factory(_deployment, _coverage, _output):
        raise RuntimeError("boom before any plan is written")

    with pytest.raises(RuntimeError, match="boom before any plan is written"):
        shared.run(deployment, output_root, manifest_factory=failing_factory)

    # The shared directory holds only its own marker and an empty "standard/".
    assert not artifacts_module.RUN_OUTPUT_MARKER.startswith(".")
    assert artifacts_module.RUN_OUTPUT_MARKER not in {"manifest.json", "cleanup.json"}
    assert (output_root / artifacts_module.RUN_OUTPUT_MARKER).is_file()
    assert sorted(p.name for p in output_root.iterdir()) == [artifacts_module.RUN_OUTPUT_MARKER, "standard"]
    assert list((output_root / "standard").iterdir()) == []

    # A retry into the same directory is, and remains, rejected: the leftover
    # empty "standard/" alone already fails the emptiness check, with or
    # without the new marker, so this behavior is unchanged by the fix.
    with pytest.raises(ValueError, match="must be empty"):
        shared.run(deployment, output_root, manifest_factory=failing_factory)

    downloaded = tmp_path / "downloaded"
    _copy_files_only(artifacts_dir, downloaded)
    # The files-only copy drops the empty "standard/" but keeps "shared/" itself.
    assert (downloaded / "shared" / artifacts_module.RUN_OUTPUT_MARKER).is_file()
    assert not (downloaded / "shared" / "standard").exists()

    downloaded_deployment = {**deployment, "deploymentArtifactDirectory": str(downloaded)}
    factory = Mock(side_effect=AssertionError("No cloud calls are needed when no case manifest was recorded"))
    assert shared.cleanup(downloaded_deployment, client_factory=factory) == 0
    factory.assert_not_called()
    report = json.loads((downloaded / "shared" / "cleanup.json").read_text())
    assert report["status"] == "NO_CASES"
