# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Run artifacts, redaction and deterministic case namespaces."""

import json
import os
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .catalog import COVERAGE_PROFILES
from .errors import ProvisioningError
from .rules.controls import case_key as case_key


def scrub(value):
    if isinstance(value, dict):
        return {k: "<redacted>" if k in {"CheckpointToken", "CallbackId"} else scrub(v) for k, v in value.items()}
    if isinstance(value, list):
        return [scrub(v) for v in value]
    return value


def save(path, data, *, exclusive=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(scrub(data), stream, indent=2, default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if exclusive:
            os.link(temporary, path)
        else:
            temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def clear_run_artifacts(artifacts: Path, *, preserve: frozenset[str] = frozenset()) -> Path | None:
    """Archive prior evidence and invalidate the active run before deployment."""
    if not artifacts.exists():
        return None
    entries = sorted(
        (path for path in artifacts.iterdir() if path.name not in {"build.json", *preserve}),
        key=lambda path: path.name != "manifest.json",
    )
    if not entries:
        return None
    archive = Path(tempfile.mkdtemp(prefix=f"{artifacts.name}-previous-", dir=artifacts.parent))
    for path in entries:
        # Rename without following symlinks. The first rename makes the old
        # manifest unavailable to always-run cleanup even if later moves fail.
        path.replace(archive / path.name)
    return archive


def case_output_directory(artifacts: Path, run: str, *, require_available: bool = False) -> Path:
    pointer = artifacts / "case-output.json"
    if pointer.is_file():
        record = json.loads(pointer.read_text())
        if record.get("runId") == run and isinstance(directory := record.get("directory"), str) and directory:
            destination = artifacts / directory
            if require_available and not destination.is_dir():
                raise ValueError(f"Recorded case-output directory is unavailable: {destination}")
            return destination
    return artifacts / "shared"


def sdk_artifact_directory(coverage_directory: Path, plan: Mapping[str, Any], *, live: bool = False) -> Path:
    """Use the binding's journal root, with a fallback for older saved plans."""
    sdk = plan.get("sdk", {})
    if not isinstance(sdk, Mapping):
        raise ValueError("Manifest has an invalid SDK binding")
    if not live and "artifacts_dir_relative" in sdk:
        relative = sdk["artifacts_dir_relative"]
        if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
            raise ValueError("Saved SDK artifact directory hint is not relative")
        base = coverage_directory.resolve()
        target = (base / relative).resolve()
        if not target.is_relative_to(base):
            raise ValueError("Saved SDK artifact directory escapes its manifest directory")
        if not target.is_dir():
            raise ValueError("Saved in-tree SDK artifact directory is unavailable")
        return target
    if "artifacts_dir" not in sdk:
        return (coverage_directory / "source").resolve()
    root = sdk["artifacts_dir"]
    if not isinstance(root, str) or not root:
        raise ValueError("Manifest has an invalid SDK artifact directory")
    path = Path(root)
    if not live and not path.is_absolute():
        raise ValueError("Saved SDK artifact directory is relative; its original location is unresolved")
    path = path.resolve()
    if not live and not path.is_dir():
        raise ValueError("Saved external SDK artifact directory is unavailable")
    return path


def saved_case_plans(directory: Path, run_id: str) -> list[tuple[Path, dict[str, Any]]]:
    """Read direct CLI output or coverage-partitioned shared runner output."""
    direct = directory / "manifest.json"
    paths = [direct] if direct.is_file() else [directory / profile / "manifest.json" for profile in COVERAGE_PROFILES]
    plans = []
    for path in paths:
        if not path.is_file():
            continue
        plan = json.loads(path.read_text())
        if not isinstance(plan, dict) or plan.get("runId") != run_id:
            raise ValueError(f"Saved case manifest belongs to another run: {path}")
        plans.append((path.parent, plan))
    return plans


def owner_lock_path(journal: Path) -> Path:
    return journal.with_name(journal.name + ".lock")


@contextmanager
def owner_lock(journal: Path) -> Iterator[dict[str, str | int | None]]:
    """Coordinate same-root writers and recovery on a stable POSIX inode."""
    try:
        import fcntl
    except ImportError as error:
        raise ProvisioningError("Journal ownership requires POSIX file locks") from error
    path = owner_lock_path(journal)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ProvisioningError(f"Journal ownership is busy: {journal}") from error
        identity = os.fstat(lock.fileno())
        try:
            boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        except OSError:
            boot = None
        # Closing the handle releases the lock. Never remove its inode.
        yield {"bootId": boot, "device": identity.st_dev, "inode": identity.st_ino, "root": str(path.parent.resolve())}


def same_owner_lock(record: Mapping[str, Any], owner: Mapping[str, Any]) -> bool:
    return bool(owner.get("bootId")) and record.get("owner") == owner
