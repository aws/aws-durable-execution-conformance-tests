# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Run artifacts, redaction and deterministic case namespaces."""

import errno
import json
import os
import subprocess
import sys
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


def _sync_directory(path: Path) -> None:
    """Persist a directory's entries, such as a newly created or replaced file.

    Shared with ``app.py`` (imported from there), so this is the single place
    that tolerates filesystems that reject a directory fsync outright.
    """
    if os.name == "nt":
        return  # Windows cannot open a directory for fsync.
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    except OSError as exc:
        # Some filesystems cannot fsync a directory at all; nothing more is possible.
        if exc.errno not in (errno.EINVAL, errno.EBADF, errno.ENOTSUP, errno.EOPNOTSUPP):
            raise
    finally:
        os.close(descriptor)


def save(path, data, *, exclusive=False):
    path = Path(path)
    parent = path.parent
    # Only the ancestors this call actually creates need a fresh parent sync;
    # an already-existing directory's entry is already durable.
    created = [directory for directory in (parent, *parent.parents) if not directory.exists()]
    parent.mkdir(parents=True, exist_ok=True)
    for directory in created:
        if directory.parent != directory:
            _sync_directory(directory.parent)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=parent, delete=False) as stream:
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
    # The name is in place for either mode now: fsync the directory so a host
    # crash cannot leave the new or replaced entry missing or stale.
    _sync_directory(parent)


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


def _write_keep_alive_marker(root: Path, name: str, kind: str) -> None:
    """Idempotently write a small, visible keep-alive file into ``root``.

    ``name`` must not start with a dot: a files-only artifact upload (e.g.
    ``actions/upload-artifact``) uploads only files, skips hidden ones, and
    so drops a directory that ends up holding none. Writing a marker like
    this into an otherwise-empty, run-owned directory keeps it visible to
    such an upload. ``save`` already makes the write atomic and repeat
    calls with the same ``kind`` idempotent.
    """
    save(root / name, {"kind": kind})


# Written into an in-tree SDK journal root right after it is created or
# ensured, so a files-only artifact upload still keeps the directory when
# no case ever ran and nothing else was written into it. Never written into
# an external (absolute, out-of-tree) SDK artifact directory, which belongs
# to the SDK, not this runner.
SDK_ARTIFACT_DIRECTORY_MARKER = "sdk-artifacts.json"


def _mark_in_tree_sdk_artifact_directory(root: Path) -> None:
    _write_keep_alive_marker(root, SDK_ARTIFACT_DIRECTORY_MARKER, "lmi-sdk-artifacts")


# Written into the shared run-output directory right after its emptiness is
# confirmed and before anything else (including the deployment's
# case-output.json pointer) is saved, so a manifest_factory or revision
# failure on the very first coverage profile -- which otherwise leaves this
# directory holding only an empty per-coverage subdirectory -- still keeps
# it visible to a files-only upload. Must not collide with "manifest.json"
# (saved_case_plans' direct-CLI case) or "cleanup.json" (read by summary).
RUN_OUTPUT_MARKER = "run-output.json"


def mark_run_output_directory(root: Path) -> None:
    _write_keep_alive_marker(root, RUN_OUTPUT_MARKER, "lmi-run-output")


def sdk_plan_with_artifact_directory(directory: Path, plan: Mapping[str, Any]) -> dict[str, Any]:
    root = sdk_artifact_directory(directory, plan, live=True)
    root.mkdir(parents=True, exist_ok=True)
    sdk = {**plan.get("sdk", {}), "artifacts_dir": str(root)}
    sdk.pop("artifacts_dir_relative", None)
    if root.is_relative_to(base := directory.resolve()):
        sdk["artifacts_dir_relative"] = str(root.relative_to(base))
        _mark_in_tree_sdk_artifact_directory(root)
    return {**plan, "sdk": sdk}


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


# Linux exposes a stable per-boot identifier at this fixed path. Read as a
# module-level constant (rather than inlined) so tests can point it at a
# fixture file without touching the real host's /proc.
_LINUX_BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")


def _macos_boot_session_uuid() -> str | None:
    """Return macOS's `kern.bootsessionuuid`, or None if it cannot be read.

    This value is assigned once per boot and is stable across the whole
    session. Deliberately not `kern.boottime`: that changes whenever the
    system clock is adjusted, which would make a live boot look foreign to
    itself and defeat same-boot recovery. Uses the `sysctl` CLI rather than
    a ctypes binding to libc, to keep this simple; any failure (missing
    binary, non-zero exit, timeout) or empty output yields None.
    """
    try:
        result = subprocess.run(
            ["/usr/sbin/sysctl", "-n", "kern.bootsessionuuid"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
    except Exception:
        return None
    return result.stdout.strip() or None


def _boot_identity() -> str | None:
    """Return a stable identifier for the current boot, or None if unavailable.

    This binds a lock holder's identity to "this boot" so that a stale
    recovery record left by an earlier boot -- where PIDs and inodes can be
    reused -- is detectable as foreign rather than silently treated as our
    own. Supported on Linux (boot_id) and macOS (a boot-session sysctl); any
    other platform, or any failure reading the value, yields None.
    """
    try:
        if sys.platform.startswith("linux"):
            return _LINUX_BOOT_ID_PATH.read_text().strip() or None
        if sys.platform == "darwin":
            return _macos_boot_session_uuid()
        return None
    except OSError:
        return None


@contextmanager
def owner_lock(journal: Path, *, require_boot_identity: bool = True) -> Iterator[dict[str, str | int | None]]:
    """Coordinate same-root writers and recovery on a stable POSIX inode.

    A write that might need recovery later (``require_boot_identity=True``,
    the default) cannot start without a stable boot identity: otherwise a
    future recovery attempt could never prove it is the same host and would
    refuse the record forever. A pure recovery/cleanup attempt
    (``require_boot_identity=False``) does not know in advance whether the
    record it is about to read actually needs that proof -- a journal that
    already settled, or a profile fence that is no longer being acquired,
    needs only the flock below. Pass ``False`` there and let
    ``same_owner_lock``'s falsy check (defense in depth) refuse any record
    that turns out to still need an owner we cannot establish.
    """
    try:
        import fcntl
    except ImportError as error:
        raise ProvisioningError("Journal ownership requires POSIX file locks") from error
    boot = _boot_identity()
    if boot is None and require_boot_identity:
        raise ProvisioningError(
            "Journal ownership requires a stable boot identity for automatic recovery "
            "(supported on Linux and macOS); none is available on this host"
        )
    path = owner_lock_path(journal)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ProvisioningError(f"Journal ownership is busy: {journal}") from error
        identity = os.fstat(lock.fileno())
        # Closing the handle releases the lock. Never remove its inode.
        yield {"bootId": boot, "device": identity.st_dev, "inode": identity.st_ino, "root": str(path.parent.resolve())}


def same_owner_lock(record: Mapping[str, Any], owner: Mapping[str, Any]) -> bool:
    return bool(owner.get("bootId")) and record.get("owner") == owner
