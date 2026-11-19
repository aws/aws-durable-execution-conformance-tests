# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Run artifacts, redaction and deterministic case namespaces."""

import hashlib
import json
import shutil
from pathlib import Path


def scrub(value):
    if isinstance(value, dict):
        return {k: "<redacted>" if k in {"CheckpointToken", "CallbackId"} else scrub(v) for k, v in value.items()}
    if isinstance(value, list):
        return [scrub(v) for v in value]
    return value


def save(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(scrub(data), indent=2, default=str) + "\n")


def clear_run_artifacts(artifacts: Path) -> None:
    """Invalidate prior run evidence before fallible build/deployment work.

    Keep the build manifest, which deploy needs to verify the packaged wheel.
    Remove the run manifest first: even if a later local deletion fails, an exit
    trap cannot use that prior run's identity to collect, release, or stop work.
    """
    if not artifacts.exists():
        return
    (artifacts / "manifest.json").unlink(missing_ok=True)
    for path in artifacts.iterdir():
        if path.name == "build.json":
            continue
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()


def case_key(case_id: str) -> str:
    return hashlib.sha256(case_id.encode()).hexdigest()[:20]
