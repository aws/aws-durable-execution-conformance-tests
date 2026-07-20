"""Verify package archives contain their metadata and runtime resources."""

from __future__ import annotations

import email
import sys
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "aws-durable-execution-conformance-tests"
DIST_DIR = ROOT / "packages/aws-durable-execution-conformance-tests/dist"
WHEEL_REQUIREMENT = "aws_durable_execution_conformance_tests/test_requirements/step/1-1.yaml"
SDIST_REQUIREMENT = "test-requirements/step/1-1.yaml"


def _wheel_names(path: Path) -> tuple[set[str], str]:
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = archive.read(metadata_name).decode()
    return names, metadata


def _sdist_names(path: Path) -> set[str]:
    with tarfile.open(path) as archive:
        return {member.name for member in archive.getmembers()}


def main() -> None:
    errors: list[str] = []
    wheels = sorted(DIST_DIR.glob("*.whl"))
    sdists = sorted(DIST_DIR.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        errors.append(f"{PROJECT}: expected one wheel and one sdist")
    else:
        wheel_names, metadata_text = _wheel_names(wheels[0])
        metadata = email.message_from_string(metadata_text)
        if metadata["Name"] != PROJECT:
            errors.append(f"{PROJECT}: wheel metadata name is {metadata['Name']!r}")
        for suffix in ("LICENSE", "NOTICE", WHEEL_REQUIREMENT):
            if not any(name.endswith(suffix) for name in wheel_names):
                errors.append(f"{PROJECT}: wheel is missing {suffix}")

        sdist_names = _sdist_names(sdists[0])
        for suffix in ("LICENSE", "NOTICE", SDIST_REQUIREMENT):
            if not any(name.endswith(suffix) for name in sdist_names):
                errors.append(f"{PROJECT}: sdist is missing {suffix}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    print("Verified core wheel/sdist metadata and resources.")


if __name__ == "__main__":
    main()
