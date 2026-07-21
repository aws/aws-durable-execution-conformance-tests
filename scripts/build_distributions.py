"""Build or clean every publishable project in the workspace."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECTS = (
    ROOT / "packages/aws-durable-execution-conformance-tests",
    ROOT / "packages/aws-durable-execution-conformance-tests-otel",
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean-only", action="store_true")
    args = parser.parse_args(argv)

    for project in PROJECTS:
        shutil.rmtree(project / "dist", ignore_errors=True)
        if not args.clean_only:
            environment = {
                key: value
                for key, value in os.environ.items()
                if key not in {"HATCH_ENV", "HATCH_ENV_ACTIVE", "HATCH_PROJECT"}
            }
            subprocess.run(
                ["hatch", "build", "-c"],
                cwd=project,
                check=True,
                env=environment,
            )


if __name__ == "__main__":
    main()
