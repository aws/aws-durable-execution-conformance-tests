"""Load the OTel extension and requirement resources from built wheels only."""

from __future__ import annotations

import importlib
import sys
import tempfile
import zipfile
from importlib import metadata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIST_DIRS = (
    ROOT / "packages/aws-durable-execution-conformance-tests/dist",
    ROOT / "packages/aws-durable-execution-conformance-tests-otel/dist",
)


def main() -> None:
    wheels = [next(dist_dir.glob("*.whl")) for dist_dir in DIST_DIRS]
    with tempfile.TemporaryDirectory(prefix="conformance-wheel-smoke-") as temp:
        target = Path(temp)
        for wheel in wheels:
            with zipfile.ZipFile(wheel) as archive:
                archive.extractall(target)

        sys.path.insert(0, str(target))
        for module_name in tuple(sys.modules):
            if module_name.startswith("aws_durable_execution_conformance_tests"):
                del sys.modules[module_name]

        config = importlib.import_module("aws_durable_execution_conformance_tests.config")
        extensions = importlib.import_module("aws_durable_execution_conformance_tests.extensions")
        distributions = metadata.distributions(path=[target])
        entry_points = [
            entry_point
            for distribution in distributions
            for entry_point in distribution.entry_points
            if entry_point.group == "aws_durable_execution_conformance_tests.extensions"
        ]
        registry = extensions.load_extensions(
            config.TESTS_DIR,
            entry_points=entry_points,
        )
        requirements = registry.discover_requirements(["otel"])

        assert "otel" in registry.suites
        assert set(requirements) == {f"otel-{case_number}" for case_number in range(1, 20)}
        assert all(str(case.path).startswith(str(target)) for case in requirements.values())

    print("Verified wheel-only extension discovery and OTel requirement loading.")


if __name__ == "__main__":
    main()
