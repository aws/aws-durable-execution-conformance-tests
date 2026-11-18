"""Load the OTel and Telemetry extensions and requirement resources from built wheels only."""

from __future__ import annotations

import importlib
import sys
import tempfile
import zipfile
from importlib import metadata
from importlib.resources import files
from pathlib import Path

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

ROOT = Path(__file__).resolve().parent.parent
DIST_DIRS = (
    ROOT / "packages/aws-durable-execution-conformance-tests/dist",
    ROOT / "packages/aws-durable-execution-conformance-tests-otel/dist",
    ROOT / "packages/aws-durable-execution-conformance-tests-telemetry/dist",
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
        distributions = list(metadata.distributions(path=[target]))
        otel_distribution = next(
            distribution
            for distribution in distributions
            if distribution.metadata["Name"] == "aws-durable-execution-conformance-tests-otel"
        )
        core_requirement = next(
            requirement
            for value in otel_distribution.requires or ()
            if (requirement := Requirement(value)).name == "aws-durable-execution-conformance-tests"
        )
        otel = importlib.import_module("aws_durable_execution_conformance_tests_otel.extension")
        assert core_requirement.specifier == SpecifierSet(otel.OtelExtension.requires_core)
        assert extensions.__version__ in core_requirement.specifier
        assert "1.0.0" not in core_requirement.specifier
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
        try:
            extensions.load_extensions(config.TESTS_DIR, entry_points=entry_points, core_version="1.0.0")
        except extensions.ExtensionCompatibilityError:
            pass
        else:
            raise AssertionError("OTel extension accepted core 1.0.0 without required catalog controls")

        lmi_catalog = importlib.import_module("aws_durable_execution_conformance_tests.lmi.catalog")
        assert len(lmi_catalog.requirements("lifecycle")) == 7
        lmi_matrix = importlib.import_module("aws_durable_execution_conformance_tests.lmi.matrix")
        assert lmi_catalog.CAPABILITIES == ("lifecycle",)
        assert lmi_catalog.COVERAGE_PROFILES == ("standard",)
        for runtime in ("javascript", "java", "python"):
            assert len(lmi_matrix.required_cases(runtime, "lifecycle")) == 7
            assert set(lmi_catalog.requirements("lifecycle", runtime=runtime)) == {
                f"lmi-lifecycle-{index}" for index in range(1, 8)
            }
        for name in (
            "adapter",
            "artifacts",
            "cloud",
            "configuration",
            "deployment",
            "errors",
            "observations",
            "runner",
            "runtime",
            "scenarios",
            "service",
            "source_evidence",
            "summary",
        ):
            module = importlib.import_module(f"aws_durable_execution_conformance_tests.lmi.{name}")
            assert Path(module.__file__).is_relative_to(target)
        assert not any(name.startswith("aws_durable_execution_sdk_") for name in sys.modules)
        assert "lmi-lifecycle" not in registry.suites
        lmi_resources = files("aws_durable_execution_conformance_tests.lmi")
        assert lmi_resources.joinpath("evidence-schema.json").is_file()
        assert not lmi_resources.joinpath("lifecycle_regressions.py").is_file()
        assert not lmi_resources.joinpath("python-lifecycle.md").is_file()
        for number in range(8, 17):
            assert not lmi_resources.joinpath(f"requirements/lmi-lifecycle/lmi-lifecycle-{number}.yaml").is_file()

        callback = importlib.import_module("aws_durable_execution_conformance_tests.callback")
        action = callback.CallbackAction.from_dict(
            {"CallbackName": "target", "Operation": "success", "AfterInvocationCompleted": True}
        )
        assert action.after_invocation_completed is True
        history = importlib.import_module("aws_durable_execution_conformance_tests.history")
        expected = [{"EventId": 1, "$absent": [["ParentId"]]}]
        assert history.EventHistoryMatcher().match(expected, [{"EventId": 1}]).success
        assert not history.EventHistoryMatcher().match(expected, [{"EventId": 1, "ParentId": None}]).success
        requirements = registry.discover_requirements(
            ["otel-invocation", "otel-execution", "otel-long-running", "telemetry"],
        )

        assert {
            "otel-invocation",
            "otel-execution",
            "otel-long-running",
            "telemetry",
        } <= registry.suites.keys()
        assert set(requirements) == {
            *(f"otel-invocation-{case_number}" for case_number in range(1, 28)),
            *(f"otel-execution-{case_number}" for case_number in range(1, 28)),
            *(f"otel-long-running-{case_number}" for case_number in range(1, 5)),
            *(f"telemetry-{case_number}" for case_number in range(1, 5)),
        }
        assert all(str(case.path).startswith(str(target)) for case in requirements.values())

    print("Verified wheel-only extension discovery and OTel and Telemetry requirement loading.")


if __name__ == "__main__":
    main()
