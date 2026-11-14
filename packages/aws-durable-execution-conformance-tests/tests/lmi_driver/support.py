"""Explicit test fixture policies; no language SDK is needed by the shared harness."""

from functools import partial

from aws_durable_execution_conformance_tests.lmi.configuration import configuration_identity as identity
from aws_durable_execution_conformance_tests.lmi.runtime import RuntimeDefinition
from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver as Driver
from aws_durable_execution_conformance_tests.lmi.source_evidence import build_evidence as normalize

RUNTIME = RuntimeDefinition("python", "fixture.handler")
FIXTURES = {
    "native-overlap": "barrier",
    "normal-return": "success",
    "deadline-during-step": "deadline",
    "peer-recovery": "deadline",
    "suspension-retry": "replay",
    "warm-reuse": "warm-owner",
    "normal-failure": "failure",
    "peer": "barrier",
    "recovery": "barrier",
    "warm": "warm-probe",
}


def ScenarioDriver(context, cloud, **options):
    return Driver(context, cloud, runtime="python", fixtures=FIXTURES, **options)


configuration_identity = partial(identity, runtime=RUNTIME)
_normalize = partial(
    normalize,
    runtime="python",
    expected_outcome=lambda item, scenario: {"status": "SUCCEEDED", "result": item["marker"]},
    worker_identity=lambda raw: raw["process"],
)


def build_evidence(context, cloud, **options):
    return _normalize(context, cloud, **options)
