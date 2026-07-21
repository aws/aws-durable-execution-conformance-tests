# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""OpenTelemetry backend adapters organized by provider."""

from aws_durable_execution_conformance_tests_otel.backends._common import (
    HttpClient,
    JsonHttpClient,
)
from aws_durable_execution_conformance_tests_otel.backends.collector import (
    CollectorBackend,
    CollectorBackendFactory,
)
from aws_durable_execution_conformance_tests_otel.backends.dash0 import (
    Dash0Backend,
    Dash0BackendFactory,
)
from aws_durable_execution_conformance_tests_otel.backends.datadog import (
    DatadogBackend,
    DatadogBackendFactory,
)
from aws_durable_execution_conformance_tests_otel.backends.xray import (
    XRayBackend,
    XRayBackendFactory,
)

BUILTIN_BACKENDS = {
    "xray": XRayBackendFactory,
    "datadog": DatadogBackendFactory,
    "dash0": Dash0BackendFactory,
    "collector": CollectorBackendFactory,
}

__all__ = [
    "BUILTIN_BACKENDS",
    "CollectorBackend",
    "CollectorBackendFactory",
    "Dash0Backend",
    "Dash0BackendFactory",
    "DatadogBackend",
    "DatadogBackendFactory",
    "HttpClient",
    "JsonHttpClient",
    "XRayBackend",
    "XRayBackendFactory",
]
