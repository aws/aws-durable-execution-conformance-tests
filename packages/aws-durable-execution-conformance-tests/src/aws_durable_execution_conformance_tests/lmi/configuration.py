# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Identity of the actual runtime artifact and deployment configuration."""

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .runtime import MEMORY_GIB_PER_VCPU, RuntimeDefinition


def configuration_identity(deployment: Mapping[str, Any], key: str) -> str:
    """Hash the actual deployed configuration, including the recorded fixture binding.

    The runtime model is derived from the deployment manifest itself
    (``RuntimeDefinition.from_manifest``), never passed in separately, so the
    identity always reflects what was actually recorded at ``deploy()`` time.
    """
    runtime = RuntimeDefinition.from_manifest(deployment)
    fields = (
        "runtime",
        "role",
        "bucket",
        "commit",
        "run",
        "provider",
        "codeSha256",
        "invocationTimeout",
        "executionTimeout",
    )
    configuration = {name: deployment[name] for name in fields}
    configuration.update(
        function=deployment["functions"][key],
        nativeConcurrency=deployment["concurrencies"][key],
        architecture=runtime.architecture,
        memorySize=runtime.memory_size,
        memoryGiBPerVCpu=MEMORY_GIB_PER_VCPU,
        handler=runtime.handler,
        scaling={"MinExecutionEnvironments": 1, "MaxExecutionEnvironments": 1},
        fixtureBinding=runtime.binding.to_manifest(),
    )
    return "sha256:" + hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest()
