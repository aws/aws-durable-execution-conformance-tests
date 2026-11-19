# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Identity of the actual runtime artifact and deployment configuration."""

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .runtime import RuntimeDefinition


def configuration_identity(deployment: Mapping[str, Any], key: str, runtime: RuntimeDefinition) -> str:
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
        handler=runtime.handler,
        scaling={"MinExecutionEnvironments": 1, "MaxExecutionEnvironments": 1},
    )
    return "sha256:" + hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest()
