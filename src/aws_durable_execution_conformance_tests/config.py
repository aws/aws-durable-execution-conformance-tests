# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Default configuration values for the conformance test framework."""

from importlib.resources import files
from pathlib import Path

# Project root is three levels up from this file in a source checkout:
# src/aws_durable_execution_conformance_tests/config.py -> project root
_PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
_REPOSITORY_TESTS_DIR: Path = _PROJECT_ROOT / "test-requirements"
_PACKAGE_TESTS_DIR: Path = Path(str(files("aws_durable_execution_conformance_tests").joinpath("test_requirements")))
_IN_SOURCE_CHECKOUT: bool = _REPOSITORY_TESTS_DIR.is_dir()
_RUNTIME_ROOT: Path = _PROJECT_ROOT if _IN_SOURCE_CHECKOUT else Path.cwd()

TESTS_DIR: Path = _REPOSITORY_TESTS_DIR if _IN_SOURCE_CHECKOUT else _PACKAGE_TESTS_DIR
OUTPUT_DIR: Path = _RUNTIME_ROOT / "output"
BUILD_DIR: Path = _RUNTIME_ROOT / "build"
STACK_NAME_PREFIX = "durable-execution-conformance-tests"
DEFAULT_REGION = "us-west-2"

# Async polling configuration
POLL_INTERVAL_SECONDS = 2.0
POLL_NO_PROGRESS_TIMEOUT_SECONDS = 30.0
