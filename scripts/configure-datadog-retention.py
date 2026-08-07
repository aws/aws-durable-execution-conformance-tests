"""Create or update the Datadog retention filter used by hosted conformance tests."""

from __future__ import annotations

import os

from aws_durable_execution_conformance_tests_otel.backends.datadog import (
    DATADOG_RETENTION_FILTER_NAME,
    configure_datadog_retention,
)


def main() -> None:
    api_key = os.environ.get("DATADOG_API_KEY")
    application_key = os.environ.get("DATADOG_APPLICATION_KEY")
    if not api_key or not application_key:
        raise SystemExit("DATADOG_API_KEY and DATADOG_APPLICATION_KEY are required")

    site = os.environ.get("DD_SITE", "datadoghq.com")
    action = configure_datadog_retention(
        f"https://api.{site}",
        api_key,
        application_key,
    )
    print(f"{action.capitalize()} Datadog retention filter {DATADOG_RETENTION_FILTER_NAME!r}")


if __name__ == "__main__":
    main()
