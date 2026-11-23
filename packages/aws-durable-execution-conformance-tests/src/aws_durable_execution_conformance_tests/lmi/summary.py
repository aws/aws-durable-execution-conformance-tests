"""Summarize the shared lifecycle report without inventing cloud acceptance."""

import json
from pathlib import Path

from .artifacts import case_key, case_output_directory, saved_case_plans


def summarize(directory, *, expected_run: str | None = None):
    directory = Path(directory)
    manifest = directory / "manifest.json"
    config = json.loads(manifest.read_text()) if manifest.exists() else None
    lines = ["Shared LMI lifecycle conformance."]
    if expected_run is not None and (config is None or config.get("run") != expected_run):
        lines.append("Deployment identity does not match the requested run. Current cloud acceptance is unavailable.")
        receipt = directory / "cleanup" / case_key(expected_run) / "cleanup.json"
        cleanup = json.loads(receipt.read_text()) if receipt.is_file() else {}
        if cleanup.get("runId") == expected_run and cleanup.get("status") == "NO_CASES":
            lines.append("No scenario work was recorded for cleanup.")
        else:
            lines.append("Run cleanup not confirmed; persistent resources are retained.")
        return "\n".join(lines)
    if config:
        lines.append(
            f"Commit `{config['commit']}`; {config['runtime']}; native concurrency {list(config['concurrencies'].values())}."
        )
    shared = case_output_directory(directory, config["run"]) if config else directory / "shared"
    wrong_plan = False
    report_root = shared / "standard"
    if config:
        try:
            plans = saved_case_plans(shared, config["run"])
        except ValueError:
            wrong_plan = True
        else:
            if plans:
                report_root = plans[0][0]
    path = report_root / "report.json"
    report = json.loads(path.read_text()) if not wrong_plan and path.is_file() else None
    if wrong_plan:
        lines.append("Case manifest does not match the requested run. Current cloud acceptance is unavailable.")
    elif report and config is None:
        lines.append("Deployment identity is missing; current cloud acceptance is unavailable.")
    elif report and config and report.get("run", {}).get("name") != config["run"]:
        lines.append("Report belongs to another run; current cloud acceptance is unavailable.")
    elif report and report.get("results"):
        lines.extend(f"- {case['id']}: {case['status']}" for case in report["results"])
        lines.append(f"Shared runner exit code: {report.get('ci', {}).get('exit_code', 'unknown')}.")
    else:
        lines.append("Cloud scenarios did not run. Inspect provisioning/build diagnostics.")
    for path in sorted(directory.glob("*-error.json")):
        data = json.loads(path.read_text())
        lines.append(f"- {path.stem}: {data['type']}")
    path = shared / "cleanup.json"
    cleanup = json.loads(path.read_text()) if path.exists() else {}
    if config is None or cleanup.get("runId") != config["run"]:
        lines.append("Run cleanup not confirmed; persistent resources are retained.")
    elif cleanup.get("status") == "NO_CASES":
        lines.append("No scenario work was recorded for cleanup.")
    elif cleanup.get("status") == "CLEANUP_REQUESTED" and cleanup.get("errors") == []:
        lines.append("Run-scoped cleanup requests completed; see case reports for task retirement.")
    else:
        lines.append("Run cleanup failed or is incomplete; inspect cleanup diagnostics.")
    return "\n\n".join(lines)
