# aws-durable-execution-conformance-tests

Conformance test suite for the AWS Durable Execution SDK. Validates SDK test requirements against test cases by deploying Lambda functions, invoking them, and asserting execution history matches expected results.

This repository contains the test runner and language-neutral requirements.

-----

## Prerequisites

- Python 3.14+
- [Hatch](https://hatch.pypa.io/dev/install/) (build system and environment manager)
- SAM CLI configured with appropriate credentials

## Setup

```bash
cd aws-durable-execution-conformance-tests
hatch env create
```

-----

## Test Requirements

Test requirements are language-agnostic YAML specifications that define the **expected behavior** of durable execution SDKs. They live in the `test-requirements/` directory, organized into suites. A suite is a related group of requirements and is not limited to an SDK operation: suites may cover durable operations, cross-cutting capabilities such as serialization and deserialization, or integrations such as an OpenTelemetry plugin.

```
test-requirements/
├── step/                # 1-N: Step operation suite
├── wait/                # 2-N: Wait operation suite
├── child/               # 3-N: Child context suite
├── callback/            # 4-N: Callback suite
├── invoke/              # 5-N: Invoke (chained) suite
├── wait_for_condition/  # 6-N: Wait-for-condition suite
├── wait_for_callback/   # 7-N: Wait-for-callback suite
├── parallel/            # 8-N: Parallel operation suite
└── map/                 # 9-N: Map operation suite
```

Each YAML file defines a single conformance test. The naming convention is `{suite_prefix}-{number}.yaml` (e.g., `1-1.yaml` is the first step test, `4-7.yaml` is the seventh callback test).

### Placeholders and Wildcards

Test requirements use placeholders to handle values that vary between executions:

- **`'*'` (wildcard):** Matches any value. Used for timestamps and other non-deterministic fields.
- **`${ID1}`, `${ID2}`, ...:** Auto-bound ID placeholders. The validator binds these on first encounter and asserts consistency across subsequent references. For example, if `${ID1}` first matches `"abc-123"`, all later `${ID1}` references must also equal `"abc-123"`.
- **`${GEN_STR:N}`:** Generates a random alphanumeric string of length N. Used in the `Variables` section to create unique test inputs.
- **Named variables (`${VAR_NAME}`):** Defined in the `Variables` section, substituted into `Input`, `ExpectedResult`, `CallbackActions`, and `ExpectedExecutionHistory` before validation.

### Async Test Requirements

Tests for operations that suspend execution (wait, callback, invoke) include additional fields:

```yaml
AsyncInvoke: true
```

The validator handles the full lifecycle: invoke the function, wait for suspension, perform callback actions (if any), wait for completion, then assert the final execution history.

-----

## Running the Validator

The runner maps each SAM function to the requirement ID at the start of its
`Properties.FunctionName`. The prefix must be followed by a hyphen or the end
of the name; for example, `1-1-${AWS::StackName}` maps to `1-1`. Configure AWS
credentials for an account with permission to deploy and invoke the test
resources, then run:

```bash
hatch run validate \
  --template path/to/template.yaml \
  --language python \
  --region us-west-2 \
  --suite step \
  --report console
```

The validator will:

1. Discover requirement IDs from function name prefixes in the supplied template
2. Load the corresponding requirement YAML for each ID
3. Deploy and invoke the mapped Lambda functions
4. Retrieve each durable execution's result and event history
5. Validate the result and history against the requirement
6. Report the status of every selected requirement

Use `--parameter-overrides KEY=VALUE` to pass additional SAM template
parameters, such as a pre-created Lambda execution role. Explicit overrides
take precedence over values supplied by extensions.

-----

## Test Reports

The validator can emit reports in three formats via `--report` (repeatable;
defaults to `console`). Machine formats are written next to `--report-file`
(default `<history-dir>/report`), with the extension appended per format:

| Format | `--report` value | Audience | Output |
|---|---|---|---|
| Console | `console` | Human | Grouped summary printed to stdout |
| JSON | `json` | Machine | `<report-file>.json` (schema-versioned) |
| JUnit XML | `junit` | Machine → CI viewers | `<report-file>.xml` |

```bash
hatch run validate --template path/to/template.yaml \
                   --language python --suite step \
                   --report console json junit --report-file build/report
```

### Result statuses

Every requirement resolves to one status:

| Status | Meaning | Blocks the run? |
|---|---|---|
| `PASSED` | History + result matched | no |
| `FAILED` | Real mismatch or error | **yes** |
| `OPTIONAL_FAILED` | Failed, but requirement marked `optional: true` | no |
| `NOT_IMPLEMENTED` | No function name starts with the requirement ID | no |

The runner exits non-zero only when at least one requirement is `FAILED`.
`NOT_IMPLEMENTED` and `OPTIONAL_FAILED` remain visible without blocking the
run.

### JUnit details (CI correlation)

Each `<testcase>` uses `classname="{language}.{suite}"` and `name="{id}"`
(the stable key). `FAILED` maps to `<failure>`; every other non-passing status
maps to `<skipped>` so CI viewers render gaps as skipped, not failed. A
`<properties>` block plus `<system-out>` carry the correlation metadata:

```xml
<testcase classname="python.step" name="1-6" time="0.000">
  <properties>
    <property name="requirement_id" value="1-6"/>
    <property name="description"    value="Custom serdes (per-step)"/>
    <property name="language"       value="python"/>
    <property name="example"        value="StepCustomSerdes"/>
  </properties>
  <failure message="Expected Result='HELLO WORLD', got 'hello world'"/>
  <system-out>Custom serdes (per-step)</system-out>
</testcase>
```

Because `name` is the bare requirement id, JUnit files from all three SDKs can
be merged and lined up per requirement, with `classname` telling them apart.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development commands, pull-request
guidance, and rules for adding language-neutral requirement suites.

## Security

Report potential security issues through the
[AWS vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/).
Do not create a public GitHub issue for a security vulnerability.

## Code of Conduct

This project follows the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md).

## License

`aws-durable-execution-conformance-tests` is distributed under the terms of the
[Apache-2.0](https://spdx.org/licenses/Apache-2.0.html) license. See
[NOTICE](NOTICE) for additional attribution information.
