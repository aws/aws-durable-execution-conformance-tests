# Contributing Guidelines

Thank you for your interest in contributing. Bug reports, feature requests,
conformance requirements, runner improvements, corrections, and documentation
improvements are welcome.

Please read this guide before opening an issue or pull request so maintainers
have the information needed to review your contribution.

## Security issue notifications

If you discover a potential security issue, notify AWS Security through the
[AWS vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/).
Do not create a public GitHub issue for a security vulnerability.

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting bugs and requesting features

Use the GitHub issue tracker to report bugs, request features, or suggest
documentation improvements. Before filing an issue, check open and recently
closed issues for an existing report.

For bugs, include:

- Reproduction steps or a minimal test case
- The testing-framework version or commit
- The SDK language and version under test
- The relevant requirement ID, suite, and report output
- Any environment details needed to reproduce the behavior

## Repository structure

- `src/aws_durable_execution_sdk_testing/`: validator and report implementation
- `test-requirements/`: language-neutral requirement suites
- `tests/`: unit tests for the Python runner

SDK-specific test handlers and deployment templates live in their respective SDK
repositories and are not released as part of this repository.

## Development setup

Install [Hatch](https://hatch.pypa.io/dev/install/), then run commands from the
repository root:

```bash
hatch env create
hatch run test:run
hatch run test:cov
hatch run types:check
hatch fmt --check src/aws_durable_execution_sdk_testing
hatch build -c
```

To validate requirement YAML files:

```bash
hatch run yaml:lint
```

See the [README](README.md) for instructions on running the conformance suite
against an SDK implementation.

## Requirement-suite contributions

A suite is a related group of requirements. It may cover an SDK operation, a
cross-cutting capability such as serialization and deserialization, or an
integration such as an OpenTelemetry plugin. Each new suite must have its own
dedicated `test-requirements/<suite>/` directory. Matching test handlers and
deployment templates must be added to each applicable SDK repository through
that repository's contribution process.

Keep every requirement language-neutral. Test cases must exercise each SDK's
real public API. Do not replace a missing or defective SDK capability with
hand-written logic that forces a test to pass. A failing test that exposes an
SDK incompatibility is useful evidence and should remain visible in the report.

Declare genuinely missing SDK capabilities as `NOT_IMPLEMENTED` with a clear
reason rather than silently excluding the requirement.

## Contributing through pull requests

Before opening a pull request:

1. Work from the latest `main` branch.
2. Keep the change focused and avoid unrelated reformatting.
3. Add or update tests and documentation as appropriate.
4. Run the development checks listed above.
5. Explain user-visible behavior changes and known SDK divergences.
6. Respond to CI failures and review feedback.

For significant behavior or schema changes, open an issue first so the design
can be discussed before implementation.

## Licensing

See [LICENSE](LICENSE) for the project's license. You will be asked to confirm
the licensing of your contribution.
