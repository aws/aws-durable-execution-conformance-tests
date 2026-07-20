# AWS Durable Execution Conformance Tests

Language-neutral conformance requirements and a Python runner for AWS Durable
Execution SDKs.

## Workspace

This repository is a Hatch workspace for independently versioned conformance
packages:

```text
packages/
  aws-durable-execution-conformance-tests/
```

The core distribution contains the runner, reports, generic requirements, and
their unit tests. Its distribution and import names remain unchanged:

```bash
pip install aws-durable-execution-conformance-tests
```

Detailed package documentation lives in the
[core package README](packages/aws-durable-execution-conformance-tests/README.md).

## Development

Install Hatch and run workspace commands from the repository root:

```bash
hatch run test:all
hatch run test:cov
hatch run types:check
hatch fmt --check packages scripts
hatch run yaml:lint
hatch run dist:all
```

Run `hatch build` from a child package directory to build that distribution
independently.

## Running Conformance

The runner accepts a SAM template whose functions map to requirement IDs with
`TestingMetadata.TestDescription`:

```bash
hatch run validate \
  --template path/to/template.yaml \
  --language python \
  --region us-west-2 \
  --suite step \
  --report console json
```

It builds and deploys the template, invokes each mapped function, validates the
execution result and history, and emits console, JSON, or JUnit reports.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues should be reported
through the [AWS vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
