# Debugging OpenTelemetry Conformance Tests

## Known Issues

`otel-execution-19` does not produce a completed `Workflow` span. Every handler
invocation fails, so the execution plugin receives a retrying invocation
completion but no terminal workflow completion callback. Validate the retrying
`Invocation` span without requiring a `Workflow` span.
