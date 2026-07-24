// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import java.util.Map;
import software.amazon.lambda.durable.DurableContext;
import software.amazon.lambda.durable.config.StepConfig;
import software.amazon.lambda.durable.retry.RetryStrategies;

/** Terminal step failure scenario for OTel requirement otel-4. */
public final class Otel4TerminalFailure extends OtelConformanceHandler<Void> {

    @Override
    public Void handleRequest(Map<String, Object> event, DurableContext context) {
        requireScenario(event, "terminal-failure");
        return context.step(
                "otel-terminal-failure",
                Void.class,
                step -> {
                    throw new RuntimeException("Intentional terminal failure");
                },
                StepConfig.builder()
                        .retryStrategy(RetryStrategies.Presets.NO_RETRY)
                        .build());
    }
}
