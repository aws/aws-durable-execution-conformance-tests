// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import java.util.Map;
import software.amazon.lambda.durable.DurableContext;

/** Chained-invoke scenario for OTel requirement otel-11. */
public final class Otel11ChainedInvoke extends OtelConformanceHandler<Map<String, Object>> {

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, DurableContext context) {
        requireScenario(event, "chained-invoke");
        return context.invoke(
                "otel-invoke",
                System.getenv("OTEL_INVOKE_TARGET_FUNCTION_NAME"),
                event,
                new software.amazon.lambda.durable.TypeToken<Map<String, Object>>() {});
    }
}
