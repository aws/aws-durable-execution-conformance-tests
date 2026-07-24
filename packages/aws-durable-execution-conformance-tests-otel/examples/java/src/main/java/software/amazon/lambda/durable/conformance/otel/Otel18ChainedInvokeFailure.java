// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import java.util.Map;
import software.amazon.lambda.durable.DurableContext;

/** Failed chained-invoke scenario for OTel requirement otel-18. */
public final class Otel18ChainedInvokeFailure extends OtelConformanceHandler<Void> {

    @Override
    public Void handleRequest(Map<String, Object> event, DurableContext context) {
        requireScenario(event, "chained-invoke-failure");
        return context.invoke(
                "otel-failed-invoke",
                System.getenv("OTEL_INVOKE_TARGET_FUNCTION_NAME"),
                event,
                Void.class);
    }
}
