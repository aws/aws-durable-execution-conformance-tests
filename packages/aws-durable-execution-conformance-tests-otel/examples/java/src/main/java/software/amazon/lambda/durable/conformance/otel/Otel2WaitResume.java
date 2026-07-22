// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import java.time.Duration;
import java.util.Map;
import software.amazon.lambda.durable.DurableContext;

/** Wait-and-resume scenario for OTel requirement otel-2. */
public final class Otel2WaitResume extends OtelConformanceHandler<String> {

    @Override
    public String handleRequest(Map<String, Object> event, DurableContext context) {
        requireScenario(event, "wait-resume");
        context.wait("otel-wait", Duration.ofSeconds(1));
        return context.step("otel-after-resume", String.class, step -> "resumed");
    }
}
