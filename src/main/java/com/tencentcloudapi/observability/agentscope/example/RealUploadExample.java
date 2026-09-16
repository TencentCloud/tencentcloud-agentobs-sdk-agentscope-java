package com.tencentcloudapi.observability.agentscope.example;

import com.tencentcloudapi.observability.agentscope.ClsObservability;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

/**
 * Live upload demo — sends spans to a real CLS OTLP/HTTP endpoint.
 *
 * <p>Unlike {@link LoggingTracingExample} (which only prints to stdout), this installs
 * the real {@link ClsObservability} pipeline: it resolves CLS credentials from env vars
 * or a {@code .env} file, builds the OTLP/HTTP exporter, emits a span tree with the exact
 * shape the v2 {@code ClsTracingMiddleware} produces, then flushes so the batch is actually
 * uploaded before the JVM exits.
 *
 * <p><b>Span tree (CLS GenAI Trace spec, per-turn trace model).</b> This hand-builds the same
 * hierarchy the middleware emits for one turn with one ReAct round:
 * <pre>
 *   entry (SERVER, enter_application)
 *   └─ agent (INTERNAL, invoke_agent)
 *      └─ step (INTERNAL, react, round 1)
 *         ├─ chat (CLIENT)          — the model call
 *         └─ tool (CLIENT, execute_tool)  — same ReAct round ⇒ shares the step parent
 * </pre>
 * All spans carry the spec's dotted {@code gen_ai.*} attributes so the CLS OTLP→CLS
 * conversion promotes them to first-class columns, and the ID chain follows
 * {@code session.id → turn.id ({sessionId}:t{N}) → step.id ({turnId}:s{N})}.
 *
 * <p><b>Required config</b> (env vars or {@code ./.env} — never hard-coded):
 * <pre>
 *   CLS_ENDPOINT     e.g. ap-guangzhou.cls.tencentcs.com
 *   CLS_TOPIC_ID     the trace topic id
 *   CLS_SECRET_ID    Tencent Cloud SecretId
 *   CLS_SECRET_KEY   Tencent Cloud SecretKey
 *   CLS_SERVICE_NAME (optional) service.name, defaults to agentscope-app
 * </pre>
 *
 * <p>Run:
 * <pre>{@code
 * mvn -q exec:java \
 *   -Dexec.mainClass="com.tencentcloudapi.observability.agentscope.example.RealUploadExample"
 * }</pre>
 */
public final class RealUploadExample {

    private RealUploadExample() {
    }

    public static void main(String[] args) {
        // Resolves CLS_* from env / .env; throws with a clear message if anything is missing.
        ClsObservability obs = ClsObservability.install();
        Tracer tracer = obs.tracer();

        String sessionId = "session-" + System.currentTimeMillis();
        String userId = "user-demo";
        String agentName = "assistant";

        // ID chain per spec: session.id → turn.id → step.id
        String turnId = sessionId + ":t1";
        String stepId = turnId + ":s1";

        // Message payloads in the spec's [{role, parts:[{type, content}]}] shape.
        String inputMessages =
                "[{\"role\":\"user\",\"parts\":[{\"type\":\"text\","
                + "\"content\":\"What's the weather in Beijing?\"}]}]";
        String finalAnswer = "The weather in Beijing is sunny, around 22\u00B0C.";
        String outputMessages =
                "[{\"role\":\"assistant\",\"parts\":[{\"type\":\"text\","
                + "\"content\":\"" + finalAnswer + "\"}]}]";

        System.out.println("Emitting demo spans (session=" + sessionId + ") ...");

        // ── entry span (turn root, SERVER) ──
        Span entry = tracer.spanBuilder("enter_application")
                .setSpanKind(SpanKind.SERVER).startSpan();
        entry.setAttribute("gen_ai.span.kind", "entry");
        entry.setAttribute("gen_ai.operation.name", "enter_application");
        entry.setAttribute("gen_ai.session.id", sessionId);
        entry.setAttribute("gen_ai.turn.id", turnId);
        entry.setAttribute("gen_ai.user.id", userId);
        entry.setAttribute("gen_ai.input.messages", inputMessages);
        Context entryCtx = Context.current().with(entry);

        try {
            // ── agent span (INTERNAL, invoke_agent) ──
            Span agentSpan = tracer.spanBuilder("invoke_agent " + agentName)
                    .setParent(entryCtx)
                    .setSpanKind(SpanKind.INTERNAL).startSpan();
            agentSpan.setAttribute("gen_ai.span.kind", "agent");
            agentSpan.setAttribute("gen_ai.operation.name", "invoke_agent");
            agentSpan.setAttribute("gen_ai.session.id", sessionId);
            agentSpan.setAttribute("gen_ai.turn.id", turnId);
            agentSpan.setAttribute("gen_ai.agent.type", "agentscope");
            agentSpan.setAttribute("gen_ai.agent.name", agentName);
            agentSpan.setAttribute("gen_ai.user.id", userId);
            agentSpan.setAttribute("gen_ai.input.messages", inputMessages);
            Context agentCtx = entryCtx.with(agentSpan);

            try {
                // ── step span (INTERNAL, react, round 1) — wraps chat + tool of this round ──
                Span stepSpan = tracer.spanBuilder("react round_1")
                        .setParent(agentCtx)
                        .setSpanKind(SpanKind.INTERNAL).startSpan();
                stepSpan.setAttribute("gen_ai.span.kind", "step");
                stepSpan.setAttribute("gen_ai.operation.name", "react");
                stepSpan.setAttribute("gen_ai.session.id", sessionId);
                stepSpan.setAttribute("gen_ai.turn.id", turnId);
                stepSpan.setAttribute("gen_ai.step.id", stepId);
                stepSpan.setAttribute("gen_ai.react.round", 1L);
                stepSpan.setAttribute("gen_ai.react.finish_reason", "tool_calls");
                Context stepCtx = agentCtx.with(stepSpan);

                try {
                    // ── chat span (CLIENT) — the model call, child of step ──
                    Span chatSpan = tracer.spanBuilder("chat gpt-4o")
                            .setParent(stepCtx)
                            .setSpanKind(SpanKind.CLIENT).startSpan();
                    try {
                        chatSpan.setAttribute("gen_ai.span.kind", "chat");
                        chatSpan.setAttribute("gen_ai.operation.name", "chat");
                        chatSpan.setAttribute("gen_ai.session.id", sessionId);
                        chatSpan.setAttribute("gen_ai.turn.id", turnId);
                        chatSpan.setAttribute("gen_ai.step.id", stepId);
                        chatSpan.setAttribute("gen_ai.react.round", 1L);
                        chatSpan.setAttribute("gen_ai.request.model", "gpt-4o");
                        chatSpan.setAttribute("gen_ai.usage.input_tokens", 42L);
                        chatSpan.setAttribute("gen_ai.usage.output_tokens", 17L);
                        chatSpan.setAttribute("gen_ai.usage.total_tokens", 59L);
                        chatSpan.setAttribute("gen_ai.usage.cache_read.input_tokens", 0L);
                        chatSpan.setAttribute("gen_ai.response.finish_reasons", "[\"tool_calls\"]");
                        chatSpan.setAttribute("gen_ai.output.messages",
                                "[{\"role\":\"assistant\",\"parts\":["
                                + "{\"type\":\"thinking\",\"content\":\"I should call the weather "
                                + "tool for Beijing.\"}]}]");
                    } finally {
                        chatSpan.end();
                    }

                    // ── tool span (CLIENT, execute_tool) — same ReAct round ⇒ shares step ──
                    Span toolSpan = tracer.spanBuilder("execute_tool get_weather")
                            .setParent(stepCtx)
                            .setSpanKind(SpanKind.CLIENT).startSpan();
                    try {
                        toolSpan.setAttribute("gen_ai.span.kind", "tool");
                        toolSpan.setAttribute("gen_ai.operation.name", "execute_tool");
                        toolSpan.setAttribute("gen_ai.session.id", sessionId);
                        toolSpan.setAttribute("gen_ai.turn.id", turnId);
                        toolSpan.setAttribute("gen_ai.step.id", stepId);
                        toolSpan.setAttribute("gen_ai.user.id", userId);
                        toolSpan.setAttribute("gen_ai.tool.name", "get_weather");
                        toolSpan.setAttribute("gen_ai.tool.type", "function");
                        toolSpan.setAttribute("gen_ai.tool.call.id", "call_abc123");
                        toolSpan.setAttribute("gen_ai.tool.call.arguments", "{\"city\":\"Beijing\"}");
                        toolSpan.setAttribute("gen_ai.tool.call.result",
                                "{\"temp\":\"22C\",\"condition\":\"sunny\"}");
                    } finally {
                        toolSpan.end();
                    }
                } finally {
                    stepSpan.end();
                }

                // agent-level aggregates (mirrors the middleware's doFinally)
                agentSpan.setAttribute("gen_ai.agent.message_count", 1L);
                agentSpan.setAttribute("gen_ai.agent.tool_call_count", 1L);
                agentSpan.setAttribute("gen_ai.usage.input_tokens", 42L);
                agentSpan.setAttribute("gen_ai.usage.output_tokens", 17L);
                agentSpan.setAttribute("gen_ai.usage.total_tokens", 59L);
                agentSpan.setAttribute("gen_ai.usage.cache_read.input_tokens", 0L);
                agentSpan.setAttribute("gen_ai.finish_reason", "normal");
                agentSpan.setAttribute("gen_ai.turn.completed", true);
                agentSpan.setAttribute("gen_ai.output.messages", outputMessages);
            } finally {
                agentSpan.end();
            }

            entry.setAttribute("gen_ai.finish_reason", "normal");
            entry.setAttribute("gen_ai.turn.completed", true);
            entry.setAttribute("gen_ai.output.messages", outputMessages);
        } finally {
            entry.end();
        }

        // Force the batch out before we exit, and report the result.
        System.out.println("Flushing to CLS ...");
        obs.flush();
        obs.shutdown();
        System.out.println("Done. If no OTLP error was logged above, the spans reached CLS. "
                + "Query them in the CLS trace topic (session=" + sessionId + ").");
    }
}
