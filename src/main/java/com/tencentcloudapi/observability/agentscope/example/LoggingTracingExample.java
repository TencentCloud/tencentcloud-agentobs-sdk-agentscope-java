package com.tencentcloudapi.observability.agentscope.example;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Offline demo — no live CLS endpoint required.
 *
 * <p>It stands up an OpenTelemetry tracer whose exporter simply prints each finished span's
 * attributes to stdout — exactly the payload the OTLP/HTTP exporter would ship to CLS, with no
 * intermediate conversion. It hand-builds the <b>same span tree and the same
 * {@code gen_ai.*} attribute keys</b> that {@code ClsTracingMiddleware} emits, so you can eyeball
 * how session / turn / step ids, token usage, tool arguments/results, thoughts and the
 * <b>HITL finish reason</b> travel over OTLP — without running a real agent.
 *
 * <p>Two turns are emitted to show both the normal and the parked shapes:
 * <pre>
 *   TURN 1 (normal completion, one ReAct round with a tool call)
 *   entry (SERVER, enter_application)
 *   └─ agent (INTERNAL, invoke_agent)          finish_reason=normal, turn.completed=true
 *      └─ step (INTERNAL, react, round 1)
 *         ├─ chat (CLIENT)                      finish_reasons=["tool_calls"]
 *         └─ tool (CLIENT, execute_tool)        same round ⇒ shares the step parent
 *
 *   TURN 2 (HITL: parked awaiting user confirmation — no tool span, turn not completed)
 *   entry (SERVER, enter_application)
 *   └─ agent (INTERNAL, invoke_agent)          finish_reason=awaiting_user_confirm,
 *      └─ step (INTERNAL, react, round 1)      turn.completed=false, hitl.event=require_user_confirm
 *         └─ chat (CLIENT)                      finish_reasons=["tool_calls"]
 * </pre>
 * The ID chain follows {@code session.id → turn.id ({sessionId}:t{N}) → step.id ({turnId}:s{N})}.
 *
 * <p>Run:
 * <pre>{@code
 * mvn -q exec:java \
 *   -Dexec.mainClass="com.tencentcloudapi.observability.agentscope.example.LoggingTracingExample"
 * }</pre>
 */
public final class LoggingTracingExample {

    private static final String AGENT_NAME = "assistant";
    private static final String SESSION_ID = "session-1234";
    private static final String USER_ID = "user-5678";

    private static final String INPUT_MESSAGES =
            "[{\"role\":\"user\",\"parts\":[{\"type\":\"text\","
            + "\"content\":\"What's the weather in Beijing?\"}]}]";

    private LoggingTracingExample() {
    }

    public static void main(String[] args) {
        SpanExporter printingExporter = new SpanExporter() {
            @Override
            public CompletableResultCode export(Collection<SpanData> spans) {
                for (SpanData span : spans) {
                    System.out.println("──────────────────────────────────────────");
                    System.out.println("span: " + span.getName()
                            + "  (kind=" + span.getKind() + ")");
                    System.out.println("  traceId=" + span.getTraceId()
                            + " spanId=" + span.getSpanId()
                            + " parentSpanId=" + span.getParentSpanId());
                    span.getAttributes().forEach((key, value) ->
                            System.out.printf("  %-40s = %s%n", key.getKey(), value));
                }
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode flush() {
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode shutdown() {
                return CompletableResultCode.ofSuccess();
            }
        };

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(printingExporter))
                .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        Tracer tracer = sdk.getTracer("demo");

        System.out.println("### TURN 1 — normal completion (chat + tool) ###");
        emitNormalTurn(tracer);

        System.out.println();
        System.out.println("### TURN 2 — HITL: parked awaiting user confirmation ###");
        emitHitlTurn(tracer);

        tracerProvider.forceFlush().join(5, TimeUnit.SECONDS);
        tracerProvider.close();
        System.out.println("──────────────────────────────────────────");
        System.out.println("Done. The attributes above are exactly what the OTLP exporter uploads to CLS.");
    }

    /** TURN 1: one ReAct round that calls a tool and finishes normally. */
    private static void emitNormalTurn(Tracer tracer) {
        String turnId = SESSION_ID + ":t1";
        String stepId = turnId + ":s1";
        String outputMessages =
                "[{\"role\":\"assistant\",\"parts\":[{\"type\":\"text\","
                + "\"content\":\"The weather in Beijing is sunny, around 22\u00B0C.\"}]}]";

        Span entry = startEntry(tracer, turnId);
        Context entryCtx = Context.root().with(entry);
        try {
            Span agentSpan = startAgent(tracer, entryCtx, turnId);
            Context agentCtx = entryCtx.with(agentSpan);
            try {
                Span step = startStep(tracer, agentCtx, turnId, stepId);
                Context stepCtx = agentCtx.with(step);
                step.setAttribute("gen_ai.react.finish_reason", "tool_calls");
                step.setAttribute("gen_ai.react.thought",
                        "I should call the weather tool for Beijing.");
                try {
                    emitChat(tracer, stepCtx, turnId, stepId, "tool_calls",
                            42L, 17L, 59L,
                            "[{\"role\":\"assistant\",\"parts\":[{\"type\":\"thinking\","
                            + "\"content\":\"I should call the weather tool for Beijing.\"}]}]");
                    emitTool(tracer, stepCtx, turnId, stepId);
                } finally {
                    step.end();
                }

                // agent-level aggregates (mirrors the middleware's doFinally)
                agentSpan.setAttribute("gen_ai.agent.message_count", 1L);
                agentSpan.setAttribute("gen_ai.agent.tool_call_count", 1L);
                agentSpan.setAttribute("gen_ai.usage.input_tokens", 42L);
                agentSpan.setAttribute("gen_ai.usage.output_tokens", 17L);
                agentSpan.setAttribute("gen_ai.usage.total_tokens", 59L);
                agentSpan.setAttribute("gen_ai.usage.cache_read.input_tokens", 30L);
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
    }

    /**
     * TURN 2: the model asks to call a tool, but the turn parks awaiting user confirmation.
     * No {@code tool} span is emitted (the tool never ran), and the turn is not completed.
     */
    private static void emitHitlTurn(Tracer tracer) {
        String turnId = SESSION_ID + ":t2";
        String stepId = turnId + ":s1";

        Span entry = startEntry(tracer, turnId);
        Context entryCtx = Context.root().with(entry);
        try {
            Span agentSpan = startAgent(tracer, entryCtx, turnId);
            Context agentCtx = entryCtx.with(agentSpan);
            try {
                Span step = startStep(tracer, agentCtx, turnId, stepId);
                Context stepCtx = agentCtx.with(step);
                step.setAttribute("gen_ai.react.finish_reason", "tool_calls");
                try {
                    emitChat(tracer, stepCtx, turnId, stepId, "tool_calls",
                            51L, 9L, 60L,
                            "[{\"role\":\"assistant\",\"parts\":[{\"type\":\"thinking\","
                            + "\"content\":\"I need to run a destructive action, ask the user "
                            + "first.\"}]}]");
                } finally {
                    step.end();
                }

                // HITL: middleware sets these when it sees RequireUserConfirmEvent
                agentSpan.setAttribute("gen_ai.hitl.event", "require_user_confirm");
                agentSpan.setAttribute("gen_ai.hitl.pending_tool_calls", "delete_file");
                agentSpan.setAttribute("gen_ai.agent.message_count", 1L);
                agentSpan.setAttribute("gen_ai.agent.tool_call_count", 0L);
                agentSpan.setAttribute("gen_ai.usage.input_tokens", 51L);
                agentSpan.setAttribute("gen_ai.usage.output_tokens", 9L);
                agentSpan.setAttribute("gen_ai.usage.total_tokens", 60L);
                agentSpan.setAttribute("gen_ai.usage.cache_read.input_tokens", 30L);
                agentSpan.setAttribute("gen_ai.finish_reason", "awaiting_user_confirm");
                agentSpan.setAttribute("gen_ai.turn.completed", false);
                agentSpan.setAttribute("gen_ai.output.messages", "[]");
            } finally {
                agentSpan.end();
            }
            entry.setAttribute("gen_ai.finish_reason", "awaiting_user_confirm");
            entry.setAttribute("gen_ai.turn.completed", false);
            entry.setAttribute("gen_ai.output.messages", "[]");
        } finally {
            entry.end();
        }
    }

    // ── span builders mirroring ClsTracingMiddleware ──

    private static Span startEntry(Tracer tracer, String turnId) {
        Span entry = tracer.spanBuilder("enter_application")
                .setSpanKind(SpanKind.SERVER).startSpan();
        entry.setAttribute("gen_ai.span.kind", "entry");
        entry.setAttribute("gen_ai.operation.name", "enter_application");
        entry.setAttribute("gen_ai.session.id", SESSION_ID);
        entry.setAttribute("gen_ai.turn.id", turnId);
        entry.setAttribute("gen_ai.user.id", USER_ID);
        entry.setAttribute("gen_ai.input.messages", INPUT_MESSAGES);
        return entry;
    }

    private static Span startAgent(Tracer tracer, Context parent, String turnId) {
        Span agentSpan = tracer.spanBuilder("invoke_agent " + AGENT_NAME)
                .setParent(parent)
                .setSpanKind(SpanKind.INTERNAL).startSpan();
        agentSpan.setAttribute("gen_ai.span.kind", "agent");
        agentSpan.setAttribute("gen_ai.operation.name", "invoke_agent");
        agentSpan.setAttribute("gen_ai.session.id", SESSION_ID);
        agentSpan.setAttribute("gen_ai.turn.id", turnId);
        agentSpan.setAttribute("gen_ai.agent.type", "agentscope");
        agentSpan.setAttribute("gen_ai.agent.name", AGENT_NAME);
        agentSpan.setAttribute("gen_ai.user.id", USER_ID);
        agentSpan.setAttribute("gen_ai.input.messages", INPUT_MESSAGES);
        return agentSpan;
    }

    private static Span startStep(Tracer tracer, Context parent, String turnId, String stepId) {
        Span step = tracer.spanBuilder("react round_1")
                .setParent(parent)
                .setSpanKind(SpanKind.INTERNAL).startSpan();
        step.setAttribute("gen_ai.span.kind", "step");
        step.setAttribute("gen_ai.operation.name", "react");
        step.setAttribute("gen_ai.session.id", SESSION_ID);
        step.setAttribute("gen_ai.turn.id", turnId);
        step.setAttribute("gen_ai.step.id", stepId);
        step.setAttribute("gen_ai.react.round", 1L);
        step.setAttribute("gen_ai.input.messages", INPUT_MESSAGES);
        return step;
    }

    private static void emitChat(Tracer tracer, Context parent, String turnId, String stepId,
                                 String finishReason, long inTokens, long outTokens, long totalTokens,
                                 String outputMessages) {
        Span chat = tracer.spanBuilder("chat gpt-4o")
                .setParent(parent)
                .setSpanKind(SpanKind.CLIENT).startSpan();
        try {
            chat.setAttribute("gen_ai.span.kind", "chat");
            chat.setAttribute("gen_ai.operation.name", "chat");
            chat.setAttribute("gen_ai.session.id", SESSION_ID);
            chat.setAttribute("gen_ai.turn.id", turnId);
            chat.setAttribute("gen_ai.step.id", stepId);
            chat.setAttribute("gen_ai.react.round", 1L);
            chat.setAttribute("gen_ai.request.model", "gpt-4o");
            chat.setAttribute("gen_ai.usage.input_tokens", inTokens);
            chat.setAttribute("gen_ai.usage.output_tokens", outTokens);
            chat.setAttribute("gen_ai.usage.total_tokens", totalTokens);
            chat.setAttribute("gen_ai.usage.cache_read.input_tokens", 30L);
            chat.setAttribute("gen_ai.response.finish_reasons", "[\"" + finishReason + "\"]");
            chat.setAttribute("gen_ai.output.messages", outputMessages);
        } finally {
            chat.end();
        }
    }

    private static void emitTool(Tracer tracer, Context parent, String turnId, String stepId) {
        Span tool = tracer.spanBuilder("execute_tool get_weather")
                .setParent(parent)
                .setSpanKind(SpanKind.CLIENT).startSpan();
        try {
            tool.setAttribute("gen_ai.span.kind", "tool");
            tool.setAttribute("gen_ai.operation.name", "execute_tool");
            tool.setAttribute("gen_ai.session.id", SESSION_ID);
            tool.setAttribute("gen_ai.turn.id", turnId);
            tool.setAttribute("gen_ai.step.id", stepId);
            tool.setAttribute("gen_ai.user.id", USER_ID);
            tool.setAttribute("gen_ai.tool.name", "get_weather");
            tool.setAttribute("gen_ai.tool.type", "function");
            tool.setAttribute("gen_ai.tool.call.id", "call_abc123");
            tool.setAttribute("gen_ai.tool.call.arguments", "{\"city\":\"Beijing\"}");
            tool.setAttribute("gen_ai.tool.call.result",
                    "{\"temp\":\"22C\",\"condition\":\"sunny\"}");
        } finally {
            tool.end();
        }
    }
}
