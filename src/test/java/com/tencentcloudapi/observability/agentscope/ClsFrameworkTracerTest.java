package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.tracing.TracerRegistry;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the gap {@link ClsFrameworkTracer} exists to close: model calls that AgentScope issues
 * <em>outside</em> any middleware chain — the framework's own memory flush / history
 * summarisation prompts — must still produce a {@code chat} span, while model calls the middleware
 * already instruments must NOT produce a second one.
 */
@SuppressWarnings("removal") // TracerRegistry is deprecated-for-removal but still the live hook
class ClsFrameworkTracerTest {

    private SdkTracerProvider tracerProvider;
    private CollectingExporter exporter;
    private Tracer tracer;

    @BeforeEach
    void setUp() {
        ClsTracingMiddleware.resetSessionIndexForTesting();
        exporter = new CollectingExporter();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        tracer = tracerProvider.get("test");
        ClsFrameworkTracer.install(tracer);
    }

    @AfterEach
    void tearDown() {
        TracerRegistry.resetToNoop();
        tracerProvider.close();
    }

    /** A direct {@code Model.stream()} — what the framework's memory work does — is traced. */
    @Test
    void tracesModelCallOutsideMiddlewareChain() {
        new EchoModel("qwen-max", "memory summary").stream(
                List.of(userMsg("compress this history")), List.of(), null).blockLast();

        List<SpanData> spans = exporter.spans();
        assertEquals(1, spans.size(), "framework-issued model call must produce exactly one span");

        SpanData span = spans.get(0);
        assertEquals("chat qwen-max", span.getName());
        assertEquals("chat", attr(span, "gen_ai.span.kind"));
        assertEquals("framework", attr(span, "gen_ai.model.call.origin"));
        assertEquals("qwen-max", attr(span, "gen_ai.request.model"));

        String input = attr(span, "gen_ai.input.messages");
        assertNotNull(input);
        assertTrue(input.contains("compress this history"),
                "the framework's own prompt must be captured, got: " + input);

        String output = attr(span, "gen_ai.output.messages");
        assertNotNull(output);
        assertTrue(output.contains("memory summary"),
                "the model's reply must be captured, got: " + output);

        assertEquals(Long.valueOf(11L), longAttr(span, "gen_ai.usage.input_tokens"));
        assertEquals(Long.valueOf(7L), longAttr(span, "gen_ai.usage.output_tokens"));
    }

    /**
     * A model call made underneath {@link ClsTracingMiddleware}'s {@code chat} span is marked on
     * the Reactor Context; the tracer must defer so the call is not counted twice.
     */
    @Test
    void skipsModelCallAlreadyInstrumentedByMiddleware() {
        new EchoModel("qwen-max", "answer").stream(
                        List.of(userMsg("hi")), List.of(), null)
                .contextWrite(ctx -> ctx.put(ClsTracingMiddleware.MODEL_CALL_MARK_KEY, Boolean.TRUE))
                .blockLast();

        assertEquals(List.of(), exporter.spans(),
                "middleware-owned model calls must not be traced twice");
    }

    /** Installing twice must not stack tracers and double every framework span. */
    @Test
    void installIsIdempotent() {
        ClsFrameworkTracer.install(tracer);
        ClsFrameworkTracer.install(tracer);

        new EchoModel("qwen-max", "once").stream(
                List.of(userMsg("hi")), List.of(), null).blockLast();

        assertEquals(1, exporter.spans().size(), "re-installing must not duplicate spans");
    }

    /** Business flow must survive a failing model call, with the error recorded on the span. */
    @Test
    void recordsModelCallFailure() {
        Flux<ChatResponse> flux = new FailingModel().stream(List.of(userMsg("hi")), List.of(), null);

        assertTrue(org.junit.jupiter.api.Assertions
                        .assertThrows(IllegalStateException.class, flux::blockLast)
                        .getMessage().contains("model down"),
                "the business error must propagate unchanged");

        List<SpanData> spans = exporter.spans();
        assertEquals(1, spans.size());
        assertEquals(io.opentelemetry.api.trace.StatusCode.ERROR,
                spans.get(0).getStatus().getStatusCode());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static Msg userMsg(String text) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    private static String attr(SpanData span, String key) {
        return span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(key));
    }

    private static Long longAttr(SpanData span, String key) {
        return span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.longKey(key));
    }

    /** Minimal {@link ChatModelBase} so {@code stream()} routes through {@code TracerRegistry}. */
    private static final class EchoModel extends ChatModelBase {
        private final String modelName;
        private final String reply;

        EchoModel(String modelName, String reply) {
            this.modelName = modelName;
            this.reply = reply;
        }

        @Override
        public String getModelName() {
            return modelName;
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            return Flux.just(ChatResponse.builder()
                    .id("resp-1")
                    .content(List.<ContentBlock>of(TextBlock.builder().text(reply).build()))
                    .usage(new ChatUsage(11, 7, 0.1))
                    .finishReason("stop")
                    .build());
        }
    }

    private static final class FailingModel extends ChatModelBase {
        @Override
        public String getModelName() {
            return "broken";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            return Flux.error(new IllegalStateException("model down"));
        }
    }

    private static final class CollectingExporter implements SpanExporter {
        private final List<SpanData> collected = new CopyOnWriteArrayList<>();

        List<SpanData> spans() {
            return new ArrayList<>(collected);
        }

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            collected.addAll(spans);
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
    }
}
