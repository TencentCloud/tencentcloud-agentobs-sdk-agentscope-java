package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.tracing.TracerRegistry;

import io.opentelemetry.api.common.AttributeKey;
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
 * Covers the <b>synchronous / blocking</b> shape of a framework memory call: a component that runs
 * {@code Model.stream(...).block()} straight from a callback, with no middleware involved.
 *
 * <p>The reference implementation of this shape is {@code AutoContextMemory} from
 * {@code agentscope-extensions-autocontext-memory}: it implements
 * {@link io.agentscope.core.memory.Memory} and calls {@code Model.stream()} directly from
 * {@code callModelForSummary} — four call sites, zero references to {@code MiddlewareChain} —
 * using the framework's own default prompts ({@code Prompts.PREVIOUS_ROUND_TOOL_INVOCATION_
 * COMPRESS_PROMPT}, {@code ..._CONVERSATION_SUMMARY_PROMPT}, {@code CURRENT_ROUND_LARGE_MESSAGE_
 * SUMMARY_PROMPT}, {@code CURRENT_ROUND_MESSAGE_COMPRESS_PROMPT}). Those calls never reach
 * {@code onModelCall()}: without a {@code TracerRegistry} hook they produce no span at all, while
 * a lower-level probe still records them.
 *
 * <p><b>Scope.</b> That extension is published only for AgentScope 1.x — it implements
 * {@code io.agentscope.core.state.StateModule} and references {@code core.session.Session},
 * neither of which exists in core 2.0.x — so a 2.x application cannot load it. The shape is still
 * worth pinning: it is what any synchronous, callback-driven memory component looks like, and the
 * blocking path is genuinely different from the detached-Scheduler path that
 * {@code agentscope-harness} uses (see {@code ClsHarnessMemoryFlushTest}). This test drives the
 * shape directly rather than depending on the unusable artifact.
 *
 * <p>{@link ClsFrameworkTracer} covers it because the component receives a {@link Model} and real
 * implementations extend {@link ChatModelBase}, whose {@code stream()} is {@code final} and routes
 * through {@code TracerRegistry}.
 */
@SuppressWarnings("removal")
class ClsMemoryCompressionCaptureTest {

    /** Verbatim opening of the extension's own compression prompt. */
    private static final String AUTO_CONTEXT_PROMPT =
            "You are an expert content compression specialist";

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

    /** Mimics {@code AutoContextMemory.callModelForSummary}: a bare {@code Model.stream()} call. */
    private static final class CompressingMemoryModel extends ChatModelBase {
        @Override
        public String getModelName() {
            return "compression-model";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> msgs, List<ToolSchema> tools, GenerateOptions options) {
            return Flux.just(ChatResponse.builder()
                    .id("compress-1")
                    .content(List.<ContentBlock>of(
                            TextBlock.builder().text("I called search with q=x; it returned 3 hits.")
                                    .build()))
                    .usage(new ChatUsage(900, 120, 0.2))
                    .finishReason("stop")
                    .build());
        }
    }

    private static Msg compressionPrompt() {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder()
                        .text(AUTO_CONTEXT_PROMPT
                                + ". Your task is to integrate new tool execution results into "
                                + "the current conversation context.")
                        .build())
                .build();
    }

    /**
     * The compression call happens on the memory component's own thread, outside any agent turn —
     * the shape {@code AutoContextMemory} uses. It must still be captured.
     */
    @Test
    void memoryCompressionCallOutsideAnyTurnIsCaptured() {
        // No agent, no RuntimeContext, no Reactor Context — exactly how the extension invokes it.
        new CompressingMemoryModel()
                .stream(List.of(compressionPrompt()), List.of(), null)
                .blockLast();

        List<SpanData> spans = exporter.spans();
        assertEquals(1, spans.size(),
                "the memory compression model call produced no span; spans=" + names(spans));

        SpanData span = spans.get(0);
        assertEquals("chat compression-model", span.getName());
        assertEquals("framework", str(span, "gen_ai.model.call.origin"),
                "must be tagged as a call the middleware chain never saw");

        String input = str(span, "gen_ai.input.messages");
        assertNotNull(input);
        assertTrue(input.contains(AUTO_CONTEXT_PROMPT),
                "the extension's default compression prompt must be captured, got: " + input);
        assertTrue(str(span, "gen_ai.output.messages").contains("search with q=x"),
                "the compression result must be captured");
        assertEquals(Long.valueOf(900L), longAttr(span, "gen_ai.usage.input_tokens"));
    }

    /**
     * A memory-compression call from inside a live turn (via imperative {@code Mono.block()},
     * exactly like {@code AutoContextMemory.callModelForSummary}) is correlated to the turn: same
     * traceId, correct parentSpanId, and carries the turn's session/turn IDs.
     *
     * <p>This works because {@link ClsFrameworkTracer#runWithContext} bridges the Reactor Context
     * to ThreadLocals on every signal. The memory call fires from a {@code doOnNext} callback
     * (like a real hook), and {@code Mono.block()} runs synchronously on the same thread, so the
     * ThreadLocals are still set.
     */
    @Test
    void memoryCompressionSpanInsideTurnIsCorrelated() {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);

        // The memory call fires in doOnNext — i.e. during signal processing, where
        // TracerRegistry's hook has already called runWithContext and set the ThreadLocals.
        // This matches real AutoContextMemory: it fires from a hook/callback during the
        // agent's reactive chain, not during Flux assembly.
        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("mem-s").userId("u").build(),
                        new AgentInput(List.of(compressionPrompt())),
                        in -> {
                            // Emit one event so doOnNext fires (doOnComplete doesn't go through
                            // TracerRegistry's hook in all Reactor versions).
                            return Flux.<AgentEvent>just(new io.agentscope.core.event.TextBlockDeltaEvent("r", "x", "hi"))
                                    .doOnNext(ev -> {
                                        // Imperative block inside signal processing, exactly like
                                        // callModelForSummary.
                                        new CompressingMemoryModel()
                                                .stream(List.of(compressionPrompt()), List.of(), null)
                                                .blockLast();
                                    });
                        })
                .blockLast();

        List<SpanData> spans = exporter.spans();
        SpanData compression = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("memory call not captured; spans="
                        + names(spans)));

        // Correlated to the turn.
        assertEquals("mem-s", str(compression, "gen_ai.session.id"),
                "memory span must carry the owning turn's session ID");
        assertNotNull(str(compression, "gen_ai.turn.id"),
                "memory span must carry the owning turn's turn ID");

        // Same traceId as the turn's entry/agent spans.
        SpanData entry = spans.stream()
                .filter(s -> "entry".equals(str(s, "gen_ai.span.kind")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no entry span"));
        assertEquals(entry.getTraceId(), compression.getTraceId(),
                "memory span must share the turn's traceId");

        // Not a dangling root — it has a parent within the trace.
        assertTrue(spans.stream().anyMatch(
                        s -> s.getSpanId().equals(compression.getParentSpanId())),
                "memory span must have a parent in the turn's trace tree");
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static String str(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.stringKey(key));
    }

    private static Long longAttr(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.longKey(key));
    }

    private static String names(List<SpanData> spans) {
        return spans.stream().map(SpanData::getName)
                .collect(java.util.stream.Collectors.joining(", "));
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
