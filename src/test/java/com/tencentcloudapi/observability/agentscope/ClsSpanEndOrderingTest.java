package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
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
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down <b>when</b> spans are ended: a span must be closed BEFORE the terminal signal reaches
 * the subscriber, i.e. before a blocking caller is released.
 *
 * <p><b>The bug this guards against.</b> Reactor invokes a {@code doFinally} callback <em>after</em>
 * propagating {@code onComplete} downstream. With a synchronous model (plain {@code Flux.just})
 * emission happens on the caller's own thread, so the callback still runs inside the blocking call
 * and nothing is observable. With a <em>real</em> model the response is emitted on an HTTP/worker
 * thread: {@code block()} is released by {@code onComplete} while {@code span.end()} is still
 * pending on that other thread. An application that then flushes and shuts the exporter down —
 * which is exactly what a CLI/batch job does on its last line — loses those spans: they are ended
 * after the provider is closed and are never exported.
 *
 * <p>Both tests therefore emit on another thread and assert <em>immediately</em> after the blocking
 * call returns, with no sleep and no flush: the spans must already be exported.
 */
@SuppressWarnings("removal")
class ClsSpanEndOrderingTest {

    private SdkTracerProvider tracerProvider;
    private CollectingExporter exporter;
    private Tracer tracer;

    @BeforeEach
    void setUp() {
        ClsTracingMiddleware.resetSessionIndexForTesting();
        exporter = new CollectingExporter();
        // SimpleSpanProcessor exports on Span.end(), so "already exported" == "already ended".
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

    /** Emits its single response on a worker thread, like a real HTTP-backed model. */
    private static final class AsyncEmittingModel extends ChatModelBase {
        @Override
        public String getModelName() {
            return "async-model";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> msgs, List<ToolSchema> tools, GenerateOptions options) {
            return Flux.just(ChatResponse.builder()
                            .id("a1")
                            .content(List.<ContentBlock>of(
                                    TextBlock.builder().text("summarised").build()))
                            .usage(new ChatUsage(120, 30, 0.0))
                            .finishReason("stop")
                            .build())
                    .subscribeOn(Schedulers.boundedElastic());
        }
    }

    private static Msg prompt() {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text("compress the context").build())
                .build();
    }

    /**
     * The framework-tracer path: a memory/summarisation call that blocks on a model emitting
     * asynchronously. When {@code blockLast()} returns, the {@code chat} span must already be
     * ended — otherwise a following {@code flush()} / {@code shutdown()} drops it.
     */
    @Test
    void frameworkChatSpanIsEndedBeforeBlockingCallReturns() {
        new AsyncEmittingModel().stream(List.of(prompt()), List.of(), null).blockLast();

        // No sleep, no forceFlush: whatever is here was ended before block() returned.
        List<SpanData> spans = exporter.spans();
        assertTrue(spans.stream().anyMatch(s -> "chat async-model".equals(s.getName())),
                "the chat span must be ended before the blocking caller is released, otherwise "
                        + "an immediate flush/shutdown loses it; exported=" + names(spans));
    }

    /**
     * The middleware path: same guarantee for the turn's own spans, so
     * {@code agent.call(...).block()} followed immediately by {@code obs.flush()} cannot lose
     * {@code enter_application} / {@code invoke_agent}.
     */
    @Test
    void turnSpansAreEndedBeforeBlockingCallReturns() {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);

        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("order-s").userId("u").build(),
                        new AgentInput(List.of(prompt())),
                        in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r", "b", "hi"))
                                .subscribeOn(Schedulers.boundedElastic()))
                .blockLast();

        List<SpanData> spans = exporter.spans();
        assertTrue(spans.stream().anyMatch(s -> "entry".equals(str(s, "gen_ai.span.kind"))),
                "the entry span must be ended before block() returns; exported=" + names(spans));
        assertTrue(spans.stream().anyMatch(s -> "agent".equals(str(s, "gen_ai.span.kind"))),
                "the agent span must be ended before block() returns; exported=" + names(spans));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static String str(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.stringKey(key));
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
