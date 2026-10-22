package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down trace correlation for framework memory calls issued ASYNCHRONOUSLY.
 *
 * <p>"Async" covers three shapes here, with different outcomes:
 * <ol>
 *   <li><b>Async within a Reactor chain</b> ({@code subscribeOn}/{@code publishOn}): the Reactor
 *       Context — and therefore the turn state — propagates across the thread hop, so correlation
 *       works through the same mechanism as the synchronous case.</li>
 *   <li><b>Detached thread with the OTel Context propagated</b> (the standard OTel idiom, and what
 *       an agent-based probe does automatically): linkage survives even though neither Reactor
 *       Context nor our ThreadLocal does. See {@link #detachedThreadWithOtelContextPropagated()}.</li>
 *   <li><b>Detached thread with nothing propagated</b> (raw {@code ExecutorService.submit}): every
 *       live channel is gone, so the span can only be recovered through the per-session index —
 *       and only when the attribution is unambiguous.</li>
 * </ol>
 *
 * <p>A fourth shape, a detached subscription handed to a Reactor {@code Scheduler}, is what
 * {@code agentscope-harness} actually uses; it is covered separately by
 * {@code ClsHarnessMemoryFlushTest} and {@code ClsDetachedSchedulerCorrelationTest}, since it
 * relies on the {@code Schedulers} hook rather than on any of the channels above.
 */
@SuppressWarnings("removal")
class ClsAsyncMemoryCorrelationTest {

    private SdkTracerProvider tracerProvider;
    private CollectingExporter exporter;
    private Tracer tracer;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        ClsTracingMiddleware.resetSessionIndexForTesting();
        exporter = new CollectingExporter();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        tracer = tracerProvider.get("test");
        ClsFrameworkTracer.install(tracer);
        pool = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() throws Exception {
        pool.shutdownNow();
        pool.awaitTermination(2, TimeUnit.SECONDS);
        TracerRegistry.resetToNoop();
        tracerProvider.close();
    }

    private static final class CompressingMemoryModel extends ChatModelBase {
        @Override
        public String getModelName() {
            return "compression-model";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> msgs, List<ToolSchema> tools, GenerateOptions options) {
            return Flux.just(ChatResponse.builder()
                    .id("c1")
                    .content(List.<ContentBlock>of(
                            TextBlock.builder().text("compressed").build()))
                    .usage(new ChatUsage(900, 120, 0.2))
                    .finishReason("stop")
                    .build());
        }
    }

    private static Msg prompt() {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder()
                        .text("You are an expert content compression specialist.")
                        .build())
                .build();
    }

    /**
     * Case 1 — async inside the Reactor chain. A Reactor Context survives {@code subscribeOn},
     * so the memory span must join the turn's trace even though it runs on another thread.
     */
    @Test
    void asyncWithinReactorChainIsCorrelated() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);

        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("async-s").userId("u").build(),
                        new AgentInput(List.of(prompt())),
                        in -> new CompressingMemoryModel()
                                .stream(List.of(prompt()), List.of(), null)
                                // Hops to another thread but stays inside the reactive chain,
                                // so the Reactor Context travels with it.
                                .subscribeOn(Schedulers.boundedElastic())
                                .thenMany(Flux.<AgentEvent>just(
                                        new io.agentscope.core.event.TextBlockDeltaEvent(
                                                "r", "b", "hi"))))
                .blockLast();

        List<SpanData> spans = exporter.awaitSpans(4);
        SpanData memory = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "async memory call not captured; spans=" + names(spans)));

        assertEquals("async-s", str(memory, "gen_ai.session.id"),
                "Reactor Context survives subscribeOn, so the turn must still be attributed");
        assertNotNull(str(memory, "gen_ai.turn.id"));

        SpanData entry = spans.stream()
                .filter(s -> "entry".equals(str(s, "gen_ai.span.kind")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no entry span"));
        assertEquals(entry.getTraceId(), memory.getTraceId(),
                "async memory span must share the turn's traceId");
        assertTrue(spans.stream().anyMatch(
                        s -> s.getSpanId().equals(memory.getParentSpanId())),
                "async memory span must have a parent inside the trace");
    }

    /**
     * Case 2 — fully detached thread, with the OTel Context explicitly propagated (the standard
     * OTel idiom, and what an agent-based probe does automatically). Trace linkage must survive
     * even though neither Reactor Context nor our ThreadLocal does.
     */
    @Test
    void detachedThreadWithOtelContextPropagated() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);

        // Capture the turn's OTel context from inside the turn, then run the memory call on an
        // unrelated thread with that context made current — i.e. io.opentelemetry.context.Context
        // propagation, nothing Reactor-specific.
        java.util.concurrent.atomic.AtomicReference<io.opentelemetry.context.Context> captured =
                new java.util.concurrent.atomic.AtomicReference<>();

        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("detached-s").userId("u").build(),
                        new AgentInput(List.of(prompt())),
                        in -> Flux.<AgentEvent>just(
                                        new io.agentscope.core.event.TextBlockDeltaEvent(
                                                "r", "b", "hi"))
                                // Capture during signal processing, where the turn is in scope.
                                .doOnNext(ev -> captured.set(
                                        ClsTracingMiddleware.currentTurnOtelContext())))
                .blockLast();

        io.opentelemetry.context.Context turnCtx = captured.get();
        assertNotNull(turnCtx, "the turn's OTel context must be exposed for propagation");

        pool.submit(() -> {
            try (io.opentelemetry.context.Scope ignored = turnCtx.makeCurrent()) {
                new CompressingMemoryModel()
                        .stream(List.of(prompt()), List.of(), null)
                        .blockLast();
            }
        }).get(5, TimeUnit.SECONDS);

        List<SpanData> spans = exporter.spans();
        SpanData memory = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "detached memory call not captured; spans=" + names(spans)));

        SpanData entry = spans.stream()
                .filter(s -> "entry".equals(str(s, "gen_ai.span.kind")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no entry span"));
        assertEquals(entry.getTraceId(), memory.getTraceId(),
                "a propagated OTel Context must keep the detached span on the turn's trace");
        assertEquals("detached-s", str(memory, "gen_ai.session.id"),
                "turn identity must travel with the OTel Context, not just the span parent");
        assertNotNull(str(memory, "gen_ai.turn.id"));
    }

    /**
     * Case 3 — the real-world shape: memory extraction fires on a detached thread AFTER the agent
     * has replied, with nothing propagated. Every context channel is gone by then, yet the span
     * must still land on the turn's trace, recovered through the session index.
     *
     * <p>This is what the framework's memory component actually does — it summarises the
     * conversation that just finished — and is why a session-scoped fallback exists rather than
     * relying on context propagation alone.
     */
    @Test
    void detachedPostTurnMemoryCallIsAttachedViaSessionIndex() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);

        // A complete turn, fully terminated before the memory call starts.
        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("post-s").userId("u").build(),
                        new AgentInput(List.of(prompt())),
                        in -> Flux.<AgentEvent>just(
                                new io.agentscope.core.event.TextBlockDeltaEvent("r", "b", "hi")))
                .blockLast();

        List<SpanData> afterTurn = exporter.awaitSpans(2);
        SpanData entry = afterTurn.stream()
                .filter(s -> "entry".equals(str(s, "gen_ai.span.kind")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no entry span"));

        // Now the memory call: unrelated thread, no Reactor Context, no OTel Context, no
        // ThreadLocal turn state — and the turn's spans have already ended.
        pool.submit(() -> new CompressingMemoryModel()
                .stream(List.of(prompt()), List.of(), null)
                .blockLast()).get(5, TimeUnit.SECONDS);

        List<SpanData> spans = exporter.spans();
        SpanData memory = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "post-turn memory call not captured; spans=" + names(spans)));

        assertEquals(entry.getTraceId(), memory.getTraceId(),
                "a post-turn memory span must be recovered onto the turn's trace");
        assertEquals("post-s", str(memory, "gen_ai.session.id"),
                "session identity must be recovered from the session index");
        assertNotNull(str(memory, "gen_ai.turn.id"));
        assertTrue(spans.stream().anyMatch(
                        s -> s.getSpanId().equals(memory.getParentSpanId())),
                "the memory span must hang off a span inside the turn's tree");
        // Content is captured regardless of correlation.
        assertTrue(str(memory, "gen_ai.input.messages").contains("compression specialist"));
    }

    /**
     * Case 4 — the safety guarantee that makes the session fallback acceptable: with SEVERAL
     * sessions tracked and no thread hint, a model call must NOT be guessed onto one of them.
     *
     * <p>Cross-attributing one tenant's memory call to another tenant's trace would be worse than
     * an orphan trace — it corrupts the data. So the fallback deliberately declines when the
     * attribution is ambiguous; the span is still recorded, just on its own trace.
     */
    @Test
    void ambiguousMultiSessionCallIsNotMisattributed() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);

        for (String session : List.of("tenant-a", "tenant-b")) {
            middleware.onAgent(null,
                            RuntimeContext.builder().sessionId(session).userId("u").build(),
                            new AgentInput(List.of(prompt())),
                            in -> Flux.<AgentEvent>just(
                                    new io.agentscope.core.event.TextBlockDeltaEvent(
                                            "r", "b", "hi")))
                    .blockLast();
        }
        exporter.awaitSpans(4);
        int beforeMemory = exporter.spans().size();

        // A pool thread that never served either turn: no hint, two candidates.
        pool.submit(() -> new CompressingMemoryModel()
                .stream(List.of(prompt()), List.of(), null)
                .blockLast()).get(5, TimeUnit.SECONDS);

        List<SpanData> spans = exporter.spans();
        assertEquals(beforeMemory + 1, spans.size(), "the memory call must still be recorded");

        SpanData memory = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("memory call not captured"));

        assertEquals("0000000000000000", memory.getParentSpanId(),
                "with two candidate sessions the span must stay an orphan rather than be guessed");
        assertNull(str(memory, "gen_ai.session.id"),
                "an ambiguous call must not be stamped with someone else's session id");
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

        List<SpanData> awaitSpans(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (collected.size() < expected && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            return spans();
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
