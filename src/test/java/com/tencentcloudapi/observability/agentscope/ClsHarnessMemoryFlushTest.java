package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.middleware.ReasoningInput;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the memory-flush model call made by <b>{@code agentscope-harness}</b> — the extra
 * {@code chat} span that shows up after the agent has answered, carrying harness's
 * "You are a memory extraction assistant…" prompt.
 *
 * <p><b>The real call shape</b>, read off {@code agentscope-harness} 2.0.4 bytecode:
 * <ul>
 *   <li>{@code MemoryFlushMiddleware implements HarnessRuntimeMiddleware} and overrides
 *       <b>only {@code onAgent}</b> — never {@code onModelCall}. The flush is hung off
 *       {@code Flux.doOnComplete(...)}, so it fires <em>after</em> the turn's event stream
 *       terminates.</li>
 *   <li>{@code runFlush} does
 *       {@code Mono.defer(..).subscribeOn(Schedulers.boundedElastic()).doFinally(..).subscribe(..)}
 *       — a fresh, <b>detached</b> subscription on another thread.</li>
 *   <li>Neither {@code MemoryFlushMiddleware} nor {@code MemoryFlushManager} calls
 *       {@code contextWrite} (verified: zero occurrences), so the turn's Reactor Context cannot
 *       reach the flush. {@code flushMemories} itself is reactive — it returns
 *       {@code Mono<Void>} and never blocks.</li>
 *   <li>The model is invoked through {@code Model.stream(...)}, and {@code ChatModelBase.stream()}
 *       is {@code final} with {@code TracerRegistry.get()} as its very first instruction.</li>
 * </ul>
 *
 * <p>That last point is why the call is observable at all: no matter which component decides to
 * talk to the model, it funnels through {@code TracerRegistry}. {@link ClsFrameworkTracer} hooks
 * exactly there.
 *
 * <p>Correlation is the harder half. With the Reactor Context gone, the owning turn is recovered
 * through the per-session index — see {@link #harnessMemoryFlushIsCapturedAndCorrelated()} for the
 * single-session case and {@link #concurrentSessionsAreNotCrossAttributed()} for the deliberate
 * refusal to guess when several sessions are in flight.
 */
@SuppressWarnings("removal")
class ClsHarnessMemoryFlushTest {

    /** Verbatim opening of harness's {@code MemoryFlushManager.DEFAULT_FLUSH_PROMPT}. */
    private static final String FLUSH_PROMPT = "You are a memory extraction assistant.";

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

    private static final class FlushModel extends ChatModelBase {
        @Override
        public String getModelName() {
            return "flush-model";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> msgs, List<ToolSchema> tools, GenerateOptions options) {
            return Flux.just(ChatResponse.builder()
                    .id("flush-1")
                    .content(List.<ContentBlock>of(TextBlock.builder()
                            .text("- User asked about Beijing weather").build()))
                    .usage(new ChatUsage(1016, 127, 0.0))
                    .finishReason("stop")
                    .build());
        }
    }

    private static Msg msg(String text) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    /**
     * Reproduce {@code MemoryFlushMiddleware}: trigger on {@code doOnComplete}, then run the
     * model call as a detached {@code subscribeOn(boundedElastic())} subscription with no
     * context written — exactly what the bytecode does.
     */
    private static Flux<AgentEvent> withHarnessMemoryFlush(
            Flux<AgentEvent> turn, FlushModel model, CountDownLatch flushed) {
        return turn.doOnComplete(() ->
                Mono.defer(() -> model
                                .stream(List.of(msg(FLUSH_PROMPT
                                        + " Analyze the conversation below and extract important "
                                        + "facts, decisions, preferences.")), List.of(), null)
                                .then())
                        .subscribeOn(Schedulers.boundedElastic())
                        .doFinally(sig -> flushed.countDown())
                        .subscribe(v -> { }, err -> flushed.countDown()));
    }

    /**
     * One ReAct round inside the turn, so the middleware opens a real {@code step} span.
     *
     * <p>This is not decoration: that step span is kept open past its own reasoning Flux (to host
     * the round's tool span) and is still referenced by the turn when {@code doOnComplete} fires.
     * A test that skips {@code onReasoning} never creates it, so it cannot tell whether a
     * post-turn call is correctly given its own framework step or wrongly folded into the last
     * round — which is exactly the regression this shape catches.
     */
    private Flux<AgentEvent> oneReasoningRound(ClsTracingMiddleware middleware) {
        return middleware.onReasoning(null, null,
                new ReasoningInput(List.of(msg("What's the weather in Beijing?")),
                        List.of(), null),
                ri -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r", "b", "sunny, 22C")));
    }

    /**
     * The single-session case — a CLI tool, a desktop app, or any service handling one
     * conversation at a time. The flush span must be captured AND land on the turn's trace.
     */
    @Test
    void harnessMemoryFlushIsCapturedAndCorrelated() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        FlushModel model = new FlushModel();
        CountDownLatch flushed = new CountDownLatch(1);

        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("harness-s").userId("u").build(),
                        new AgentInput(List.of(msg("What's the weather in Beijing?"))),
                        in -> withHarnessMemoryFlush(
                                oneReasoningRound(middleware),
                                model, flushed))
                .blockLast();

        assertTrue(flushed.await(5, TimeUnit.SECONDS), "the memory flush never ran");

        List<SpanData> spans = exporter.spans();
        SpanData flush = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "harness's memory-flush model call produced no span; spans="
                                + names(spans)));

        // Captured, with content.
        assertEquals("chat flush-model", flush.getName());
        assertTrue(str(flush, "gen_ai.input.messages").contains("memory extraction assistant"),
                "harness's own flush prompt must be captured");
        assertTrue(str(flush, "gen_ai.output.messages").contains("Beijing weather"),
                "the extracted memory must be captured");
        assertEquals(Long.valueOf(1016L), longAttr(flush, "gen_ai.usage.input_tokens"));

        // Correlated, even though the flush ran detached on another thread after the turn ended.
        assertEquals("harness-s", str(flush, "gen_ai.session.id"),
                "a post-turn flush must be recovered onto its session via the session index");
        assertNotNull(str(flush, "gen_ai.turn.id"));

        SpanData entry = spans.stream()
                .filter(s -> "entry".equals(str(s, "gen_ai.span.kind")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no entry span"));
        assertEquals(entry.getTraceId(), flush.getTraceId(),
                "the flush span must share the turn's traceId, like the agent-based probe's tree");
        assertTrue(spans.stream().anyMatch(s -> s.getSpanId().equals(flush.getParentSpanId())),
                "the flush span must hang off a span inside the turn's tree");

        // It must sit under a SYNTHESISED framework step, not inside the final ReAct round. The
        // last round's step span is deliberately kept open until the turn ends, so a naive
        // "is a step in flight?" check would still see it when the flush is submitted from
        // doOnComplete and would file memory work as part of that round.
        SpanData parent = spans.stream()
                .filter(s -> s.getSpanId().equals(flush.getParentSpanId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the flush span's parent is not in the trace"));
        assertEquals("framework", str(parent, "gen_ai.step.type"),
                "the flush must hang off a synthesised framework step, but its parent is '"
                        + parent.getName() + "' (step.id=" + str(parent, "gen_ai.step.id") + ")");
        assertTrue(str(flush, "gen_ai.step.id").contains(":f"),
                "a framework call's step id must use the :f{N} sequence, keeping the :s{N} ids of "
                        + "real ReAct rounds untouched; got " + str(flush, "gen_ai.step.id"));
    }

    /**
     * The honest boundary of attribution: a model call whose <b>submitting</b> thread never served
     * any turn, while several sessions are tracked. There is genuinely nothing to tie it to, so it
     * must stay unattributed rather than be guessed — mislabelling one tenant's call as another's
     * corrupts the data far worse than an orphan trace does.
     *
     * <p>Note what is NOT asserted here: a flush submitted from a thread that <em>did</em> just
     * serve a turn is legitimately attributed to it, even across a Scheduler hop — that is the
     * harness case and is covered by {@link ClsDetachedSchedulerCorrelationTest}. The signal is
     * the submitting thread, not the worker thread.
     */
    @Test
    void callFromAThreadThatNeverServedATurnIsNotAttributed() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        FlushModel model = new FlushModel();

        for (String session : List.of("tenant-a", "tenant-b")) {
            middleware.onAgent(null,
                            RuntimeContext.builder().sessionId(session).userId("u").build(),
                            new AgentInput(List.of(msg("hi"))),
                            in -> Flux.<AgentEvent>just(
                                    new TextBlockDeltaEvent("r", "b", "hello")))
                    .blockLast();
        }

        // Submit from a brand-new thread: it has served no turn, so there is no hint to carry.
        CountDownLatch flushed = new CountDownLatch(1);
        Thread stranger = new Thread(() ->
                Mono.defer(() -> model.stream(List.of(msg(FLUSH_PROMPT)), List.of(), null).then())
                        .subscribeOn(Schedulers.boundedElastic())
                        .doFinally(sig -> flushed.countDown())
                        .subscribe(v -> { }, err -> flushed.countDown()),
                "unrelated-submitter");
        stranger.start();
        stranger.join(5_000);
        assertTrue(flushed.await(5, TimeUnit.SECONDS), "the flush never ran");

        SpanData flush = exporter.spans().stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the flush call must still be recorded"));

        assertEquals("0000000000000000", flush.getParentSpanId(),
                "with two candidate sessions and no thread hint, the span must stay an orphan "
                        + "rather than be guessed");
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
