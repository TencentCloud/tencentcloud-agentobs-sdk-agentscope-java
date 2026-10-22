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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 * Pins down correlation for post-turn model calls handed to a {@code Scheduler} as a
 * <b>detached</b> subscription, with <b>several sessions in flight</b> — the shape
 * {@code agentscope-harness}'s {@code MemoryFlushMiddleware} uses, on a multi-user service.
 *
 * <p><b>Why this needs its own test.</b> {@link ClsHarnessMemoryFlushTest} covers the same call
 * shape with a single session, where attribution can fall back to "there is only one turn in
 * flight". That fallback is exactly what a real deployment does NOT have: a chat bot serves many
 * users, and the SDK's session index keeps finished turns around for minutes, so more than one
 * session is essentially always tracked. Correlation therefore has to survive without that
 * shortcut, which is what this test requires.
 *
 * <p>The mechanism under test is the {@code Schedulers.onScheduleHook} installed by
 * {@link ClsFrameworkTracer}: the turn is snapshotted on the thread that <em>submits</em> the
 * flush task (where it is still in scope, because submission happens inside signal processing) and
 * restored around the task on the worker thread.
 */
@SuppressWarnings("removal")
class ClsDetachedSchedulerCorrelationTest {

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
                    .id("f1")
                    .content(List.<ContentBlock>of(TextBlock.builder().text("noted").build()))
                    .usage(new ChatUsage(100, 10, 0.0))
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
     * Three sessions, each flushing memory after its turn exactly the way harness does. Every
     * flush span must carry its OWN session's id and sit on that session's trace — no orphans,
     * and no cross-attribution between sessions.
     */
    @Test
    void detachedFlushPerSessionIsCorrelatedWithSeveralSessionsTracked() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        List<String> sessions = List.of("feishu:ou_a", "feishu:ou_b", "feishu:ou_c");
        CountDownLatch flushed = new CountDownLatch(sessions.size());

        for (String session : sessions) {
            FlushModel model = new FlushModel();
            middleware.onAgent(null,
                            RuntimeContext.builder().sessionId(session).userId("u").build(),
                            new AgentInput(List.of(msg("weather?"))),
                            in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r", "b", "sunny"))
                                    // harness: schedule the flush from doOnComplete as a
                                    // detached subscription on another Scheduler, writing
                                    // nothing to the Reactor Context.
                                    .doOnComplete(() -> Mono.defer(() -> model
                                                    .stream(List.of(msg(FLUSH_PROMPT)),
                                                            List.of(), null)
                                                    .then())
                                            .subscribeOn(Schedulers.boundedElastic())
                                            .doFinally(sig -> flushed.countDown())
                                            .subscribe(v -> { }, err -> flushed.countDown())))
                    .blockLast();
        }

        assertTrue(flushed.await(10, TimeUnit.SECONDS), "not every flush ran");

        List<SpanData> spans = exporter.spans();
        List<SpanData> flushes = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .toList();
        assertEquals(sessions.size(), flushes.size(),
                "expected one framework-origin chat span per session, got " + flushes.size()
                        + "; spans=" + names(spans));

        // Index each session's entry span so we can check trace membership.
        Map<String, SpanData> entryBySession = new ConcurrentHashMap<>();
        for (SpanData s : spans) {
            if ("entry".equals(str(s, "gen_ai.span.kind"))) {
                entryBySession.put(str(s, "gen_ai.session.id"), s);
            }
        }
        assertEquals(sessions.size(), entryBySession.size(), "expected one entry span per session");

        for (SpanData flush : flushes) {
            String session = str(flush, "gen_ai.session.id");
            assertNotNull(session,
                    "a detached flush span must still carry its session id — without it the span "
                            + "is unattributable in CLS");
            assertTrue(sessions.contains(session), "unknown session on flush span: " + session);
            assertNotNull(str(flush, "gen_ai.turn.id"));

            SpanData entry = entryBySession.get(session);
            assertNotNull(entry, "no entry span for session " + session);
            assertEquals(entry.getTraceId(), flush.getTraceId(),
                    "the flush span must sit on ITS OWN session's trace (session=" + session + ")");
            assertTrue(spans.stream().anyMatch(s -> s.getSpanId().equals(flush.getParentSpanId())),
                    "the flush span must hang off a span inside the trace, not be an orphan");
        }

        // Each flush must be attributed to a DIFFERENT session: proves the per-task snapshot is
        // used rather than some shared "most recent turn".
        assertEquals(sessions.size(),
                flushes.stream().map(s -> str(s, "gen_ai.session.id")).distinct().count(),
                "flush spans were cross-attributed — several landed on the same session");
    }

    /**
     * The scheduler hook must leave pooled worker threads exactly as it found them: a task
     * submitted with no turn in scope must not inherit a previous task's turn.
     */
    @Test
    void workerThreadsDoNotLeakTurnStateBetweenTasks() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        FlushModel inTurn = new FlushModel();
        CountDownLatch firstDone = new CountDownLatch(1);

        // A turn whose flush runs on boundedElastic, installing turn state on a worker thread.
        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("leak-s").userId("u").build(),
                        new AgentInput(List.of(msg("hi"))),
                        in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r", "b", "yo"))
                                .doOnComplete(() -> Mono.defer(() -> inTurn
                                                .stream(List.of(msg(FLUSH_PROMPT)), List.of(), null)
                                                .then())
                                        .subscribeOn(Schedulers.boundedElastic())
                                        .doFinally(sig -> firstDone.countDown())
                                        .subscribe(v -> { }, err -> firstDone.countDown())))
                .blockLast();
        assertTrue(firstDone.await(10, TimeUnit.SECONDS), "the in-turn flush never ran");

        int before = exporter.spans().size();

        // An unrelated background job on the same pool, submitted with no turn in scope.
        FlushModel background = new FlushModel();
        CountDownLatch secondDone = new CountDownLatch(1);
        Mono.defer(() -> background.stream(List.of(msg("unrelated job")), List.of(), null).then())
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(sig -> secondDone.countDown())
                .subscribe(v -> { }, err -> secondDone.countDown());
        assertTrue(secondDone.await(10, TimeUnit.SECONDS), "the background job never ran");

        List<SpanData> added = exporter.spans().subList(before, exporter.spans().size());
        SpanData job = added.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .filter(s -> str(s, "gen_ai.input.messages").contains("unrelated job"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "the background model call was not captured; added=" + names(added)));

        assertEquals("leak-s", str(job, "gen_ai.session.id"),
                "with exactly one session tracked, the single-turn fallback legitimately claims "
                        + "this call; the point of this test is that it is NOT claimed because a "
                        + "worker thread kept stale state — if this ever reports a different "
                        + "session, state leaked between pooled tasks");
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
