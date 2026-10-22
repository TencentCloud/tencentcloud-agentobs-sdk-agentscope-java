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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the two properties that make the Scheduler-hop snapshot hold <b>values</b> rather than the
 * live {@code TurnState}.
 *
 * <p>Both are invisible to the other tests: they pass either way, which is exactly why these exist.
 *
 * <ol>
 *   <li><b>No span retention.</b> A snapshot must not keep the turn's {@code Span} objects — and
 *       with them whole prompts held in {@code gen_ai.input.messages} — reachable. Otherwise a host
 *       that schedules a long-delayed task during a turn pins that memory until the task runs.</li>
 *   <li><b>No misfiling across turns of one session.</b> Recovering the turn by looking it up from
 *       the session index on the worker thread would return whatever turn is <em>current</em> at
 *       that moment; when the same user has already sent their next message, that is turn N+1, and
 *       turn N's flush would be filed under it.</li>
 * </ol>
 */
@SuppressWarnings("removal")
class ClsTurnSnapshotTest {

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
                    .id("f")
                    .content(List.<ContentBlock>of(TextBlock.builder().text("ok").build()))
                    .usage(new ChatUsage(10, 2, 0.0))
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
     * A snapshot captured inside a turn must not hold anything that can reach the turn's spans.
     *
     * <p>Allowed: our id strings, a {@code SpanContext} (a value: trace id, span id, flags), and
     * the turn's {@code AtomicInteger} framework-step counter. That last one is a reference, but a
     * standalone object — holding it does not keep the declaring {@code TurnState} reachable — and
     * it has to be shared so step numbers are claimed on use rather than on capture.
     *
     * <p>Banned: {@code Span}, OTel {@code Context} (which transitively reaches spans), and the
     * {@code TurnState} itself. Those are what would pin whole prompts in memory until a
     * long-delayed task runs.
     */
    @Test
    void snapshotHoldsNoReferenceToSpansOrTurnState() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        AtomicReference<Object> captured = new AtomicReference<>();

        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("snap-s").userId("u").build(),
                        new AgentInput(List.of(msg("a very long prompt ".repeat(50)))),
                        in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r", "b", "hi"))
                                // Inside signal processing the turn is on this thread, which is
                                // exactly when the Scheduler hook takes its snapshot.
                                .doOnNext(ev -> captured.set(
                                        ClsTracingMiddleware.captureTurnContextForHop())))
                .blockLast();

        Object snapshot = captured.get();
        assertNotNull(snapshot, "no snapshot was captured inside the turn");

        for (java.lang.reflect.Field f : snapshot.getClass().getDeclaredFields()) {
            f.setAccessible(true);
            Class<?> type = f.getType();
            assertTrue(
                    type == String.class
                            || java.util.concurrent.atomic.AtomicInteger.class == type
                            || io.opentelemetry.api.trace.SpanContext.class.isAssignableFrom(type),
                    "snapshot field '" + f.getName() + "' is a " + type.getName()
                            + "; only id strings, an AtomicInteger sequence and a SpanContext are "
                            + "allowed — anything else can pin the turn's spans and their prompts");

            Object value = f.get(snapshot);
            if (value != null) {
                assertTrue(!(value instanceof io.opentelemetry.api.trace.Span),
                        "snapshot field '" + f.getName() + "' holds a Span");
                assertTrue(!(value instanceof io.opentelemetry.context.Context),
                        "snapshot field '" + f.getName() + "' holds an OTel Context, which "
                                + "transitively reaches the turn's spans");
            }
        }
    }

    /**
     * The first framework call of a turn must be {@code :f1}.
     *
     * <p>The Scheduler hook snapshots every task submitted during a turn, but only the few that
     * reach a model call ever synthesise a step. Claiming the sequence number when the snapshot is
     * taken therefore burned one per submitted task and left gaps — the first memory flush came
     * out as {@code :f3}. The number must be drawn when the step is actually created.
     */
    @Test
    void frameworkStepNumbersAreClaimedOnUseNotOnCapture() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        FlushModel model = new FlushModel();
        CountDownLatch flushed = new CountDownLatch(1);

        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId("seq-s").userId("u").build(),
                        new AgentInput(List.of(msg("hi"))),
                        in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r", "b", "yo"))
                                // Several Scheduler hops happen before the flush: each one is
                                // snapshotted, and none of them may consume a step number.
                                .publishOn(Schedulers.boundedElastic())
                                .publishOn(Schedulers.parallel())
                                .publishOn(Schedulers.boundedElastic())
                                .doOnComplete(() -> Mono.defer(() ->
                                                model.stream(List.of(msg("memory extraction")),
                                                        List.of(), null).then())
                                        .subscribeOn(Schedulers.boundedElastic())
                                        .doFinally(sig -> flushed.countDown())
                                        .subscribe(v -> { }, err -> flushed.countDown())))
                .blockLast();

        assertTrue(flushed.await(10, TimeUnit.SECONDS), "the flush never ran");

        SpanData flush = exporter.spans().stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the flush produced no span"));

        assertEquals("seq-s:t1:f1", str(flush, "gen_ai.step.id"),
                "the turn's first framework step must be :f1 — a higher number means a sequence "
                        + "number was consumed by a Scheduler hop that never created a step");
    }

    /**
     * One session, two consecutive turns. Turn 1's flush is submitted while turn 1 is in scope but
     * only runs after turn 2 has started and claimed the session index. It must still be filed
     * under turn 1.
     *
     * <p>This is the case that rules out "store ids, look the turn back up": the index holds a
     * single entry per session, so by the time the flush runs the lookup would yield turn 2.
     */
    @Test
    void flushIsFiledUnderItsOwnTurnNotTheSessionsLatest() throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        String session = "feishu:ou_same_user";
        FlushModel model = new FlushModel();

        // Gate the flush so it cannot run until turn 2 has begun.
        CountDownLatch turn2Started = new CountDownLatch(1);
        CountDownLatch flushDone = new CountDownLatch(1);

        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId(session).userId("u").build(),
                        new AgentInput(List.of(msg("first question"))),
                        in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r1", "b", "a1"))
                                .doOnComplete(() -> Mono.defer(() -> {
                                            // Runs on the worker: wait until turn 2 owns the
                                            // session index, then make the model call.
                                            try {
                                                turn2Started.await(5, TimeUnit.SECONDS);
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                            }
                                            return model.stream(
                                                    List.of(msg("memory extraction")),
                                                    List.of(), null).then();
                                        })
                                        .subscribeOn(Schedulers.boundedElastic())
                                        .doFinally(sig -> flushDone.countDown())
                                        .subscribe(v -> { }, err -> flushDone.countDown())))
                .blockLast();

        // Turn 2 of the SAME session: this overwrites the session index entry.
        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId(session).userId("u").build(),
                        new AgentInput(List.of(msg("second question"))),
                        in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r2", "b", "a2"))
                                .doOnNext(ev -> turn2Started.countDown()))
                .blockLast();

        assertTrue(flushDone.await(10, TimeUnit.SECONDS), "the gated flush never ran");

        List<SpanData> spans = exporter.spans();
        SpanData flush = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "the gated flush produced no span; spans=" + names(spans)));

        // Turn ids are {sessionId}:t{N}; turn 1 is the first turn this middleware ran.
        String turn1 = session + ":t1";
        String turn2 = session + ":t2";
        assertEquals(turn1, str(flush, "gen_ai.turn.id"),
                "the flush belongs to turn 1 — it was submitted while turn 1 was in scope. "
                        + "Seeing turn 2 here means the turn was resolved by looking up the "
                        + "session index on the worker thread, which races with the next turn.");
        assertNotEquals(turn2, str(flush, "gen_ai.turn.id"));

        // And it must be on turn 1's trace, not turn 2's.
        SpanData turn1Entry = spans.stream()
                .filter(s -> "entry".equals(str(s, "gen_ai.span.kind")))
                .filter(s -> turn1.equals(str(s, "gen_ai.turn.id")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no entry span for turn 1"));
        assertEquals(turn1Entry.getTraceId(), flush.getTraceId(),
                "the flush span must sit on turn 1's trace");
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
