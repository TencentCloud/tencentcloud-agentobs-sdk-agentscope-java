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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@code CLS_PROPAGATE_ACROSS_SCHEDULERS} / {@code .propagateAcrossSchedulers(Boolean)}.
 *
 * <p>The switch exists because carrying turn context across a thread hop requires a
 * <b>process-wide</b> {@code Schedulers.onScheduleHook}: every task submitted to any Reactor
 * Scheduler in the host application passes through it. A deployment that cannot accept that must
 * be able to decline — and when it does, framework model calls have to keep being <em>traced</em>,
 * just without turn correlation. Silently losing the spans, or silently keeping the hook, would
 * both be wrong.
 */
@SuppressWarnings("removal")
class ClsSchedulerPropagationConfigTest {

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
    }

    @AfterEach
    void tearDown() {
        // Remove our Scheduler hook so one case cannot leak into the next.
        Schedulers.resetOnScheduleHooks();
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

    // ── config resolution ────────────────────────────────────────────────

    @Test
    void defaultsToEnabled() {
        assertTrue(ClsConfig.builder()
                        .endpoint("https://example.com").topicId("t")
                        .secretId("i").secretKey("k")
                        .build()
                        .propagateAcrossSchedulers(),
                "Scheduler propagation must be on by default — it is what correlates the "
                        + "framework's post-turn memory calls on a multi-session service");
    }

    @Test
    void builderCanDisableIt() {
        assertFalse(ClsConfig.builder()
                        .endpoint("https://example.com").topicId("t")
                        .secretId("i").secretKey("k")
                        .propagateAcrossSchedulers(false)
                        .build()
                        .propagateAcrossSchedulers());
    }

    @Test
    void builderWinsOverEverythingElse() {
        // Explicit true must survive even if the environment says otherwise.
        assertTrue(ClsConfig.builder()
                .endpoint("https://example.com").topicId("t")
                .secretId("i").secretKey("k")
                .propagateAcrossSchedulers(true)
                .build()
                .propagateAcrossSchedulers());
    }

    // ── behaviour ────────────────────────────────────────────────────────

    /**
     * With propagation ON, a detached post-turn flush is correlated even though several sessions
     * are tracked — the snapshot taken on the submitting thread names the right turn.
     */
    @Test
    void enabledCorrelatesDetachedFlushAcrossSessions() throws Exception {
        ClsFrameworkTracer.install(tracer, true);
        assertEquals("flush-s", runFlushAndReadSessionId("flush-s", "other-s"),
                "with propagation on, the flush must carry its own turn's session id");
    }

    /**
     * With propagation OFF, the same call must still produce a span — only uncorrelated. This is
     * the contract of the switch: you lose attribution, not data.
     */
    @Test
    void disabledStillTracesTheCallButWithoutCorrelation() throws Exception {
        ClsFrameworkTracer.install(tracer, false);
        assertNull(runFlushAndReadSessionId("flush-s", "other-s"),
                "with propagation off the flush must remain unattributed (two sessions are "
                        + "tracked, so no honest fallback exists) — but the span must still exist");
    }

    /**
     * Run one turn per session, with the LAST one dispatching a detached post-turn flush, then
     * return the flush span's {@code gen_ai.session.id} (possibly {@code null}).
     *
     * @param flushSession the session whose turn schedules the flush
     * @param otherSessions further sessions, so the single-session fallback cannot apply
     */
    private String runFlushAndReadSessionId(String flushSession, String... otherSessions)
            throws Exception {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);

        for (String other : otherSessions) {
            middleware.onAgent(null,
                            RuntimeContext.builder().sessionId(other).userId("u").build(),
                            new AgentInput(List.of(msg("hi"))),
                            in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r", "b", "yo")))
                    .blockLast();
        }

        FlushModel model = new FlushModel();
        CountDownLatch flushed = new CountDownLatch(1);
        middleware.onAgent(null,
                        RuntimeContext.builder().sessionId(flushSession).userId("u").build(),
                        new AgentInput(List.of(msg("weather?"))),
                        in -> Flux.<AgentEvent>just(new TextBlockDeltaEvent("r", "b", "sunny"))
                                .doOnComplete(() -> Mono.defer(() ->
                                                model.stream(List.of(msg("memory extraction")),
                                                        List.of(), null).then())
                                        .subscribeOn(Schedulers.boundedElastic())
                                        .doFinally(sig -> flushed.countDown())
                                        .subscribe(v -> { }, err -> flushed.countDown())))
                .blockLast();

        assertTrue(flushed.await(10, TimeUnit.SECONDS), "the flush never ran");

        List<SpanData> spans = exporter.spans();
        SpanData flush = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "the framework model call produced no span at all; disabling Scheduler "
                                + "propagation must not stop calls being traced. spans="
                                + names(spans)));
        assertNotNull(str(flush, "gen_ai.input.messages"),
                "the call's content must be captured either way");
        return str(flush, "gen_ai.session.id");
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
