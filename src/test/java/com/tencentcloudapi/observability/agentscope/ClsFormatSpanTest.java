package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.ModelCallEndEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.formatter.AbstractBaseFormatter;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.ModelCallInput;
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
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the {@code format} span — prompt formatting, the one AgentScope stage
 * {@code MiddlewareBase} exposes no hook for (it has only {@code onAgent} / {@code onReasoning} /
 * {@code onActing} / {@code onModelCall} / {@code onSystemPrompt}), so it is reachable solely
 * through {@code Tracer.callFormat}.
 *
 * <p><b>The assumption under test.</b> {@code Tracer.callFormat} is handed no {@code ContextView},
 * so the owning round can only be found through the ThreadLocal bridge that
 * {@code ClsFrameworkTracer.runWithContext} maintains. Whether that bridge is actually populated
 * at the moment a formatter runs — formatting happens during the <em>assembly</em> of the model
 * Flux, not while a signal is being delivered — is not obvious, and if it were not, these spans
 * would silently become orphan traces. {@link #formatSpanIsAttachedToTheRoundThatTriggeredIt}
 * pins it down.
 */
@SuppressWarnings("removal")
class ClsFormatSpanTest {

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
        TracerRegistry.resetToNoop();
        tracerProvider.close();
    }

    /**
     * A formatter whose {@code format} is reached through {@code AbstractBaseFormatter}, i.e. via
     * {@code TracerRegistry.get().callFormat(...)} — the same route a provider's formatter takes.
     */
    private static final class StubFormatter extends AbstractBaseFormatter<String, String, String> {
        @Override
        protected List<String> doFormat(List<Msg> msgs) {
            List<String> out = new ArrayList<>();
            for (Msg m : msgs) {
                out.add(m.getTextContent());
            }
            // One extra entry, so format.count differs from input_count and we can tell which
            // side of the call each attribute came from.
            out.add("<<provider-envelope>>");
            return out;
        }

        @Override
        public ChatResponse parseResponse(String resp, java.time.Instant start) {
            throw new UnsupportedOperationException("not needed: only format() is under test");
        }

        @Override
        public void applyOptions(String params, GenerateOptions a, GenerateOptions b) {
        }

        @Override
        public void applyTools(String params, List<ToolSchema> tools) {
        }
    }

    /** Runs the formatter from inside the model call, like {@code OpenAIChatModel.doStream0}. */
    private static final class FormattingModel extends ChatModelBase {
        private final StubFormatter formatter = new StubFormatter();

        @Override
        public String getModelName() {
            return "formatting-model";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> msgs, List<ToolSchema> tools, GenerateOptions options) {
            // Assembly-time formatting, matching the real provider: the call happens in the
            // method body, before anything subscribes.
            formatter.format(msgs);
            return Flux.just(ChatResponse.builder()
                    .id("fmt-1")
                    .content(List.<ContentBlock>of(TextBlock.builder().text("ok").build()))
                    .usage(new ChatUsage(10, 5, 0.0))
                    .finishReason("stop")
                    .build());
        }
    }

    private static List<Msg> messages() {
        return List.of(
                Msg.builder()
                        .role(MsgRole.SYSTEM)
                        .content(TextBlock.builder().text("be terse").build())
                        .build(),
                Msg.builder()
                        .role(MsgRole.USER)
                        .content(TextBlock.builder().text("hi").build())
                        .build());
    }

    /**
     * The core guarantee: inside a live round, a formatter call produces a {@code format} span on
     * the turn's trace, parented at that round's {@code step} and carrying its identity — not an
     * orphan trace.
     */
    @Test
    void formatSpanIsAttachedToTheRoundThatTriggeredIt() {
        runTurnWithFormattingModel(true);

        SpanData format = span("format").orElseThrow(
                () -> new AssertionError("no format span was produced; spans=" + names()));
        SpanData step = span("step").orElseThrow(
                () -> new AssertionError("no step span; spans=" + names()));

        assertEquals(step.getSpanId(), format.getParentSpanId(),
                "the format span must hang off the round that triggered it");
        assertEquals(step.getTraceId(), format.getTraceId(),
                "the format span must stay on the turn's trace, not form its own");
        assertEquals("fmt-s", str(format, "gen_ai.session.id"));
        assertNotNull(str(format, "gen_ai.turn.id"));
        assertEquals(str(step, "gen_ai.step.id"), str(format, "gen_ai.step.id"));
    }

    /**
     * {@code format} is a <em>sibling</em> of {@code chat}, not its parent or child: formatting
     * runs while the model Flux is being assembled, before the chat span is opened. Both therefore
     * share the round's step as their parent.
     */
    @Test
    void formatSpanIsASiblingOfTheChatSpanUnderTheSameStep() {
        runTurnWithFormattingModel(true);

        SpanData format = span("format").orElseThrow(AssertionError::new);
        SpanData chat = span("chat").orElseThrow(AssertionError::new);

        assertEquals(chat.getParentSpanId(), format.getParentSpanId(),
                "format and chat must share the step as parent");
        assertFalse(format.getSpanId().equals(chat.getParentSpanId()),
                "the chat span must not be nested under the format span");
        assertFalse(chat.getSpanId().equals(format.getParentSpanId()),
                "the format span must not be nested under the chat span");
    }

    /** Counts come from both sides of the call, so each is checked against the other. */
    @Test
    void formatSpanRecordsInputAndOutputCounts() {
        runTurnWithFormattingModel(true);
        SpanData format = span("format").orElseThrow(AssertionError::new);

        assertEquals(2L, lng(format, "agentscope.format.input_count"));
        assertEquals(3L, lng(format, "agentscope.format.count"),
                "the formatter's own output size must be recorded, not the input size");
        assertEquals("StubFormatter", str(format, "agentscope.format.formatter"));
    }

    /** Disabled by default, because a formatter runs on every single model call. */
    @Test
    void noFormatSpanIsProducedWhenTheFeatureIsOff() {
        runTurnWithFormattingModel(false);

        assertTrue(span("format").isEmpty(),
                "format spans must be opt-in; spans=" + names());
        assertTrue(span("chat").isPresent(),
                "disabling format spans must not affect the chat span; spans=" + names());
    }

    /**
     * A formatter invoked with no round in flight is not traced at all. Attributing it by guesswork
     * would emit a stream of mis-parented spans — one per model call — and a {@code format} span
     * detached from the {@code chat} it prepared carries no diagnostic value.
     */
    @Test
    void formatOutsideATurnIsNotTraced() {
        ClsFrameworkTracer.install(tracer, true, true);

        List<String> formatted = new StubFormatter().format(messages());

        assertEquals(3, formatted.size(), "formatting itself must still work");
        assertTrue(span("format").isEmpty(),
                "an unattributable format call must be dropped, not orphaned; spans=" + names());
    }

    /**
     * <b>A known gap, pinned here deliberately.</b> A model call that bypasses the middleware chain
     * (the framework's own memory / summarisation work, {@code gen_ai.model.call.origin=framework})
     * gets a {@code chat} span but <em>no</em> {@code format} span, even though its provider
     * formats a prompt just the same.
     *
     * <p>Two reasons, both structural. Such a call never passes through {@code onModelCall}, so the
     * assembly window of {@link ClsTracingMiddleware#enterModelAssembly} is never opened. And it is
     * by definition not inside a ReAct round — it runs at a turn boundary or after the turn — so
     * there is no live step for {@link ClsTracingMiddleware#currentFormatStep} to return. The
     * {@code framework_step} that hosts its {@code chat} span is synthesised later, when the call
     * is resolved at subscribe time, by which point formatting has already happened.
     *
     * <p>Confirmed end-to-end: a demo turn issuing two ReAct calls and two framework calls produces
     * four {@code chat} spans but only two {@code format} spans. Closing the gap would mean
     * synthesising a step during assembly, before it is known whether the call is even traceable —
     * not worth it for a span whose content is a re-encoding of the sibling {@code chat}'s input.
     */
    @Test
    void frameworkOriginCallsGetNoFormatSpan() {
        ClsFrameworkTracer.install(tracer, true, true);
        // A bare model call, exactly like the framework's memory work: no middleware above it.
        new FormattingModel().stream(messages(), List.of(), null).blockLast();

        assertTrue(span("chat").isPresent(),
                "the framework model call itself must still be traced; spans=" + names());
        assertTrue(span("format").isEmpty(),
                "a framework-origin call has no live round, so its formatter is not traced; "
                        + "spans=" + names());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /** Drives a full turn whose model call formats its prompt, as a real provider does. */
    private void runTurnWithFormattingModel(boolean captureFormatCalls) {
        ClsFrameworkTracer.install(tracer, true, captureFormatCalls);
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        RuntimeContext ctx = RuntimeContext.builder().sessionId("fmt-s").userId("u").build();
        FormattingModel model = new FormattingModel();

        middleware.onAgent(null, ctx,
                        new io.agentscope.core.middleware.AgentInput(messages()),
                        agentIn -> middleware.onReasoning(null, ctx,
                                new io.agentscope.core.middleware.ReasoningInput(
                                        messages(), List.of(), null),
                                reasoningIn -> middleware.onModelCall(null, ctx,
                                        new ModelCallInput(messages(), List.of(), null, model),
                                        modelIn -> model
                                                .stream(modelIn.messages(), modelIn.tools(), null)
                                                .thenMany(Flux.<AgentEvent>just(
                                                        new TextBlockDeltaEvent("r1", "b1", "ok"),
                                                        new ModelCallEndEvent("r1",
                                                                new ChatUsage(10, 5, 0.0)))))))
                .blockLast();
    }

    private Optional<SpanData> span(String kind) {
        return exporter.spans().stream()
                .filter(s -> kind.equals(str(s, "gen_ai.span.kind")))
                .findFirst();
    }

    private String names() {
        return exporter.spans().stream().map(SpanData::getName)
                .collect(java.util.stream.Collectors.joining(", "));
    }

    private static String str(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.stringKey(key));
    }

    private static Long lng(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.longKey(key));
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
