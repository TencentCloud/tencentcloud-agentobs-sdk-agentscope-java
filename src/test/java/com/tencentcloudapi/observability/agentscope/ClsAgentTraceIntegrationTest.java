package com.tencentcloudapi.observability.agentscope;

import com.tencentcloudapi.observability.agentscope.example.MockChatModel;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.middleware.MiddlewareBase;
import io.agentscope.core.model.Model;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.core.tool.Toolkit;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end check against a real {@link ReActAgent}: one turn must yield one connected trace in
 * which the framework's own memory/summarisation model call — which bypasses the middleware chain
 * entirely — appears alongside the regular agent spans and carries the turn's identity.
 *
 * <p>This is the scenario the demo exercises manually, pinned here so it cannot regress.
 */
@SuppressWarnings("removal") // TracerRegistry is deprecated-for-removal but still the live hook
class ClsAgentTraceIntegrationTest {

    /**
     * {@code entry + agent + 2×step + 2×chat + tool + framework step + framework chat}. Spans end
     * on the agent's reactive {@code doFinally} callbacks, which may run slightly after
     * {@code call().block()} returns, so the assertions wait for this many before inspecting.
     */
    private static final int EXPECTED_SPAN_COUNT = 9;

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

    public static final class WeatherTool {
        @Tool(name = "get_weather", description = "Get the current weather for a given city.")
        public String getWeather(
                @ToolParam(name = "city", description = "City name") String city) {
            return "{\"city\":\"" + city + "\",\"temp\":\"22C\"}";
        }
    }

    /**
     * Mimics AgentScope's memory / history-summarisation components: a model call issued on the
     * agent's reactive chain but NOT through any middleware hook, using the framework's own prompt.
     */
    private static final class MemoryFlushSimulator implements MiddlewareBase {
        private final Model model;

        MemoryFlushSimulator(Model model) {
            this.model = model;
        }

        @Override
        public Flux<AgentEvent> onAgent(Agent agent, RuntimeContext ctx, AgentInput input,
                                        Function<AgentInput, Flux<AgentEvent>> next) {
            Msg prompt = Msg.builder()
                    .role(MsgRole.USER)
                    .content(TextBlock.builder()
                            .text("[framework default prompt] Extract durable facts.")
                            .build())
                    .build();
            return model.stream(List.of(prompt), List.of(), null)
                    .thenMany(Flux.<AgentEvent>empty())
                    .concatWith(next.apply(input));
        }
    }

    @Test
    void frameworkModelCallJoinsTheTurnTrace() throws Exception {
        MockChatModel chatModel = new MockChatModel(
                "mock-gpt-4o", "get_weather", Map.of("city", "Beijing"), "Sunny, 22°C.");
        MockChatModel memoryModel = new MockChatModel(
                "mock-memory-model", null, Map.of(), "User asked about Beijing weather.");

        Toolkit toolkit = new Toolkit();
        toolkit.registerTool(new WeatherTool());

        ReActAgent agent = ReActAgent.builder()
                .name("assistant")
                .sysPrompt("You are a helpful assistant.")
                .description("Answers weather questions for a given city")
                .model(chatModel)
                .toolkit(toolkit)
                .middleware((MiddlewareBase) new ClsTracingMiddleware(tracer))
                .middleware(new MemoryFlushSimulator(memoryModel))
                .maxIters(5)
                .build();

        String sessionId = "it-session-1";
        agent.call("What's the weather in Beijing?",
                RuntimeContext.builder().sessionId(sessionId).userId("it-user").build()).block();
        agent.close();

        List<SpanData> spans = exporter.awaitSpans(EXPECTED_SPAN_COUNT);

        // One turn ⇒ one trace, every span included.
        assertEquals(1, spans.stream().map(SpanData::getTraceId).distinct().count(),
                "all spans of a turn must share one trace, got: " + names(spans));

        SpanData framework = spans.stream()
                .filter(s -> "framework".equals(str(s, "gen_ai.model.call.origin")))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "the middleware-bypassing model call produced no span; got: "
                                + names(spans)));

        assertEquals("chat mock-memory-model", framework.getName());
        assertEquals(sessionId, str(framework, "gen_ai.session.id"),
                "the framework call must be attributable to the turn's session");
        assertNotNull(str(framework, "gen_ai.turn.id"));

        String input = str(framework, "gen_ai.input.messages");
        assertNotNull(input);
        assertTrue(input.contains("framework default prompt"),
                "the framework's own prompt must be captured, got: " + input);
        assertTrue(str(framework, "gen_ai.output.messages").contains("Beijing weather"),
                "the memory model's reply must be captured");

        // The regular agent model calls must still be traced exactly once each — the framework
        // tracer must not add a second chat span on top of the middleware's.
        assertEquals(2, spans.stream()
                        .filter(s -> "chat".equals(str(s, "gen_ai.span.kind")))
                        .filter(s -> str(s, "gen_ai.model.call.origin") == null)
                        .count(),
                "expected exactly one chat span per ReAct round, got: " + names(spans));

        // The expected hierarchy is intact around it.
        assertEquals(1, count(spans, "entry"));
        assertEquals(1, count(spans, "agent"));
        assertEquals(1, count(spans, "tool"));
        // 2 ReAct rounds + 1 step synthesised to hold the framework call.
        assertEquals(3, count(spans, "step"));

        // Identity read off the real agent and its real toolkit, not off our own test doubles:
        // what distinguishes agents and tools in a trace where names alone are generic.
        SpanData agentSpan = spans.stream()
                .filter(s -> "agent".equals(str(s, "gen_ai.span.kind")))
                .findFirst().orElseThrow();
        assertEquals("Answers weather questions for a given city",
                str(agentSpan, "gen_ai.agent.description"),
                "the agent's configured description must be recorded");

        SpanData toolSpan = spans.stream()
                .filter(s -> "tool".equals(str(s, "gen_ai.span.kind")))
                .findFirst().orElseThrow();
        // Resolved by name against the Toolkit, since ActingInput carries only ToolUseBlocks.
        // Tells apart "the model picked the wrong tool" from "the description misled it".
        assertNotNull(str(toolSpan, "gen_ai.tool.description"),
                "the invoked tool's declared description must be recorded");
        assertTrue(str(toolSpan, "gen_ai.tool.description").toLowerCase().contains("weather"),
                "got: " + str(toolSpan, "gen_ai.tool.description"));

        // Per the CLS GenAI spec a chat span hangs off a step, never off the agent directly.
        SpanData frameworkStep = spans.stream()
                .filter(s -> s.getSpanId().equals(framework.getParentSpanId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("framework chat span has no parent in trace"));
        assertEquals("step", str(frameworkStep, "gen_ai.span.kind"),
                "the framework chat must nest under a step, not under " + frameworkStep.getName());
        assertEquals("framework", str(frameworkStep, "gen_ai.step.type"),
                "the synthesised step must be distinguishable from a real ReAct round");
        assertEquals(str(framework, "gen_ai.step.id"), str(frameworkStep, "gen_ai.step.id"),
                "chat and its step must agree on step.id");

        // The synthesised step must not disturb ReAct round numbering.
        assertEquals(List.of("react round_1", "react round_2"), spans.stream()
                        .filter(s -> "step".equals(str(s, "gen_ai.span.kind")))
                        .filter(s -> str(s, "gen_ai.step.type") == null)
                        .map(SpanData::getName)
                        .sorted()
                        .collect(Collectors.toList()));

        // The step must fully contain its chat span in time.
        assertTrue(frameworkStep.getStartEpochNanos() <= framework.getStartEpochNanos()
                        && frameworkStep.getEndEpochNanos() >= framework.getEndEpochNanos(),
                "the synthesised step must enclose the chat span it was created for");
    }

    /**
     * An agent with no description configured must carry <b>no</b> {@code gen_ai.agent.description}.
     *
     * <p>{@code Agent.getDescription()} is a {@code default} method that synthesises
     * {@code "Agent({agentId}) {name}"} when unset, so it never returns null. Copying that
     * blindly writes a value onto every agent span which merely restates
     * {@code gen_ai.agent.id} and {@code gen_ai.agent.name} — already present on the very same
     * span — while making the field look populated. Observed on a real demo run before the
     * filter was added.
     */
    @Test
    void anAgentWithoutADescriptionCarriesNoDescriptionAttribute() throws Exception {
        MockChatModel chatModel = new MockChatModel(
                "mock-gpt-4o", null, Map.of(), "Sunny, 22°C.");

        ReActAgent agent = ReActAgent.builder()
                .name("assistant")
                .sysPrompt("You are a helpful assistant.")
                // deliberately no .description(...)
                .model(chatModel)
                .toolkit(new Toolkit())
                .middleware((MiddlewareBase) new ClsTracingMiddleware(tracer))
                .maxIters(2)
                .build();

        agent.call("hi", RuntimeContext.builder()
                .sessionId("desc-session").userId("u").build()).block();
        agent.close();

        SpanData agentSpan = exporter.spans().stream()
                .filter(s -> "agent".equals(str(s, "gen_ai.span.kind")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no agent span produced"));

        assertNull(str(agentSpan, "gen_ai.agent.description"),
                "the synthesised \"Agent(id) name\" placeholder must not be recorded; got: "
                        + str(agentSpan, "gen_ai.agent.description"));
        // The real identity fields are still there — nothing was lost by filtering.
        assertNotNull(str(agentSpan, "gen_ai.agent.id"));
        assertEquals("assistant", str(agentSpan, "gen_ai.agent.name"));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static long count(List<SpanData> spans, String kind) {
        return spans.stream().filter(s -> kind.equals(str(s, "gen_ai.span.kind"))).count();
    }

    private static String str(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.stringKey(key));
    }

    private static String names(List<SpanData> spans) {
        return spans.stream().map(SpanData::getName).collect(Collectors.joining(", "));
    }

    private static final class CollectingExporter implements SpanExporter {
        private final List<SpanData> collected = new CopyOnWriteArrayList<>();

        List<SpanData> spans() {
            return new ArrayList<>(collected);
        }

        /**
         * Wait until {@code expected} spans have ended, or ~5s elapse. Returns whatever was
         * collected so the assertions — not a timeout — report what is actually missing.
         */
        List<SpanData> awaitSpans(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
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
