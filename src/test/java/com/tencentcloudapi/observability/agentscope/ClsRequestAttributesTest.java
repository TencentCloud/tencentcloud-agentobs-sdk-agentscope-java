package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.ModelCallEndEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
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
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down the request-side attributes of a {@code chat} span — the sampling parameters, the tool
 * schemas offered to the model, and the system prompt.
 *
 * <p><b>Why these matter.</b> Without them a recorded call is not reproducible (the same prompt at
 * a different temperature is a different experiment) and a model that failed to call an expected
 * tool is indistinguishable from one that was never offered it.
 *
 * <p>Both paths are covered, because they are separate code paths that have drifted before: the
 * middleware's {@code onModelCall} and {@link ClsFrameworkTracer}'s framework-level hook.
 *
 * <p>The most important test here is {@link #credentialsAreNeverWrittenToASpan()}: the same
 * {@code GenerateOptions} object that carries {@code temperature} also carries {@code apiKey} and
 * {@code baseUrl}.
 */
@SuppressWarnings("removal")
class ClsRequestAttributesTest {

    private static final String SECRET_KEY = "sk-must-never-be-exported-7f3a9c";
    private static final String SECRET_URL = "https://internal-gateway.example.invalid/v1";

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

    /** Every sampling knob set here must surface on the span, plus the credentials that must not. */
    private static GenerateOptions options() {
        return GenerateOptions.builder()
                .temperature(0.42)
                .topP(0.9)
                .topK(40)
                .maxTokens(2048)
                .seed(12345L)
                .frequencyPenalty(0.25)
                .presencePenalty(0.5)
                .apiKey(SECRET_KEY)
                .baseUrl(SECRET_URL)
                .build();
    }

    private static List<ToolSchema> tools() {
        return List.of(ToolSchema.builder()
                .name("get_weather")
                .description("Look up the forecast for a city")
                .parameters(Map.of(
                        "type", "object",
                        "properties", Map.of("city", Map.of("type", "string"))))
                .build());
    }

    private static List<Msg> messages() {
        return List.of(
                Msg.builder()
                        .role(MsgRole.SYSTEM)
                        .content(TextBlock.builder().text("You are a terse assistant.").build())
                        .build(),
                Msg.builder()
                        .role(MsgRole.USER)
                        .content(TextBlock.builder().text("weather in Shenzhen?").build())
                        .build());
    }

    // ── middleware path ──────────────────────────────────────────────────

    @Test
    void middlewareChatSpanCarriesSamplingParametersToolsAndSystemPrompt() {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        runMiddlewareModelCall(middleware);

        SpanData chat = chatSpan();
        assertSamplingParameters(chat);
        assertToolDefinitions(chat);
        assertSystemInstructions(chat);
    }

    /** Mean decode latency needs output tokens, which only arrive with ModelCallEndEvent. */
    @Test
    void middlewareChatSpanReportsTimePerOutputToken() {
        ClsTracingMiddleware middleware = new ClsTracingMiddleware(tracer);
        runMiddlewareModelCall(middleware);

        assertNotNull(dbl(chatSpan(), "gen_ai.response.time_per_output_token_ms"),
                "mean per-token decode latency must be derived from usage.output_tokens");
    }

    // ── framework-tracer path ────────────────────────────────────────────

    @Test
    void frameworkChatSpanCarriesSamplingParametersToolsAndSystemPrompt() {
        ClsFrameworkTracer.install(tracer);
        new StubModel().stream(messages(), tools(), options()).blockLast();

        SpanData chat = chatSpan();
        assertSamplingParameters(chat);
        assertToolDefinitions(chat);
        assertSystemInstructions(chat);
    }

    /**
     * The provider's completion id is the only field that lets a span be reconciled against the
     * model vendor's billing records. It is reachable on this path only: the middleware path sees
     * {@link ModelCallEndEvent}, which carries no response id.
     */
    @Test
    void frameworkChatSpanCarriesProviderResponseId() {
        ClsFrameworkTracer.install(tracer);
        new StubModel().stream(messages(), tools(), options()).blockLast();

        assertEquals("resp-abc-123", str(chatSpan(), "gen_ai.response.id"));
    }

    // ── the security invariant ───────────────────────────────────────────

    /**
     * {@code GenerateOptions} exposes {@code getApiKey()} and {@code getBaseUrl()} alongside the
     * sampling parameters. Serialising it wholesale — or adding a "copy every field" loop as a
     * convenience later — would publish the caller's API key to CLS, where it is durably stored
     * and broadly readable. This test fails on any attribute, on any span, that contains either.
     */
    @Test
    void credentialsAreNeverWrittenToASpan() {
        ClsFrameworkTracer.install(tracer);
        new StubModel().stream(messages(), tools(), options()).blockLast();
        runMiddlewareModelCall(new ClsTracingMiddleware(tracer));

        for (SpanData span : exporter.spans()) {
            span.getAttributes().forEach((key, value) -> {
                String rendered = String.valueOf(value);
                assertTrue(!rendered.contains(SECRET_KEY),
                        "API key leaked into " + span.getName() + "/" + key.getKey());
                assertTrue(!rendered.contains(SECRET_URL),
                        "base URL leaked into " + span.getName() + "/" + key.getKey());
            });
        }
        // Guard the obvious key names too, in case a future change renames rather than inlines.
        SpanData chat = chatSpan();
        assertNull(str(chat, "gen_ai.request.api_key"));
        assertNull(str(chat, "gen_ai.request.base_url"));
    }

    // ── shared assertions ────────────────────────────────────────────────

    private static void assertSamplingParameters(SpanData chat) {
        assertEquals(0.42, dbl(chat, "gen_ai.request.temperature"));
        assertEquals(0.9, dbl(chat, "gen_ai.request.top_p"));
        assertEquals(40L, lng(chat, "gen_ai.request.top_k"));
        assertEquals(2048L, lng(chat, "gen_ai.request.max_tokens"));
        assertEquals(12345L, lng(chat, "gen_ai.request.seed"));
        assertEquals(0.25, dbl(chat, "gen_ai.request.frequency_penalty"));
        assertEquals(0.5, dbl(chat, "gen_ai.request.presence_penalty"));
    }

    private static void assertToolDefinitions(SpanData chat) {
        assertEquals(1L, lng(chat, "gen_ai.request.tool_count"));
        String defs = str(chat, "gen_ai.tool.definitions");
        assertNotNull(defs, "the tool schemas offered to the model must be recorded");
        assertTrue(defs.contains("get_weather"), "tool name missing: " + defs);
        assertTrue(defs.contains("Look up the forecast"), "tool description missing: " + defs);
        // The parameter schema is the part that explains *why* a model called a tool wrongly.
        assertTrue(defs.contains("city"), "tool parameter schema missing: " + defs);
    }

    private static void assertSystemInstructions(SpanData chat) {
        String sys = str(chat, "gen_ai.system_instructions");
        assertNotNull(sys, "the system prompt must be readable without parsing input.messages");
        assertTrue(sys.contains("terse assistant"), "system prompt missing: " + sys);
        assertTrue(sys.contains("\"system\""), "role should be preserved: " + sys);
        // input.messages stays the verbatim record of the call, system message included.
        assertTrue(str(chat, "gen_ai.input.messages").contains("terse assistant"),
                "input.messages must remain the full record of what was sent");
    }

    // ── provider identity ────────────────────────────────────────────────

    /**
     * {@code gen_ai.provider.name} is derived from the model's implementing class, because
     * AgentScope exposes only a model <em>name</em> ({@code hy4-preview}, {@code qwen-plus}) —
     * which says nothing about who served it.
     *
     * <p>Asserted against the real fully-qualified names so the mapping is checked the way
     * production sees it, rather than against a test double whose nested-class name
     * ({@code ...Test$OpenAIChatModel}) matches neither rule.
     */
    @Test
    void providerNameIsNormalisedFromTheImplementingClass() {
        assertEquals("openai", ClsTracingMiddleware.providerNameOf(
                "io.agentscope.extensions.model.openai.OpenAIChatModel"));
        assertEquals("dashscope", ClsTracingMiddleware.providerNameOf(
                "io.agentscope.extensions.model.dashscope.DashScopeChatModel"));
        assertEquals("anthropic", ClsTracingMiddleware.providerNameOf(
                "io.agentscope.extensions.model.anthropic.AnthropicChatModel"));
        assertEquals("gcp.gemini", ClsTracingMiddleware.providerNameOf(
                "io.agentscope.extensions.model.gemini.GeminiChatModel"));
        assertEquals("ollama", ClsTracingMiddleware.providerNameOf(
                "io.agentscope.extensions.model.ollama.OllamaChatModel"));
    }

    /**
     * Anything unrecognised yields no attribute at all. A missing value is honest; a wrong one
     * silently corrupts per-provider cost and latency aggregates — and an OpenAI-compatible
     * gateway is not OpenAI.
     */
    @Test
    void anUnrecognisedProviderYieldsNoAttributeRatherThanAGuess() {
        assertEquals("", ClsTracingMiddleware.providerNameOf("com.example.MyCustomChatModel"));
        assertEquals("", ClsTracingMiddleware.providerName(new StubModel()));
        assertEquals("", ClsTracingMiddleware.providerName(null));
        assertEquals("", ClsTracingMiddleware.providerNameOf(null));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /** Drives onModelCall with a downstream that reports usage, as a real model does. */
    private void runMiddlewareModelCall(ClsTracingMiddleware middleware) {
        middleware.onModelCall(null,
                        RuntimeContext.builder().sessionId("attr-s").userId("u").build(),
                        new ModelCallInput(messages(), tools(), options(), null),
                        in -> Flux.<AgentEvent>just(
                                new TextBlockDeltaEvent("r1", "b1", "sunny"),
                                new ModelCallEndEvent("r1", new ChatUsage(120, 30, 0.0))))
                .blockLast();
    }

    private SpanData chatSpan() {
        return exporter.spans().stream()
                .filter(s -> "chat".equals(str(s, "gen_ai.span.kind")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no chat span was produced"));
    }

    private static String str(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.stringKey(key));
    }

    private static Double dbl(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.doubleKey(key));
    }

    private static Long lng(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.longKey(key));
    }

    /** Emits one response carrying a provider-assigned completion id. */
    private static final class StubModel extends ChatModelBase {
        @Override
        public String getModelName() {
            return "stub-model";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> msgs, List<ToolSchema> tools, GenerateOptions options) {
            return Flux.just(ChatResponse.builder()
                    .id("resp-abc-123")
                    .content(List.<ContentBlock>of(TextBlock.builder().text("sunny").build()))
                    .usage(new ChatUsage(120, 30, 0.0))
                    .finishReason("stop")
                    .build());
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
