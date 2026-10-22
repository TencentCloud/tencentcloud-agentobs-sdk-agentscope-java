package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.middleware.MiddlewareBase;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.ToolSchema;
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
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down how <b>agentscope-core 2.0.1</b> actually issues its built-in summarisation call, so
 * the SDK's coverage claims stay honest across upgrades.
 *
 * <p>Verified behaviour in 2.0.1: when the ReAct loop exhausts {@code maxIters},
 * {@code ReActAgent.reasoning()} early-returns into {@code summarizing()}, which reaches the model
 * through {@code MiddlewareChain} → {@code onModelCall}. The framework's default summarisation
 * prompt is therefore captured by {@link ClsTracingMiddleware} alone — {@link ClsFrameworkTracer}
 * is not involved, which is exactly why the resulting span carries no
 * {@code gen_ai.model.call.origin}.
 *
 * <p>If a future AgentScope moves that call off the middleware chain, this test fails and tells us
 * the framework-level tracer has become load-bearing for summarisation.
 */
@SuppressWarnings("removal")
class ClsSummarisationCaptureTest {

    /** The framework's built-in prompt when the ReAct loop runs out of iterations. */
    private static final String FRAMEWORK_SUMMARY_PROMPT =
            "You have failed to generate response within the maximum iterations";

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

    public static final class EchoTool {
        @Tool(name = "ping", description = "ping")
        public String ping(@ToolParam(name = "x", description = "x") String x) {
            return "pong";
        }
    }

    /** Always requests a tool, so the loop never settles and {@code maxIters} is exhausted. */
    private static final class AlwaysToolModel extends ChatModelBase {
        private final AtomicInteger n = new AtomicInteger();

        @Override
        public String getModelName() {
            return "always-tool";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> msgs, List<ToolSchema> tools, GenerateOptions options) {
            int i = n.incrementAndGet();
            return Flux.just(ChatResponse.builder()
                    .id("r" + i)
                    .content(List.<ContentBlock>of(
                            new ToolUseBlock("call_" + i, "ping", Map.of("x", "1")),
                            TextBlock.builder().text("thinking " + i).build()))
                    .usage(new ChatUsage(10, 5, 0.1))
                    .finishReason("tool_calls")
                    .build());
        }
    }

    @Test
    void frameworkSummarisationPromptIsCaptured() throws Exception {
        Toolkit toolkit = new Toolkit();
        toolkit.registerTool(new EchoTool());

        ReActAgent agent = ReActAgent.builder()
                .name("assistant")
                .sysPrompt("sys")
                .model(new AlwaysToolModel())
                .toolkit(toolkit)
                .middleware((MiddlewareBase) new ClsTracingMiddleware(tracer))
                .maxIters(1)
                .build();

        agent.call("hi", RuntimeContext.builder().sessionId("sum-s").userId("u").build()).block();
        agent.close();

        List<SpanData> spans = exporter.awaitSpans(6);

        SpanData summary = spans.stream()
                .filter(s -> "chat".equals(str(s, "gen_ai.span.kind")))
                .filter(s -> {
                    String in = str(s, "gen_ai.input.messages");
                    return in != null && in.contains(FRAMEWORK_SUMMARY_PROMPT);
                })
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "the framework's default summarisation prompt was not captured by any "
                                + "chat span; spans=" + names(spans)));

        // Captured by the middleware chain, NOT by the framework-level tracer. The absence of an
        // origin attribute is the assertion: it proves onModelCall saw the call.
        assertEquals(null, str(summary, "gen_ai.model.call.origin"),
                "in 2.0.1 summarisation goes through onModelCall, so no framework-origin tag");

        // It is attributed to the turn and sits inside the ReAct round that exhausted maxIters.
        assertEquals("sum-s", str(summary, "gen_ai.session.id"));
        assertNotNull(str(summary, "gen_ai.step.id"));

        SpanData parent = spans.stream()
                .filter(s -> s.getSpanId().equals(summary.getParentSpanId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("summary chat span has no parent in trace"));
        assertEquals("step", str(parent, "gen_ai.span.kind"),
                "a chat span must hang off a step, found: " + parent.getName());
        assertTrue(parent.getName().startsWith("react round_"),
                "2.0.1 attributes summarisation to the exhausted ReAct round, found: "
                        + parent.getName());

        // The model's reply must be recorded too, not just the prompt.
        assertTrue(str(summary, "gen_ai.output.messages").contains("thinking"),
                "summary response not captured");
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

        List<SpanData> awaitSpans(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while (collected.size() < expected && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
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
