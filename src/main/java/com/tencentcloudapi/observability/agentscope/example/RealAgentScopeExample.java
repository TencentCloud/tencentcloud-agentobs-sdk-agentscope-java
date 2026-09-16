package com.tencentcloudapi.observability.agentscope.example;

import com.tencentcloudapi.observability.agentscope.ClsObservability;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.middleware.MiddlewareBase;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.core.tool.Toolkit;

import java.util.Map;

/**
 * End-to-end demo running a <b>real</b> AgentScope {@link ReActAgent} — genuine agent, toolkit,
 * middleware chain and event stream — with only the LLM faked ({@link MockChatModel}), and every
 * span exported to Tencent Cloud CLS over OTLP/HTTP by the v2 {@code ClsTracingMiddleware}.
 *
 * <p>What actually happens on one {@code agent.call(...)}:
 * <ol>
 *   <li>{@code invoke_agent} span opens (turn).</li>
 *   <li>Round 1 {@code chat} span: {@link MockChatModel} returns a {@code ToolUseBlock} → the real
 *       ReActAgent dispatches the {@link WeatherTool#getWeather} tool.</li>
 *   <li>{@code execute_tool} span: the tool runs, its arguments & result captured by the middleware.</li>
 *   <li>Round 2 {@code chat} span: the model returns the final text answer, ending the ReAct loop.</li>
 * </ol>
 * All spans carry {@code sessionID}/{@code userID} from {@link RuntimeContext} plus GenAI attributes,
 * then are flushed to CLS.
 *
 * <p>Required CLS config (env vars or {@code ./.env}): {@code CLS_ENDPOINT}, {@code CLS_TOPIC_ID},
 * {@code CLS_SECRET_ID}, {@code CLS_SECRET_KEY}.
 *
 * <pre>{@code
 * mvn -q exec:java \
 *   -Dexec.mainClass="com.tencentcloudapi.observability.agentscope.example.RealAgentScopeExample"
 * }</pre>
 */
public final class RealAgentScopeExample {

    private RealAgentScopeExample() {
    }

    /** A trivial, deterministic weather tool the real ReActAgent will invoke. */
    public static final class WeatherTool {
        @Tool(name = "get_weather", description = "Get the current weather for a given city.")
        public String getWeather(
                @ToolParam(name = "city", description = "City name, e.g. Beijing") String city) {
            // Deterministic canned result — no network needed.
            return "{\"city\":\"" + city + "\",\"temp\":\"22C\",\"condition\":\"sunny\"}";
        }
    }

    public static void main(String[] args) {
        // 1) Install the real CLS OTLP pipeline (reads CLS_* from env / .env).
        ClsObservability obs = ClsObservability.install();
        MiddlewareBase tracing = (MiddlewareBase) obs.newTracingMiddleware();

        // 2) Build a real toolkit with a real @Tool.
        Toolkit toolkit = new Toolkit();
        toolkit.registerTool(new WeatherTool());

        // 3) Fake only the LLM: round 1 → call get_weather(Beijing); round 2 → final answer.
        MockChatModel model = new MockChatModel(
                "mock-gpt-4o",
                "get_weather",
                Map.of("city", "Beijing"),
                "The weather in Beijing is sunny, around 22°C.");

        // 4) Assemble the REAL ReActAgent with our v2 tracing middleware.
        ReActAgent agent = ReActAgent.builder()
                .name("assistant")
                .sysPrompt("You are a helpful assistant. Use tools when needed.")
                .model(model)
                .toolkit(toolkit)
                .middleware(tracing)
                .maxIters(5)
                .build();

        // 5) RuntimeContext carries sessionID / userID that the middleware promotes to columns.
        String sessionId = "session-" + System.currentTimeMillis();
        String userId = "user-demo";
        RuntimeContext ctx = RuntimeContext.builder()
                .sessionId(sessionId)
                .userId(userId)
                .build();

        System.out.println("Running REAL ReActAgent (session=" + sessionId + ") ...");

        // 6) Drive one real turn. Blocking here is fine for a demo.
        Msg reply = agent.call("What's the weather in Beijing?", ctx).block();

        System.out.println("Agent replied: "
                + (reply == null ? "<null>" : reply.getTextContent()));

        // 7) Flush the span batch to CLS before exiting.
        System.out.println("Flushing spans to CLS ...");
        obs.flush();
        obs.shutdown();
        try {
            agent.close();
        } catch (Exception ignored) {
            // best-effort
        }
        System.out.println("Done. If no OTLP error was logged above, the real-agent spans reached CLS. "
                + "Query them in the CLS trace topic (session=" + sessionId + ").");
    }
}
