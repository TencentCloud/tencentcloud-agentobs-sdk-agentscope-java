package com.tencentcloudapi.observability.testbench;

import com.tencentcloudapi.observability.agentscope.ClsObservability;
import com.tencentcloudapi.observability.agentscope.example.MockChatModel;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.middleware.MiddlewareBase;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.core.tool.Toolkit;

import io.agentscope.extensions.model.openai.OpenAIChatModel;

import io.agentscope.harness.agent.memory.MemoryConfig;
import io.agentscope.harness.agent.memory.MemoryFlushManager;
import io.agentscope.harness.agent.middleware.MemoryFlushMiddleware;
import io.agentscope.harness.agent.workspace.WorkspaceManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * 用**真实的** {@code agentscope-harness} 验证记忆抽取调用能否被 CLS SDK 采集并关联到 turn。
 *
 * <p><b>与 {@link HarnessMemoryFlushDemo} 的区别。</b>那个 demo 是手写复刻的调用形态；本 demo
 * 直接用 Maven Central 上的真包 {@code io.agentscope:agentscope-harness:2.0.1}
 * （{@code MemoryFlushMiddleware} + {@code MemoryFlushManager}），所以验证的是上游真实字节码，
 * 一旦上游改变调用形态，这里就会跟着变。
 *
 * <p><b>两个版本是两种形态</b>（javap 核对）：
 * <ul>
 *   <li><b>2.0.1</b>（本 demo，对齐 core 2.0.1）：
 *       {@code next.apply(input).concatWith(Mono.defer(..).subscribeOn(boundedElastic))}
 *       —— flush 是主链的一部分，只是换了线程。</li>
 *   <li><b>2.0.4</b>（对齐 core 2.0.4，由 {@link HarnessMemoryFlushDemo} 复刻）：
 *       {@code doOnComplete(() -> Mono.defer(..).subscribeOn(..).doFinally(..).subscribe(..))}
 *       —— 完全游离的订阅，更难关联，依赖 SDK 的 Scheduler 钩子。</li>
 * </ul>
 * 选 2.0.1 是因为本工程的 core 是 2.0.1，harness 2.0.4 依赖 core 2.0.4。
 *
 * <p><b>中间件注册顺序是关键变量。</b>{@code .middleware(tracing)} 在前 ⇒ tracing 位于外层，
 * harness 的 {@code concatWith} 在其内层完成，因此记忆调用发生在 turn span 关闭**之前**，
 * Reactor Context 仍然可见。反序则记忆调用落在 turn 之后，要靠 session 索引兜底。本 demo
 * 跑的是前者（也是推荐装法）。
 *
 * <p>运行（默认发往 .env 配置的真实 CLS；要本地看树就把 CLS_ENDPOINT 指向 otlp_receiver.py）：
 * <pre>{@code
 * java -cp demo/target/demo-shaded.jar \
 *   com.tencentcloudapi.observability.testbench.RealHarnessMemoryDemo
 * }</pre>
 */
public final class RealHarnessMemoryDemo {

    private RealHarnessMemoryDemo() {
    }

    public static final class WeatherTool {
        @Tool(name = "get_weather", description = "Get the current weather for a given city.")
        public String getWeather(
                @ToolParam(name = "city", description = "City name") String city) {
            return "{\"city\":\"" + city + "\",\"temp\":\"22C\",\"condition\":\"sunny\"}";
        }
    }

    /**
     * 真实模型优先；没配 LLM_API_KEY 时回退离线 mock，保证 demo 总能跑通结构。
     *
     * <p>配置经 {@link DemoMain#conf} 读取 —— 它会先查环境变量再查 {@code ./.env}。直接用
     * {@code System.getenv} 是不够的：凭证通常只在 .env 里，第一次跑就因此静默回退成了 mock。
     */
    private static Model buildModel(String envName, String mockName, String toolName,
                                    Map<String, Object> toolArgs, String mockReply) {
        String apiKey = DemoMain.conf("LLM_API_KEY");
        if (apiKey == null || apiKey.isEmpty()) {
            System.out.println("[demo] LLM_API_KEY 未配置 → " + mockName + " 使用离线 MockChatModel");
            return new MockChatModel(mockName, toolName, toolArgs, mockReply);
        }
        String model = DemoMain.conf(envName);
        if (model == null || model.isEmpty()) {
            model = DemoMain.conf("LLM_MODEL");
        }
        System.out.println("[demo] 使用真实模型: " + model);
        return OpenAIChatModel.builder()
                .baseUrl(DemoMain.conf("LLM_BASE_URL"))
                .apiKey(apiKey)
                .modelName(model)
                .stream(true)
                .build();
    }

    public static void main(String[] args) throws Exception {
        ClsObservability obs = ClsObservability.install();
        MiddlewareBase tracing = (MiddlewareBase) obs.newTracingMiddleware();

        Path workspace = Files.createTempDirectory("harness-demo-");
        System.out.println("[demo] harness workspace: " + workspace);

        Model chatModel = buildModel("LLM_MODEL", "mock-chat",
                "get_weather", Map.of("city", "Beijing"),
                "The weather in Beijing is sunny, around 22C.");
        Model memoryModel = buildModel("LLM_MEMORY_MODEL", "mock-memory",
                null, Map.of(),
                "- User asked about Beijing weather; answer was sunny 22C.");

        Toolkit toolkit = new Toolkit();
        toolkit.registerTool(new WeatherTool());

        try (WorkspaceManager workspaceManager = new WorkspaceManager(workspace)) {
            // 真包的 MemoryFlushMiddleware。FlushTrigger.always() 强制每个 turn 都抽取，
            // 否则默认的 THROTTLED 会因为 minGap 未到而跳过，demo 就看不到这条 span。
            MemoryFlushMiddleware memoryFlush = new MemoryFlushMiddleware(
                    workspaceManager,
                    memoryModel,
                    MemoryFlushManager.DEFAULT_FLUSH_PROMPT,
                    MemoryConfig.FlushTrigger.always());

            ReActAgent agent = ReActAgent.builder()
                    .name("assistant")
                    .sysPrompt("You are a helpful assistant. Use tools when needed.")
                    .description("Answers weather questions by calling tools")
                    .model(chatModel)
                    .generateOptions(GenerateOptions.builder()
                            .temperature(0.7)
                            .maxTokens(1024)
                            .build())
                    .toolkit(toolkit)
                    .middleware(tracing)
                    // 注册在 tracing 之后 ⇒ 内层，记忆抽取在 turn span 关闭前完成
                    .middleware(memoryFlush)
                    .maxIters(5)
                    .build();

            String sessionId = "harness-session-" + System.currentTimeMillis();
            RuntimeContext ctx = RuntimeContext.builder()
                    .sessionId(sessionId)
                    .userId("harness-user")
                    .build();

            System.out.println("[demo] 真实 harness 记忆抽取 — session=" + sessionId);
            Msg reply = agent.call("What's the weather in Beijing?", ctx).block();
            System.out.println("[demo] agent 回复: "
                    + (reply == null ? "<null>" : reply.getTextContent()));
            agent.close();

            obs.flush();
            obs.shutdown();

            System.out.println();
            System.out.println("================================================================");
            System.out.println("[demo] done. sessionId=" + sessionId);
            System.out.println("       判读：应出现一条 origin=framework 的 chat span，");
            System.out.println("             其 input.messages 为 harness 的真实 flush prompt");
            System.out.println("             (\"You are a memory extraction assistant…\")，");
            System.out.println("             且与 enter_application 同属一条 trace。");
            System.out.println("================================================================");
        }
    }
}
