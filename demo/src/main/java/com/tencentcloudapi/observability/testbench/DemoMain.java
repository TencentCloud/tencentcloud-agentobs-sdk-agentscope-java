package com.tencentcloudapi.observability.testbench;

import com.tencentcloudapi.observability.agentscope.ClsObservability;
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
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.core.tool.Toolkit;

import io.agentscope.extensions.model.openai.OpenAIChatModel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;

import java.util.List;
import java.util.Map;

import reactor.core.publisher.Flux;

/**
 * 本地联调入口：验证 0.0.5（已 shade OpenTelemetry API）配合 Aliyun ARMS Java agent
 * (-javaagent) 并存时，CLS 侧能否收到一棵干净的、父子关系完整的 trace，无需任何
 * {@code -Dotel.*} 启动参数做 workaround。
 *
 * <p>本 demo 严格使用客户视角的公开 API：{@link ClsObservability#install()} +
 * {@link ClsObservability#newTracingMiddleware()}，不直接访问 SDK 内部的 shaded OTel 类，
 * 以此验证最终用户的开箱即用体验。CLS 真实上报的 span 数据在 CLS 控制台通过
 * {@code gen_ai.session.id} 过滤查看。
 */
public final class DemoMain {

    private DemoMain() {
    }

    public static final class WeatherTool {
        @Tool(name = "get_weather", description = "Get the current weather for a given city.")
        public String getWeather(
                @ToolParam(name = "city", description = "City name, e.g. Beijing") String city) {
            return "{\"city\":\"" + city + "\",\"temp\":\"22C\",\"condition\":\"sunny\"}";
        }
    }

    // ══════════════ 模型选择：真实 LLM 或离线 mock ══════════════

    /**
     * 解析配置：先查进程环境变量，再查 {@code ./.env}（与 SDK 的 {@code ClsConfig} 同源，
     * 这样 CLS 凭证和 LLM 凭证可以放在同一个文件里）。
     *
     * <p>注意：这里只做 demo 的配置读取，SDK 本身不依赖这些变量。
     * 包内可见，{@link RealHarnessMemoryDemo} 共用同一份解析，避免两处读 .env 的逻辑漂移。
     */
    static String conf(String key) {
        String v = System.getenv(key);
        if (v != null && !v.isEmpty()) {
            return v;
        }
        return DOT_ENV.get(key);
    }

    private static final Map<String, String> DOT_ENV = loadDotEnv();

    private static Map<String, String> loadDotEnv() {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        java.nio.file.Path p = java.nio.file.Paths.get(
                System.getProperty("user.dir", "."), ".env");
        if (!java.nio.file.Files.isRegularFile(p)) {
            return out;
        }
        try {
            for (String line : java.nio.file.Files.readAllLines(
                    p, java.nio.charset.StandardCharsets.UTF_8)) {
                String s = line.trim();
                int eq = s.indexOf('=');
                if (s.isEmpty() || s.startsWith("#") || eq <= 0) {
                    continue;
                }
                String k = s.substring(0, eq).trim();
                String v = s.substring(eq + 1).trim();
                // 去掉 # 之后的行内注释（值本身没加引号时）
                if (!v.startsWith("\"") && !v.startsWith("'")) {
                    int hash = v.indexOf('#');
                    if (hash >= 0) {
                        v = v.substring(0, hash).trim();
                    }
                } else if (v.length() >= 2 && v.charAt(0) == v.charAt(v.length() - 1)) {
                    v = v.substring(1, v.length() - 1);
                }
                out.put(k, v);
            }
        } catch (java.io.IOException ignored) {
            // demo 场景下读不到就按"未配置"处理
        }
        return out;
    }

    /**
     * 构造对话模型。配了 {@code LLM_API_KEY} 就用真实的 OpenAI 兼容模型，否则回退到离线
     * {@link MockChatModel} —— 后者让 demo 在没有任何凭证时也能完整跑通 trace 结构。
     *
     * <p>{@code OpenAIChatModel} 继承 {@code ChatModelBase}，其 {@code stream()} 是
     * {@code final} 且经由 {@code TracerRegistry}，因此 SDK 的采集逻辑对它完全生效 ——
     * 这也正是用它来验证真实链路的意义。
     */
    private static Model buildChatModel() {
        String apiKey = conf("LLM_API_KEY");
        if (apiKey == null || apiKey.isEmpty()) {
            System.out.println("[demo] LLM_API_KEY 未配置 → 使用离线 MockChatModel");
            return new MockChatModel(
                    "mock-gpt-4o",
                    "get_weather",
                    Map.of("city", "Beijing"),
                    "The weather in Beijing is sunny, around 22°C.");
        }
        String baseUrl = require("LLM_BASE_URL");
        String modelName = require("LLM_MODEL");
        System.out.println("[demo] 使用真实模型: " + modelName + " @ " + baseUrl);
        return withEndpoint(OpenAIChatModel.builder(), baseUrl)
                .apiKey(apiKey)
                .modelName(modelName)
                .stream(true)
                .build();
    }

    /**
     * 构造记忆/摘要模型。真实记忆组件通常用独立（往往更小）的模型实例，这里照此还原；
     * 同时也避免与主对话模型共享 mock 的轮次计数器而打乱 ReAct 流程。
     */
    private static Model buildMemoryModel() {
        String apiKey = conf("LLM_API_KEY");
        if (apiKey == null || apiKey.isEmpty()) {
            return new MockChatModel(
                    "mock-memory-model",
                    null,
                    Map.of(),
                    "User asked about Beijing weather; answer was sunny 22C.");
        }
        // 记忆模型默认复用主模型，可用 LLM_MEMORY_MODEL 单独指定一个更便宜的。
        String memModel = conf("LLM_MEMORY_MODEL");
        if (memModel == null || memModel.isEmpty()) {
            memModel = require("LLM_MODEL");
        }
        return withEndpoint(OpenAIChatModel.builder(), require("LLM_BASE_URL"))
                .apiKey(apiKey)
                .modelName(memModel)
                .stream(true)
                .build();
    }

    private static String require(String key) {
        String v = conf(key);
        if (v == null || v.isEmpty()) {
            throw new IllegalStateException(
                    "配置了 LLM_API_KEY 就必须同时提供 " + key + "。\n"
                            + "  请在 .env 中补充，例如：\n"
                            + "    LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1\n"
                            + "    LLM_MODEL=qwen-plus");
        }
        return v;
    }

    /**
     * {@code OpenAIChatModel} 默认会在 baseUrl 后追加 {@code /v1/chat/completions}。但各家
     * 网关给出的"接口地址"写法不一：有的只到域名，有的到 {@code /v1}，有的直接给完整的
     * {@code /v1/chat/completions}（腾讯 TokenHub 即是后者）。若原样传入，拼接后会变成
     * {@code .../v1/chat/completions/v1/chat/completions} 而 404。
     *
     * <p>因此这里把 URL 拆成 origin + path 两段，用 {@code endpointPath()} 显式指定后者，
     * 让三种写法都能直接用，无需使用者自己去掉尾巴。
     */
    private static OpenAIChatModel.Builder withEndpoint(
            OpenAIChatModel.Builder builder, String rawBaseUrl) {
        String url = rawBaseUrl.endsWith("/")
                ? rawBaseUrl.substring(0, rawBaseUrl.length() - 1)
                : rawBaseUrl;
        try {
            java.net.URI uri = java.net.URI.create(url);
            String path = uri.getPath() == null ? "" : uri.getPath();
            String origin = uri.getScheme() + "://" + uri.getAuthority();
            if (path.isEmpty() || path.equals("/")) {
                // 只给了域名 → 交回默认拼接逻辑。
                return builder.baseUrl(origin);
            }
            if (path.endsWith("/chat/completions")) {
                // 已是完整的调用地址 → 原样使用，不要再追加。
                return builder.baseUrl(origin).endpointPath(path);
            }
            // 给到了 /v1 之类的版本前缀 → 自己补上 /chat/completions。
            return builder.baseUrl(origin).endpointPath(path + "/chat/completions");
        } catch (IllegalArgumentException e) {
            System.out.println("[demo] LLM_BASE_URL 无法解析，按原样使用: " + url);
            return builder.baseUrl(url);
        }
    }

    /**
     * 复刻「同步阻塞」形态的框架内部模型调用：在信号处理期间直接
     * {@code model.stream(...).block()}，完全绕过中间件链。
     *
     * <p>这类组件（如 1.x 扩展 {@code AutoContextMemory}）实现 {@code Memory} 接口，其
     * {@code addMessage} / {@code compressIfNeeded} 由 Agent 在**处理消息事件时**调用，
     * 也就是在 Reactor 的信号处理（{@code onNext}）期间 —— 因此这里把 block 调用放在
     * {@code doOnNext} 里，与真实时机一致。
     *
     * <p>这个时机很关键：{@code TracerRegistry} 的 hook 只在
     * {@code onNext}/{@code onError}/{@code onComplete} 上调用
     * {@code runWithContext}（**不包括 {@code onSubscribe}**），SDK 正是借此把 turn 上下文
     * 桥接到 ThreadLocal，使得不携带 Reactor Context 的 {@code block()} 调用也能关联到
     * 当前 turn。
     *
     * <p>{@code agentscope-core} 自身没有绕过中间件的路径（含 {@code summarizing()} 摘要
     * 在内都走 {@code onModelCall}，见 {@code ClsSummarisationCaptureTest}）。
     *
     * <p>另一种更常见的形态是把调用**派发给 Scheduler**（{@code agentscope-harness} 的
     * {@code MemoryFlushMiddleware} 即如此），那条路径由
     * {@link HarnessMemoryFlushDemo} 单独演示 —— 它依赖的是 Scheduler 钩子，而非本例的
     * ThreadLocal 桥接。
     */
    static final class BypassingModelCall implements MiddlewareBase {

        /** {@code Prompts.CURRENT_ROUND_MESSAGE_COMPRESS_PROMPT} 的开头原文。 */
        private static final String AUTOCONTEXT_PROMPT =
                "You are an expert context consolidator for autonomous agents. Your task is to "
                        + "integrate new tool execution results into the current conversation "
                        + "context.";

        private final Model model;
        private final java.util.concurrent.atomic.AtomicBoolean fired =
                new java.util.concurrent.atomic.AtomicBoolean();

        BypassingModelCall(Model model) {
            this.model = model;
        }

        @Override
        public Flux<AgentEvent> onAgent(Agent agent, RuntimeContext ctx, AgentInput input,
                                        java.util.function.Function<AgentInput, Flux<AgentEvent>> next) {
            return next.apply(input)
                    .doOnNext(ev -> {
                        // 只压缩一次，避免每个事件都调一次模型。
                        if (!fired.compareAndSet(false, true)) {
                            return;
                        }
                        Msg memoryPrompt = Msg.builder()
                                .role(MsgRole.USER)
                                .content(TextBlock.builder().text(AUTOCONTEXT_PROMPT).build())
                                .build();
                        // 同步 block —— 与 callModelForSummary 一致，不携带 Reactor Context。
                        // 但因为处于 onNext 信号处理中，SDK 已把 turn 上下文放进 ThreadLocal，
                        // 所以这条 span 依然会挂到当前 turn 的 trace 上。
                        model.stream(List.of(memoryPrompt), List.of(), null).blockLast();
                    });
        }
    }

    public static void main(String[] args) throws Exception {
        banner("JVM boot, BEFORE ClsObservability.install()");
        dumpGlobalOtel();

        // 1) 用客户视角的标准 API 安装 CLS 观测（isolateContext 默认 true）
        //    SDK 内部已经 shade 了 OTel API 到 com.tencentcloudapi.observability.shaded.*，
        //    阿里 ARMS javaagent 的字节码增强匹配 io.opentelemetry.* 时找不到我们的 Tracer，
        //    因此我们产生的 span 不会被劫持，trace 树完整。
        ClsObservability obs = ClsObservability.install();
        MiddlewareBase tracing = (MiddlewareBase) obs.newTracingMiddleware();

        banner("AFTER ClsObservability.install()");
        dumpGlobalOtel();

        // 2) 组装真实 ReActAgent + 真实工具 + 我们的 middleware
        Toolkit toolkit = new Toolkit();
        toolkit.registerTool(new WeatherTool());

        Model model = buildChatModel();
        Model memoryModel = buildMemoryModel();

        ReActAgent agent = ReActAgent.builder()
                .name("assistant")
                .sysPrompt("You are a helpful assistant. Use tools when needed.")
                // 没配 description 时 Agent.getDescription() 会合成 "Agent(id) name" 占位符，
                // SDK 会把它过滤掉（它只是重复 agent.id / agent.name）。配一个真实的，
                // 才能在 trace 里看到 gen_ai.agent.description。
                .description("Answers weather questions by calling tools")
                .model(model)
                // Agent 级采样参数。这些会随 ModelCallInput.options() 一起到达中间件，
                // 因此会出现在 chat span 上（gen_ai.request.temperature / top_p / max_tokens …），
                // 让一次调用可被复现。注意：配在 Model builder 上的同类参数由 provider 在
                // doStream 内部合并，SDK 观测不到 —— 要让它们可见就配在这里。
                .generateOptions(GenerateOptions.builder()
                        .temperature(0.7)
                        .topP(0.95)
                        .maxTokens(1024)
                        .build())
                .toolkit(toolkit)
                .middleware(tracing)
                // 注册在 tracing 之后 ⇒ 位于其内层，模型调用发生在 turn 执行期间
                .middleware(new BypassingModelCall(memoryModel))
                .maxIters(5)
                .build();

        String sessionId = "shadeB-session-" + System.currentTimeMillis();
        String userId = "shadeB-user";
        RuntimeContext ctx = RuntimeContext.builder()
                .sessionId(sessionId)
                .userId(userId)
                .build();

        banner("Running real ReActAgent — session=" + sessionId);
        System.out.println("[demo] 本次 turn 内部会有一次绕过中间件链的模型调用（模拟场景）");
        Msg reply = agent.call("What's the weather in Beijing?", ctx).block();
        System.out.println("[demo] agent replied: "
                + (reply == null ? "<null>" : reply.getTextContent()));

        // 另一种形态：记忆 / 摘要在 turn 结束之后才触发（框架的记忆组件都是如此 —— 它要
        // 总结刚结束的会话）。此时 Reactor Context、ThreadLocal turn 状态、OTel Context
        // 全都已失效，但本次调用就发生在刚跑完该 turn 的主线程上，SDK 借此按 session 找回
        // 上一个 turn，因此这条 span 依然落在同一条 trace 上，并带 origin=framework。
        banner("Framework-internal model call AFTER the turn finished");
        Msg memoryPrompt = Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder()
                        .text("[framework default prompt] Compact long-term memory.")
                        .build())
                .build();
        ChatResponse memoryResp =
                memoryModel.stream(List.of(memoryPrompt), List.of(), null).blockLast();
        System.out.println("[demo] framework-internal model call returned: "
                + (memoryResp == null ? "<null>" : memoryResp.getFinishReason()));

        banner("Flushing to CLS");
        obs.flush();
        obs.shutdown();
        try {
            agent.close();
        } catch (Exception ignored) {
        }

        banner("Final GlobalOpenTelemetry state");
        dumpGlobalOtel();

        System.out.println();
        System.out.println("================================================================");
        System.out.println("[demo] done. sessionId=" + sessionId);
        System.out.println("       → go to CLS and filter by gen_ai.session.id = " + sessionId);
        System.out.println("       Expected: ONE trace with the full hierarchy intact,");
        System.out.println("                 regardless of whether -javaagent is attached,");
        System.out.println("                 PLUS two extra chat spans carrying");
        System.out.println("                 gen_ai.model.call.origin=framework — one from");
        System.out.println("                 inside the turn, one fired after it finished and");
        System.out.println("                 recovered onto the same trace via the session index.");
        System.out.println("       Tip: run with CLS_TRACE_DEBUG=1 to print span linkage here.");
        System.out.println("================================================================");
    }

    private static void banner(String title) {
        System.out.println();
        System.out.println("================================================================");
        System.out.println("  " + title);
        System.out.println("================================================================");
    }

    /**
     * 打印客户进程视角的全局 OpenTelemetry 持有者。
     *
     * <p>关键点：这里访问的是客户进程 classpath 中的原版 {@code io.opentelemetry.api.GlobalOpenTelemetry}
     * （由阿里 ARMS javaagent 启动时注入），和 SDK 内部已 shade 的
     * {@code com.tencentcloudapi.observability.shaded.io.opentelemetry.api.GlobalOpenTelemetry}
     * 完全是不同的类 —— 这正是方案 B 的核心价值：两套 OTel 栈物理隔离，各走各的。
     */
    private static void dumpGlobalOtel() {
        try {
            OpenTelemetry g = GlobalOpenTelemetry.get();
            System.out.println("[demo] (host) GlobalOpenTelemetry = " + g.getClass().getName()
                    + "  (identityHash=" + System.identityHashCode(g) + ")");
        } catch (Throwable t) {
            System.out.println("[demo] GlobalOpenTelemetry.get() threw: " + t);
        }
    }
}
