package com.tencentcloudapi.observability.testbench;

import com.tencentcloudapi.observability.agentscope.ClsObservability;
import com.tencentcloudapi.observability.agentscope.example.MockChatModel;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.middleware.MiddlewareBase;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.core.tool.Toolkit;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;

import java.util.Map;

/**
 * 本地联调入口：验证 0.0.4（已 shade OpenTelemetry API）配合 Aliyun ARMS Java agent
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

        MockChatModel model = new MockChatModel(
                "mock-gpt-4o",
                "get_weather",
                Map.of("city", "Beijing"),
                "The weather in Beijing is sunny, around 22°C.");

        ReActAgent agent = ReActAgent.builder()
                .name("assistant")
                .sysPrompt("You are a helpful assistant. Use tools when needed.")
                .model(model)
                .toolkit(toolkit)
                .middleware(tracing)
                .maxIters(5)
                .build();

        String sessionId = "shadeB-session-" + System.currentTimeMillis();
        String userId = "shadeB-user";
        RuntimeContext ctx = RuntimeContext.builder()
                .sessionId(sessionId)
                .userId(userId)
                .build();

        banner("Running real ReActAgent — session=" + sessionId);
        Msg reply = agent.call("What's the weather in Beijing?", ctx).block();
        System.out.println("[demo] agent replied: "
                + (reply == null ? "<null>" : reply.getTextContent()));

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
        System.out.println("       Expected: ONE trace with 7 spans, hierarchy intact,");
        System.out.println("                 regardless of whether -javaagent is attached.");
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
