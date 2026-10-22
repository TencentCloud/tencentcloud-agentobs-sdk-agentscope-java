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
import io.agentscope.core.model.Model;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.core.tool.Toolkit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * 验证 demo：{@code agentscope-harness} 的「记忆抽取」模型调用能否被 CLS SDK 采集并关联到 turn。
 *
 * <p><b>这是手写复刻版。</b>用真包跑的版本见 {@link RealHarnessMemoryDemo}
 * （依赖 {@code io.agentscope:agentscope-harness:2.0.1}，已验证可用）。本 demo 保留的理由是它
 * 复刻的是 <b>2.0.4</b> 的调用形态，而那个版本依赖 core 2.0.4、与本工程的 core 2.0.1 不同版，
 * 没法和真包一起装：
 * <ul>
 *   <li><b>2.0.1</b>（真包 demo 用的）：
 *       {@code next.apply(input).concatWith(Mono.defer(..).subscribeOn(boundedElastic))}
 *       —— flush 是主链的一部分，Reactor Context 可见。</li>
 *   <li><b>2.0.4</b>（本 demo 复刻的）：
 *       {@code doOnComplete(() -> Mono.defer(..).subscribeOn(..).doFinally(..).subscribe(..))}
 *       —— 完全游离的订阅，所有上下文通道都断开，只能靠 SDK 的 Scheduler 钩子 + session 索引。</li>
 * </ul>
 * 两种形态都真实存在，取决于客户用哪个 harness 版本，所以两个 demo 都要留着。
 *
 * <p><b>为什么要多会话。</b>客户是飞书机器人（会话 ID 形如 {@code feishu.direct:ou_xxx}），
 * 天然多用户并发。SDK 在所有上下文通道都失效时，最后一级兜底是「在途只有一个 session 才归属」，
 * 因此单会话跑不出问题，必须多会话才能复现客户的真实情况。注意 SDK 的 session 索引有 2 分钟
 * 存活期，所以**即使串行**接待第 2 个用户之后，索引里也始终 >1 个 session。
 *
 * <p><b>复刻依据</b>（对 {@code agentscope-harness} 2.0.4 字节码 javap 核对）：
 * <ul>
 *   <li>{@code MemoryFlushMiddleware} 只重写 {@code onAgent}，**没有** {@code onModelCall}；
 *       flush 挂在 {@code Flux.doOnComplete(...)} 上，即 turn 事件流结束之后才触发。</li>
 *   <li>{@code runFlush} 的形态是
 *       {@code Mono.defer(..).subscribeOn(Schedulers.boundedElastic()).doFinally(..).subscribe(..)}
 *       —— 另一个线程上的全新、游离订阅。</li>
 *   <li>{@code MemoryFlushMiddleware} 与 {@code MemoryFlushManager} 中 {@code contextWrite}
 *       出现 **0 次**，turn 的 Reactor Context 传不进 flush。</li>
 *   <li>{@code flushMemories(RuntimeContext, List<Msg>)} 返回 {@code Mono<Void>}，全响应式、不 block。
 *       注意它**持有 RuntimeContext**（里面有 sessionId）—— 这是潜在的修复入口。</li>
 * </ul>
 *
 * <p>运行方式（配合本仓库的 {@code scripts/otlp_receiver.py} 查看真实上报的 span 树）：
 * <pre>{@code
 * python3 scripts/otlp_receiver.py 14318 &
 * CLS_ENDPOINT=http://127.0.0.1:14318 CLS_TOPIC_ID=t CLS_SECRET_ID=x CLS_SECRET_KEY=y \
 *   java -cp demo/target/demo-shaded.jar \
 *   com.tencentcloudapi.observability.testbench.HarnessMemoryFlushDemo
 * }</pre>
 * 判读标准：每个会话的 {@code origin=framework} span 是否带 {@code gen_ai.session.id}，
 * 以及是否与该会话的 {@code enter_application} 处于同一条 trace。
 */
public final class HarnessMemoryFlushDemo {

    /** 并发会话数；>1 才能复现客户的多用户场景。 */
    private static final int SESSIONS = 3;

    private HarnessMemoryFlushDemo() {
    }

    public static final class WeatherTool {
        @Tool(name = "get_weather", description = "Get the current weather for a given city.")
        public String getWeather(
                @ToolParam(name = "city", description = "City name") String city) {
            return "{\"city\":\"" + city + "\",\"temp\":\"22C\",\"condition\":\"sunny\"}";
        }
    }

    /**
     * 忠实复刻 harness 的 {@code MemoryFlushMiddleware}：turn 结束后，在 boundedElastic 线程上
     * 以游离订阅发起记忆抽取模型调用，且不向 Reactor Context 写任何东西。
     */
    static final class HarnessMemoryFlushMiddleware implements MiddlewareBase {

        /** {@code MemoryFlushManager.DEFAULT_FLUSH_PROMPT} 的开头原文。 */
        static final String DEFAULT_FLUSH_PROMPT =
                "You are a memory extraction assistant. Analyze the conversation below and "
                        + "extract important facts, decisions, and preferences worth remembering. "
                        + "Write to TODAY's daily memory ledger.";

        private final Model model;
        private final CountDownLatch flushed;

        HarnessMemoryFlushMiddleware(Model model, CountDownLatch flushed) {
            this.model = model;
            this.flushed = flushed;
        }

        @Override
        public Flux<AgentEvent> onAgent(
                Agent agent,
                RuntimeContext ctx,
                AgentInput input,
                Function<AgentInput, Flux<AgentEvent>> next) {
            // harness: next.apply(input).doOnComplete(() -> scheduleFlush(agent, ctx))
            return next.apply(input).doOnComplete(() -> scheduleFlush(ctx));
        }

        /** harness 的 runFlush：游离订阅 + boundedElastic，不传播任何上下文。 */
        private void scheduleFlush(RuntimeContext ctx) {
            Mono.defer(() -> flushMemories(ctx))
                    .subscribeOn(Schedulers.boundedElastic())
                    .doFinally(sig -> flushed.countDown())
                    .subscribe(v -> { }, err -> flushed.countDown());
        }

        /**
         * harness 的 {@code MemoryFlushManager.flushMemories}：拿着 RuntimeContext，
         * 直接 {@code model.stream(...)}，不经任何中间件链。
         */
        private Mono<Void> flushMemories(RuntimeContext ctx) {
            Msg prompt = Msg.builder()
                    .role(MsgRole.USER)
                    .content(TextBlock.builder().text(DEFAULT_FLUSH_PROMPT).build())
                    .build();
            return model.stream(List.of(prompt), List.of(), null).then();
        }
    }

    public static void main(String[] args) throws Exception {
        ClsObservability obs = ClsObservability.install();
        // ClsTracingMiddleware 本身无 per-turn 状态，官方支持单实例跨并发 turn 共享。
        MiddlewareBase tracing = (MiddlewareBase) obs.newTracingMiddleware();

        System.out.println();
        System.out.println("================================================================");
        System.out.println("  harness 记忆抽取采集验证 — " + SESSIONS + " 个并发会话");
        System.out.println("================================================================");

        CountDownLatch flushed = new CountDownLatch(SESSIONS);
        ExecutorService pool = Executors.newFixedThreadPool(SESSIONS);
        List<String> sessionIds = new ArrayList<>();
        long stamp = System.currentTimeMillis();

        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < SESSIONS; i++) {
            // 模仿飞书机器人的会话 ID 形态：每个用户一个 session。
            String sessionId = "feishu.direct:ou_user" + i + "_" + stamp;
            sessionIds.add(sessionId);
            futures.add(pool.submit(() -> runOneConversation(tracing, sessionId, flushed)));
        }
        for (java.util.concurrent.Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }

        boolean ok = flushed.await(30, TimeUnit.SECONDS);
        System.out.println("[demo] 记忆抽取调用全部完成: " + ok);

        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        obs.flush();
        obs.shutdown();

        System.out.println();
        System.out.println("================================================================");
        System.out.println("[demo] 本次会话 ID：");
        for (String s : sessionIds) {
            System.out.println("         " + s);
        }
        System.out.println();
        System.out.println("[demo] 判读标准 —— 对每个会话各看一条 origin=framework 的 chat span：");
        System.out.println("         (a) 是否带 gen_ai.session.id，且等于上面对应的会话 ID");
        System.out.println("         (b) 是否与该会话的 enter_application 同属一条 trace");
        System.out.println("       ARMS 侧这条 span 是挂在 invoke_agent 之下的，可作为对照基准。");
        System.out.println("================================================================");
    }

    /** 一次完整的天气询问：真实 ReActAgent + 真实工具 + 我们的中间件 + harness 式记忆抽取。 */
    private static void runOneConversation(
            MiddlewareBase tracing, String sessionId, CountDownLatch flushed) {
        Toolkit toolkit = new Toolkit();
        toolkit.registerTool(new WeatherTool());

        // 每个会话独立的模型实例：MockChatModel 内部有轮次计数器，不能跨会话共享。
        Model chatModel = new MockChatModel(
                "mock-chat", "get_weather", Map.of("city", "Beijing"),
                "The weather in Beijing is sunny, around 22C.");
        // 记忆抽取用独立的（通常更小的）模型，与 harness 的实际配置一致。
        Model memoryModel = new MockChatModel(
                "mock-memory", null, Map.of(),
                "- User asked about Beijing weather; answer was sunny 22C.");

        ReActAgent agent = ReActAgent.builder()
                .name("assistant")
                .sysPrompt("You are a helpful assistant. Use tools when needed.")
                .model(chatModel)
                .toolkit(toolkit)
                .middleware(tracing)
                .middleware(new HarnessMemoryFlushMiddleware(memoryModel, flushed))
                .maxIters(5)
                .build();

        RuntimeContext ctx = RuntimeContext.builder()
                .sessionId(sessionId)
                .userId(sessionId.substring(sessionId.indexOf(':') + 1))
                .build();

        Msg reply = agent.call("What's the weather in Beijing?", ctx).block();
        System.out.println("[demo] " + sessionId + " → "
                + (reply == null ? "<null>" : reply.getTextContent()));
        try {
            agent.close();
        } catch (Exception ignored) {
            // demo 场景忽略关闭异常
        }
    }
}
