# tencentcloud-agentobs-sdk-agentscope-java

> [English](./README.md) | **中文**

面向 **AgentScope Java** 的腾讯云 CLS 可观测性 SDK。

本 SDK 提供 `ClsTracingMiddleware` —— 一个基于 OpenTelemetry 的自研 tracing 中间件，
能够捕获丰富的 Agent 执行细节，并通过标准 **OTLP/HTTP** 协议将 span 导出到
[腾讯云 CLS](https://cloud.tencent.com/product/cls) —— 传输层厂商中立，数据最终落入 CLS。

### 核心特性

- **完整 span 层级** —— 每个用户 turn 产出 `entry → agent → step → chat / tool` 层级树
- **丰富的属性采集** —— session/user 身份、工具入参与结果、推理思考过程、流式输出文本、token 用量（含缓存）
- **人工介入（HITL）感知** —— 跟踪确认 / 外部执行 / 拒绝 / 中断等状态
- **并发安全 & 无状态** —— 状态随 Reactor Context 逐次调用隔离，单实例可安全用于并发场景
- **CLS GenAI Trace 规范**兼容 —— 所有属性使用 `gen_ai.*` 点分命名
- **Per-turn trace 模型** —— 每次用户交互 = 一个独立 Trace，session 通过 `gen_ai.session.id` 跨 turn 关联

## Span 层级（每个 turn）

每个 turn 产生一条 trace，其中所有 span 共享同一个 `traceID`：

```
entry (SERVER, enter_application)      —— turn 根节点
└─ agent (INTERNAL, invoke_agent)      —— 一次 ReActAgent 调用
   ├─ step (INTERNAL, react, round 1)  —— 一个 ReAct 轮次
   │  ├─ chat (CLIENT)                 —— 模型调用
   │  └─ tool (CLIENT, execute_tool)   —— 同一轮次 ⇒ 与 chat 共享 step 父节点
   └─ step (INTERNAL, react, round 2)
      └─ chat (CLIENT)                  —— 终答，无工具调用
```

ID 命名链：`gen_ai.session.id → gen_ai.turn.id（{sessionId}:t{N}）→
gen_ai.step.id（{turnId}:s{N}）`。

### 中间件 hook 映射

AgentScope 提供五个中间件 hook。`ClsTracingMiddleware` 是**无状态**的 —— 不持有任何
per-turn 字段；所有 turn/round 状态都存在随 Reactor Context 传递的 per-invocation 对象里，
因此单个实例在并发 turn 以及 HITL 挂起/恢复场景下都是安全的。

| Hook | 类型 | 是否使用 | 产出 span |
|---|---|:---:|---|
| `onAgent` | onion | ✅ | `entry`（SERVER）+ `agent`（INTERNAL）—— turn 根节点；同时监听事件流中的 HITL 事件 |
| `onReasoning` | onion | ✅ | `step`（INTERNAL, `react`）—— 一个 ReAct 轮次（输入组装 → 模型调用 → 解码） |
| `onModelCall` | onion | ✅ | `chat`（CLIENT）—— 原始模型调用，挂在当前 step 下 |
| `onActing` | onion | ✅ | `tool`（CLIENT, `execute_tool`）—— 挂在当前 step 下 |
| `onSystemPrompt` | transformer | ➖ | 无 —— 属于 prompt 改写型 hook，原样透传不改写 |

### 人工介入（HITL）感知

被挂起或中断的 turn 不会被当作正常完成上报。中间件通过监听事件流与 Reactor 信号，
在 `agent` / `entry` span 上打标记：

| 场景 | 信号 | `gen_ai.finish_reason` | `gen_ai.turn.completed` |
|---|---|---|:---:|
| 正常结束 | — | `normal` | `true` |
| 等待用户确认 | `RequireUserConfirmEvent` | `awaiting_user_confirm` | `false` |
| 等待外部执行 | `RequireExternalExecutionEvent` | `external_execution` | `false` |
| 全部工具被拒 | `AllToolsDeniedEvent` | `denied` | `true` |
| 达到 ReAct 最大轮次 | `ExceedMaxItersEvent` | `max_iters` | `true` |
| 流被取消 / 中断 | Reactor `CANCEL` | `interrupted` | `true` |
| 报错 | `onError` | `error` | `true` |

恢复时，在 agent 外部执行的工具会通过 `ExternalExecutionResultEvent` 到达
（`onActing` 看不到这些工具）；中间件会为每个工具合成一个 `tool` span，并标记
`gen_ai.tool.execution=external`。被取消的 `chat` / `tool` / `step` span 会标记
`gen_ai.incomplete=true`，从而把"卡在等人"的 span 与正常完成区分开。

## 字段来源（已对照 agentscope-java 源码核实）

| CLS 属性 | 来源 |
|---|---|
| `gen_ai.session.id` / `gen_ai.user.id` | `RuntimeContext.getSessionId()` / `getUserId()` |
| `gen_ai.turn.id` | `{sessionId}:t{N}`（每 turn 合成） |
| `gen_ai.agent.name` | `Agent.getName()` |
| `gen_ai.step.id` / `react.round` | 合成的 ReAct 轮次计数 |
| `gen_ai.request.model` | `Model.getModelName()` |
| `gen_ai.usage.*` | `ModelCallEndEvent.getUsage()` → `ChatUsage` |
| `gen_ai.tool.name` / `.call.id` | `ToolUseBlock.getName()` / `getId()` |
| `gen_ai.tool.call.arguments` | `ToolUseBlock.getInput()`（Map → JSON） |
| `gen_ai.tool.call.result` | 累积的 `ToolResultTextDeltaEvent.getDelta()` |
| `gen_ai.tool.call.state` | `ToolResultEndEvent.getState().getValue()` |
| `gen_ai.react.thought` | 累积的 `ThinkingBlockDeltaEvent.getDelta()`（写在 step span 上） |
| `gen_ai.output.messages` | 累积的 `TextBlockDeltaEvent.getDelta()` |
| `gen_ai.response.finish_reasons` / `react.finish_reason` | 依据 `ModelCallEndEvent` 之前是否出现过 `ToolCallStartEvent` 推导 |
| `gen_ai.finish_reason` / `turn.completed` | 依据 HITL 事件 + Reactor 信号推导（见 HITL 表） |

> **注意：** cache_creation 与 cache_read 的拆分、推理 token 数以及成本目前无法从框架获取
> —— 这些要么需要外部计价表，要么 AgentScope 根本不产出。

## 快速开始

### 1. 配置 CLS

通过环境变量、`.env` 文件（`./.env` 或 `~/.agentobs-agentscope/config.env`）
或 builder 参数提供凭证：

```bash
export CLS_ENDPOINT=ap-guangzhou.cls.tencentcs.com
export CLS_TOPIC_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export CLS_SECRET_ID=AKID...
export CLS_SECRET_KEY=...
# 可选
export CLS_SERVICE_NAME=my-agent-app
export CLS_DEBUG=true
```

#### 配置参数

解析优先级（从高到低）：**builder 参数 → 环境变量 → `.env` 文件**。
已存在的环境变量不会被 `.env` 文件覆盖。

| 环境变量 | Builder 方法 | 必填 | 默认值 | 说明 |
|---|---|:---:|---|---|
| `CLS_ENDPOINT` | `.endpoint(String)` | ✅ | — | CLS 地域域名，如 `ap-guangzhou.cls.tencentcs.com`。裸域名会自动补 `https://`，并自动追加 `/v1/traces`。 |
| `CLS_TOPIC_ID` | `.topicId(String)` | ✅ | — | CLS trace 主题 id；作为 `topic_id` HTTP 请求头发送。 |
| `CLS_SECRET_ID` | `.secretId(String)` | ✅ | — | 腾讯云 SecretId；组合进 HTTP Basic `Authorization` 请求头。 |
| `CLS_SECRET_KEY` | `.secretKey(String)` | ✅ | — | 腾讯云 SecretKey；组合进 HTTP Basic `Authorization` 请求头。 |
| `CLS_SERVICE_NAME` | `.serviceName(String)` | | `agentscope-app` | OTel `service.name` 资源属性。 |
| `CLS_SOURCE` | `.source(String)` | | 本机 IP（自动探测） | span 的来源标记；探测失败时回退为本机 IP，再回退 `127.0.0.1`。 |
| `CLS_BATCH_SIZE` | `.batchSize(Integer)` | | `32` | 单个导出批次的最大 span 数，限制在 `[1, 1000]`。 |
| `CLS_DEBUG` | `.debug(Boolean)` | | `false` | 详细日志。环境变量真值：`1` / `true` / `yes`。 |

若任一**必填**字段缺失，`build()` 会抛 `IllegalStateException`，并明确列出缺哪些。

### 2. 安装并挂载中间件

`install()` 会构建 OTLP 管道、注册全局 tracer 以及一个 JVM 关闭钩子。
按你的应用选择合适的配置方式：

```java
import com.tencentcloudapi.observability.agentscope.ClsConfig;
import com.tencentcloudapi.observability.agentscope.ClsObservability;
import io.agentscope.core.middleware.MiddlewareBase;

// (a) 无参 —— 全部从环境变量 / .env 解析
ClsObservability obs = ClsObservability.install();

// (b) 通过 builder 显式配置 —— 这里设置的值优先级高于 env / .env
ClsConfig config = ClsConfig.builder()
        .endpoint("ap-guangzhou.cls.tencentcs.com")
        .topicId("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
        .secretId(System.getenv("CLS_SECRET_ID"))    // 切勿把密钥写进源码
        .secretKey(System.getenv("CLS_SECRET_KEY"))
        .serviceName("my-agent-app")                 // 可选
        .batchSize(64)                               // 可选，默认 32
        .debug(true)                                 // 可选，默认 false
        .build();
ClsObservability obs = ClsObservability.install(config);

// (c) 部分覆盖 —— 只设置想覆盖的字段；
//     未设置的字段 build() 仍会从环境变量 / .env 解析
ClsConfig merged = ClsConfig.builder()
        .serviceName("my-agent-app")   // endpoint/topicId/secretId/secretKey 来自 env/.env
        .build();
ClsObservability obs = ClsObservability.install(merged);
```

> **切勿在源码中硬编码 SecretId / SecretKey。** 请从环境变量（如上）
> 或一个已被 git 忽略的 `.env` 文件读取。

然后把中间件挂到你的 agent 上：

```java
ReActAgent agent = ReActAgent.builder()
        .name("assistant")
        .model(model)
        .toolkit(toolkit)
        .middlewares(java.util.List.of((MiddlewareBase) obs.newTracingMiddleware()))
        .build();

// 自定义的 session / user 身份会直接流入 CLS 列：
RuntimeContext ctx = RuntimeContext.builder()
        .sessionId("session-1234")
        .userId("user-5678")
        .build();
```

Span 会被批量异步上传；JVM 关闭钩子会在退出时 flush。
调用 `obs.flush()` 可强制立即上传，`obs.shutdown()` 可停止。

## 构建

```bash
mvn -q clean package
```

AgentScope（`io.agentscope:agentscope-core`）与 `reactor-core` 声明为 `provided`
—— 它们由你的应用在运行期提供，不会打进本 jar。

> **注意：** 源码使用了 Java 17+ 语法特性（`instanceof` 模式匹配），
> 请使用 JDK 17 或更高版本构建。

## 示例

| 示例 | 作用 |
|---|---|
| `LoggingTracingExample` | 离线 demo —— 用 logging exporter 替换 OTLP exporter，把每个 span 属性打印到 stdout（即 OTLP 会上报的完整载荷）。无需 CLS 端点。 |
| `RealAgentScopeExample` | 用真实 `ReActAgent` 走 `ClsTracingMiddleware`，并把产出的 span 树上传到真实 CLS 端点。 |
| `RealUploadExample` | 手工构造与中间件产出一致的 span 树并上传到真实 CLS 端点 —— 便于在不跑 agent 的情况下验证字段/层级映射。 |

```bash
# 离线（无需凭证）
mvn -q exec:java -Dexec.mainClass="com.tencentcloudapi.observability.agentscope.example.LoggingTracingExample"

# 真实上传（需 CLS_* 配置；运行期需要 AgentScope 的示例请加 -Dexec.classpathScope=test）
mvn -q exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass="com.tencentcloudapi.observability.agentscope.example.RealAgentScopeExample"
```
