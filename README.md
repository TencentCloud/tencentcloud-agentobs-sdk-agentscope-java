# tencentcloud-agentobs-sdk-agentscope-java

> **English** | [中文](./README.zh-CN.md)

Tencent Cloud CLS observability SDK for **AgentScope Java**.

This SDK provides `ClsTracingMiddleware` — a custom OpenTelemetry-based tracing middleware
that captures rich agent execution details and exports spans to
[Tencent Cloud CLS](https://cloud.tencent.com/product/cls) over the standard **OTLP/HTTP**
protocol — vendor-neutral transport, data landing in CLS.

### Key features

- **Full span hierarchy** — `entry → agent → step → chat / tool` per user turn
- **Rich attribute capture** — session/user identity, tool arguments & results, reasoning thoughts, streamed output text, token usage (incl. cache)
- **Human-in-the-loop (HITL) awareness** — tracks confirm / external-exec / denied / interrupt states
- **Concurrency-safe & stateless** — per-invocation state via Reactor Context; single instance safe across concurrent turns
- **CLS GenAI Trace spec** compliant — all attributes use `gen_ai.*` dotted keys
- **Per-turn trace model** — each user interaction is one independent trace, sessions linked via `gen_ai.session.id`

## Span tree (per turn)

Each turn produces one trace whose spans share a single `traceID`:

```
entry (SERVER, enter_application)      — the turn root
└─ agent (INTERNAL, invoke_agent)      — one ReActAgent invocation
   ├─ step (INTERNAL, react, round 1)  — one ReAct round
   │  ├─ chat (CLIENT)                 — the model call
   │  └─ tool (CLIENT, execute_tool)   — same round ⇒ shares the step parent
   └─ step (INTERNAL, react, round 2)
      └─ chat (CLIENT)                  — final answer, no tool call
```

The ID chain follows: `gen_ai.session.id → gen_ai.turn.id ({sessionId}:t{N}) →
gen_ai.step.id ({turnId}:s{N})`.

### Middleware hook mapping

AgentScope exposes five middleware hooks. `ClsTracingMiddleware` is **stateless** — it holds
no per-turn fields; all turn/round state lives in per-invocation objects threaded through the
Reactor Context, so a single instance is safe across concurrent turns and HITL suspend/resume.

| Hook | Type | Used | Span produced |
|---|---|:---:|---|
| `onAgent` | onion | ✅ | `entry` (SERVER) + `agent` (INTERNAL) — turn root; also watches the stream for HITL events |
| `onReasoning` | onion | ✅ | `step` (INTERNAL, `react`) — one ReAct round (input assembly → model call → decode) |
| `onModelCall` | onion | ✅ | `chat` (CLIENT) — the raw model call, nested under the current step |
| `onActing` | onion | ✅ | `tool` (CLIENT, `execute_tool`) — nested under the current step |
| `onSystemPrompt` | transformer | ➖ | none — prompt-rewriting hook, passed through untouched |

### Human-in-the-loop (HITL) awareness

A parked or interrupted turn is not reported as a normal completion. The middleware watches
the event stream and Reactor signals to tag the `agent` / `entry` spans:

| Situation | Signal | `gen_ai.finish_reason` | `gen_ai.turn.completed` |
|---|---|---|:---:|
| Normal finish | — | `normal` | `true` |
| Awaiting user confirmation | `RequireUserConfirmEvent` | `awaiting_user_confirm` | `false` |
| Awaiting external execution | `RequireExternalExecutionEvent` | `external_execution` | `false` |
| All tool calls denied | `AllToolsDeniedEvent` | `denied` | `true` |
| Max ReAct iterations hit | `ExceedMaxItersEvent` | `max_iters` | `true` |
| Stream cancelled / interrupted | Reactor `CANCEL` | `interrupted` | `true` |
| Error | `onError` | `error` | `true` |

On resume, tools executed outside the agent arrive via `ExternalExecutionResultEvent`
(which `onActing` never sees); the middleware synthesises a `tool` span for each, tagged
`gen_ai.tool.execution=external`. Cancelled `chat` / `tool` / `step` spans are marked
`gen_ai.incomplete=true` so a "stuck waiting" span is distinguishable from a clean finish.

## Field sources (verified against agentscope-java source)

| CLS attribute | Source |
|---|---|
| `gen_ai.session.id` / `gen_ai.user.id` | `RuntimeContext.getSessionId()` / `getUserId()` |
| `gen_ai.turn.id` | `{sessionId}:t{N}` (synthetic per turn) |
| `gen_ai.agent.name` | `Agent.getName()` |
| `gen_ai.step.id` / `react.round` | synthetic ReAct round counter |
| `gen_ai.request.model` | `Model.getModelName()` |
| `gen_ai.usage.*` | `ModelCallEndEvent.getUsage()` → `ChatUsage` |
| `gen_ai.tool.name` / `.call.id` | `ToolUseBlock.getName()` / `getId()` |
| `gen_ai.tool.call.arguments` | `ToolUseBlock.getInput()` (Map → JSON) |
| `gen_ai.tool.call.result` | accumulated `ToolResultTextDeltaEvent.getDelta()` |
| `gen_ai.tool.call.state` | `ToolResultEndEvent.getState().getValue()` |
| `gen_ai.react.thought` | accumulated `ThinkingBlockDeltaEvent.getDelta()` (on the step span) |
| `gen_ai.output.messages` | accumulated `TextBlockDeltaEvent.getDelta()` |
| `gen_ai.response.finish_reasons` / `react.finish_reason` | derived from whether a `ToolCallStartEvent` was seen before `ModelCallEndEvent` |
| `gen_ai.finish_reason` / `turn.completed` | derived from HITL events + Reactor signal (see HITL table) |

> **Note:** cache_creation vs cache_read split, reasoning-token count, and cost are not
> available from the framework — these require an external pricing table or are simply not
> emitted by AgentScope.

## Quick start

### 1. Configure CLS

Provide credentials via env vars, a `.env` file (`./.env` or
`~/.agentobs-agentscope/config.env`), or builder args:

```bash
export CLS_ENDPOINT=ap-guangzhou.cls.tencentcs.com
export CLS_TOPIC_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export CLS_SECRET_ID=AKID...
export CLS_SECRET_KEY=...
# optional
export CLS_SERVICE_NAME=my-agent-app
export CLS_DEBUG=true
```

#### Configuration parameters

Resolution priority (highest first): **builder args → environment variables → `.env` file**.
Existing environment variables are never overridden by the `.env` file.

| Env var | Builder method | Required | Default | Description |
|---|---|:---:|---|---|
| `CLS_ENDPOINT` | `.endpoint(String)` | ✅ | — | CLS region host, e.g. `ap-guangzhou.cls.tencentcs.com`. A bare host is auto-prefixed with `https://`; `/v1/traces` is appended automatically. |
| `CLS_TOPIC_ID` | `.topicId(String)` | ✅ | — | CLS trace topic id; sent as the `topic_id` HTTP header. |
| `CLS_SECRET_ID` | `.secretId(String)` | ✅ | — | Tencent Cloud SecretId; combined into the HTTP Basic `Authorization` header. |
| `CLS_SECRET_KEY` | `.secretKey(String)` | ✅ | — | Tencent Cloud SecretKey; combined into the HTTP Basic `Authorization` header. |
| `CLS_SERVICE_NAME` | `.serviceName(String)` | | `agentscope-app` | OTel `service.name` resource attribute. |
| `CLS_SOURCE` | `.source(String)` | | local IP (auto-detected) | Source tag for the spans; falls back to the machine's local IP, then `127.0.0.1`. |
| `CLS_BATCH_SIZE` | `.batchSize(Integer)` | | `32` | Max spans per export batch. Clamped to `[1, 1000]`. |
| `CLS_DEBUG` | `.debug(Boolean)` | | `false` | Verbose logging. Truthy env values: `1` / `true` / `yes`. |

If any **required** field is missing, `build()` throws `IllegalStateException` listing
exactly which ones are absent.

### 2. Install and attach the middleware

`install()` builds the OTLP pipeline and registers a global tracer plus a JVM shutdown hook.
Pick whichever configuration style fits your app:

```java
import com.tencentcloudapi.observability.agentscope.ClsConfig;
import com.tencentcloudapi.observability.agentscope.ClsObservability;
import io.agentscope.core.middleware.MiddlewareBase;

// (a) zero-arg — resolve everything from env vars / .env
ClsObservability obs = ClsObservability.install();

// (b) explicit config via builder — values here win over env / .env
ClsConfig config = ClsConfig.builder()
        .endpoint("ap-guangzhou.cls.tencentcs.com")
        .topicId("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
        .secretId(System.getenv("CLS_SECRET_ID"))    // keep secrets out of source
        .secretKey(System.getenv("CLS_SECRET_KEY"))
        .serviceName("my-agent-app")                 // optional
        .batchSize(64)                               // optional, default 32
        .debug(true)                                 // optional, default false
        .build();
ClsObservability obs = ClsObservability.install(config);

// (c) partial builder — set only what you want to override;
//     any field left unset is still resolved from env vars / .env by build()
ClsConfig merged = ClsConfig.builder()
        .serviceName("my-agent-app")   // endpoint/topicId/secretId/secretKey come from env/.env
        .build();
ClsObservability obs = ClsObservability.install(merged);
```

> **Never hard-code SecretId / SecretKey in source.** Read them from env vars (as above)
> or a `.env` file that is git-ignored.

Then attach the middleware to your agent:

```java
ReActAgent agent = ReActAgent.builder()
        .name("assistant")
        .model(model)
        .toolkit(toolkit)
        .middlewares(java.util.List.of((MiddlewareBase) obs.newTracingMiddleware()))
        .build();

// custom session / user identity flows straight into CLS columns:
RuntimeContext ctx = RuntimeContext.builder()
        .sessionId("session-1234")
        .userId("user-5678")
        .build();
```

Spans are batched and uploaded asynchronously; a JVM shutdown hook flushes on exit.
Call `obs.flush()` to force an immediate flush, or `obs.shutdown()` to stop.

## Build

```bash
mvn -q clean package
```

AgentScope (`io.agentscope:agentscope-core`) and `reactor-core` are declared `provided`
— they are supplied by your application at runtime and are not bundled into this jar.

> **Note:** the source uses Java 17+ language features (pattern matching for `instanceof`).
> Build with JDK 17 or newer.

## Examples

| Example | What it does |
|---|---|
| `LoggingTracingExample` | Offline demo — swaps the OTLP exporter for a logging exporter and prints every span attribute to stdout (the exact payload OTLP would ship). No CLS endpoint needed. |
| `RealAgentScopeExample` | Runs a real `ReActAgent` through `ClsTracingMiddleware` and uploads the resulting span tree to a live CLS endpoint. |
| `RealUploadExample` | Hand-builds the same span tree the middleware emits and uploads it to a live CLS endpoint — useful for validating field/hierarchy mapping without an agent run. |

```bash
# offline (no credentials)
mvn -q exec:java -Dexec.mainClass="com.tencentcloudapi.observability.agentscope.example.LoggingTracingExample"

# live upload (needs CLS_* config; add -Dexec.classpathScope=test for examples needing AgentScope at runtime)
mvn -q exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass="com.tencentcloudapi.observability.agentscope.example.RealAgentScopeExample"
```
