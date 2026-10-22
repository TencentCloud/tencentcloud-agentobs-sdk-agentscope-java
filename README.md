# tencentcloud-agentobs-sdk-agentscope-java

> **English** | [中文](./README.zh-CN.md) · see also [CHANGELOG](./CHANGELOG.md)

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

### Framework-internal model calls (memory compression / summarisation)

The middleware chain only sees model calls routed through `onModelCall`. The SDK additionally
hooks AgentScope's `TracerRegistry` — the lowest-level entry behind `ChatModelBase.stream()` — to
cover components that **bypass the chain** and call `Model.stream()` directly.

#### Measured against the published bytecode

| Source | Path | Captured by the middleware alone |
|---|---|:---:|
| `ReActAgent` normal turn | `onModelCall → Model.stream()` | ✅ |
| `ReActAgent` out-of-iterations summary (`summarizing()`) | `MiddlewareChain → onModelCall` | ✅ |
| **`agentscope-harness` memory flush** | **`MemoryFlushManager` → direct `Model.stream()`, scheduled from `onAgent`'s `doOnComplete`** | **❌** |
| **`AutoContextMemory` compression** (1.x extension) | **`callModelForSummary` → direct `Model.stream()`; 4 call sites, 0 `MiddlewareChain` refs** | **❌** |

The one that shows up in practice is `agentscope-harness`: `MemoryFlushMiddleware` implements only
`onAgent`, and after the turn completes it hands `MemoryFlushManager.flushMemories(...)` to a
Scheduler. That call uses harness's own `DEFAULT_FLUSH_PROMPT` ("You are a memory extraction
assistant…") and never passes through `onModelCall`, so the middleware alone produces no span for
it. This is the extra `chat` span an agent-based probe shows under `invoke_agent`.

`AutoContextMemory` (from `io.agentscope:agentscope-extensions-autocontext-memory`) has the same
shape — a synchronous `callModelForSummary` ending in `Mono.block()`. Note it is published only for
AgentScope **1.x**: it implements `io.agentscope.core.state.StateModule` and references
`core.session.Session`, neither of which exists in core 2.0.x, so it cannot load there. It is
listed because the capture mechanism covers it, not because 2.x users will hit it.

Because `ChatModelBase.stream()` is `final` and its first instruction is `TracerRegistry.get()`,
hooking there sees **every** model call regardless of which layer above decided to make it.

#### Turn correlation

The hard part is not capturing these calls, it is knowing which turn they belong to. The framework
runs memory work **after the agent has replied**, from a callback, often on another thread — so the
Reactor Context is empty by then. The owning turn is resolved through five channels, tried in order:

| Channel | Covers |
|---|---|
| Reactor Context | the call sits inside the agent's reactive chain (incl. `subscribeOn`/`publishOn`) |
| ThreadLocal, bridged on every Reactor signal | an imperative `Mono.block()` fired from a callback |
| **Scheduler-hop snapshot** | a **detached** subscription handed to a Scheduler — `Mono.defer(..).subscribeOn(..).subscribe(cb, cb)`, which is what `agentscope-harness`'s `MemoryFlushMiddleware` does |
| OTel `Context` | a thread hop the host propagated explicitly, or via a context-propagating executor / java agent |
| Per-session index of the most recent turn | a post-turn call with no other hint, when a single session is tracked |

The third channel is the one that matters on a real multi-user service. `MemoryFlushMiddleware`
overrides only `onAgent`, schedules the flush from `Flux.doOnComplete(...)`, and writes nothing to
the context (verified: zero `contextWrite` calls in it or `MemoryFlushManager`) — so by the time the
model is called, Reactor Context, ThreadLocals and OTel Context are all gone. The SDK therefore
registers a `Schedulers.onScheduleHook` that snapshots the turn on the thread **submitting** the
task — where it is still in scope — and restores it around the task on the worker thread. This is
the same mechanism OpenTelemetry's own `reactor-3.1` instrumentation uses.

The snapshot holds **values only** — our ids, a `SpanContext`, and the turn's step counter — never
the turn's spans, so a long-delayed task cannot pin whole prompts in memory. It is also why the
span is filed under the turn that *submitted* it rather than whichever turn happens to be current
when it runs: a user's second message can start turn 2 while turn 1's flush is still pending.

- Every such span carries `gen_ai.model.call.origin=framework`, making them easy to isolate in CLS.
- `ClsTracingMiddleware.currentTurnOtelContext()` exposes the turn's OTel `Context` if you dispatch
  memory work onto your own executor and want to propagate it explicitly.
- Pinned by `ClsHarnessMemoryFlushTest`, `ClsDetachedSchedulerCorrelationTest` and
  `ClsTurnSnapshotTest`.

**Remaining limitation** — when the **submitting** thread carries no turn either (a bare
`ExecutorService`, a timer thread) and several sessions are tracked, attribution is deliberately
declined: cross-attributing one tenant's memory call to another tenant's trace would corrupt the
data. The span is still recorded, just on its own trace with no `gen_ai.session.id`. Propagating
`currentTurnOtelContext()` removes the ambiguity.

#### Where the span attaches

Per the CLS GenAI spec a `chat` span lives under a `step`:

| When the call happens | Where the span attaches |
|---|---|
| During a ReAct round | reuses that round's `step` |
| During a turn but outside any round | a `step` is synthesised (`gen_ai.step.type=framework`, id `{turnId}:f{N}`) |
| After the turn finished (the usual memory-flush case) | a synthesised `step` under that turn's `agent` span |
| Attribution ambiguous or no turn at all (background job) | forms its own independent trace |

- **No double counting** — the middleware publishes a marker on the Reactor Context around its own
  `chat` span; because a Reactor Context propagates downstream-to-upstream, the low-level entry
  recognises an already-instrumented call and stays silent.
- **Round numbering untouched** — synthesised steps use a separate counter (`:f{N}`), so
  `gen_ai.react.round` and the `:s{N}` ids are unaffected. A round's `step` span is deliberately
  kept open past its own reasoning Flux (to host that round's tool span), so the SDK tracks whether
  reasoning has finished — otherwise post-turn memory work would be filed inside the last round.
- **No probe displaced** — a previously registered tracer is kept as a delegate and keeps receiving
  every callback.
- **Switchable** — `CLS_CAPTURE_FRAMEWORK_CALLS=false` turns the capture off entirely;
  `CLS_PROPAGATE_ACROSS_SCHEDULERS=false` keeps capturing but installs no Scheduler hook, so
  detached calls are traced without turn correlation.
- **Degrades safely** — if that entry point is unavailable in a future AgentScope, the SDK logs a
  warning and falls back to middleware-only tracing.


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
| `CLS_CAPTURE_FRAMEWORK_CALLS` | `.captureFrameworkModelCalls(Boolean)` | | `true` | Trace model calls that bypass the middleware chain (memory flush / summarisation). Off values: `0` / `false` / `no`. |
| `CLS_PROPAGATE_ACROSS_SCHEDULERS` | `.propagateAcrossSchedulers(Boolean)` | | `true` | Carry turn context across Reactor `Scheduler` hops, so a detached post-turn memory call is attributed to its turn. Installs a process-wide `Schedulers.onScheduleHook`; turn off to leave the shared Schedulers untouched — such calls are then still traced, just uncorrelated. Off values: `0` / `false` / `no`. |
| `CLS_CAPTURE_FORMAT_CALLS` | `.captureFormatCalls(Boolean)` | | `false` | Add a `format` span per prompt-formatting call. **Off by default**: a formatter runs on every model call, so this roughly doubles a turn's span count while recording a re-encoding of messages already on the sibling `chat` span. Turn it on to debug provider-specific prompt assembly. On values: `1` / `true` / `yes`. |

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

## Interop with Aliyun ARMS Java Agent (and other OTel javaagents)

Starting from **0.0.4**, this SDK ships with its OpenTelemetry runtime shaded — all
`io.opentelemetry.*` classes are relocated to
`com.tencentcloudapi.observability.shaded.io.opentelemetry.*` inside the published jar
(together with the transitive OkHttp / okio dependencies used by the OTLP/HTTP exporter).

### Why this matters

If your application is launched with an OpenTelemetry Java agent — e.g. Aliyun ARMS
(`-javaagent:aliyun-java-agent.jar`), DataDog APM, or Dynatrace OneAgent — that agent
instruments `io.opentelemetry.api.trace.Tracer` at bytecode level: calls like
`tracer.spanBuilder(name).setParent(ctx).startSpan()` are silently redirected to the agent's
internal tracer, which **ignores the `.setParent(ctx)` you pass**. The result in CLS is a
flat list of orphaned single-span "traces" — every span claims to be a root, and the
`enter_application → invoke_agent → react → chat / tool` hierarchy is lost.

Because our shaded Tracer class name is `com.tencentcloudapi.observability.shaded.io.opentelemetry.api.trace.Tracer`,
the Aliyun agent's bytecode matcher (which only matches `io.opentelemetry.*`) **cannot find
our classes**, so its instrumentation never fires on our code paths. The two tracing stacks
coexist in physical isolation.

### Operational effect

| Capability | 0.0.3 and earlier | **0.0.4+ (shaded)** |
|---|---|---|
| JVM start flag required | Needed `-Dotel.instrumentation.opentelemetry-api.enabled=false` | **None** — zero changes |
| CLS trace tree integrity | ✅ (only after the flag) | ✅ |
| Aliyun ARMS JVM / HTTP / DB / middleware auto-instrumentation | ✅ | ✅ |
| Keeping AgentScope's built-in `OtelTracingMiddleware` alongside | ⚠️ triggered NPE in `AgentContextWrapper.get()` after the flag | ✅ coexists cleanly — reports to ARMS as usual, we report to CLS |
| Jar size | ~30 KB | ~7.6 MB (OTel runtime bundled) |

### Caveats

- The shaded SDK runs its OTel pipeline entirely in-process; **nothing of it is visible to
  `GlobalOpenTelemetry` or to a host javaagent**. This is intentional. If you had code that
  relied on `GlobalOpenTelemetry.get().getTracer(...)` to receive our spans, that path no
  longer works — but it never worked reliably with a competing javaagent in place anyway.
- Our middleware uses its own Reactor Context key for OTel Context propagation; this means
  our `invoke_agent → chat / execute_tool` spans form a single trace independent of any
  context propagated by the host app or by AgentScope's own `OtelTracingMiddleware`.

> **Migration from 0.0.3**: delete `-Dotel.instrumentation.opentelemetry-api.enabled=false`
> from your `JAVA_OPTS` and bump the dependency to `0.0.4`. Both middlewares
> (`ClsTracingMiddleware` for CLS and AgentScope's `OtelTracingMiddleware` for ARMS) can
> now coexist without any additional configuration.

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
