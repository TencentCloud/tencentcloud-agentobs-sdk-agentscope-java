# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.5] — 2026-10-22

Closes the two gaps that made a CLS trace less useful than a lower-level probe's: model calls the
agent middleware never sees were missing entirely, and the calls it did see recorded their outcome
but not what was asked for.

| Area | What changed |
|---|---|
| Coverage | Model calls bypassing the middleware chain (the framework's memory extraction) are now traced and correlated to their turn through five channels; prompt formatting is traceable opt-in |
| Attributes | 22 new keys (38 → 60) — sampling parameters, `gen_ai.tool.definitions`, `gen_ai.system_instructions`, `gen_ai.response.id`, `gen_ai.provider.name`, agent/tool descriptions, per-token latency |
| Correctness | Spans are no longer dropped when an application flushes and shuts down right after its last call; post-turn work is no longer filed inside the last ReAct round |
| Verification | End-to-end against live CLS and against the real `agentscope-harness` artifact, not a reproduction |

No breaking changes: every addition is opt-out (`CLS_CAPTURE_FRAMEWORK_CALLS`,
`CLS_PROPAGATE_ACROSS_SCHEDULERS`) or opt-in (`CLS_CAPTURE_FORMAT_CALLS`), and existing attribute
names and the span hierarchy are unchanged.

### Added

- **Prompt formatting can now be traced**, opt-in via `CLS_CAPTURE_FORMAT_CALLS` (or
  `ClsConfig.builder().captureFormatCalls(true)`). `MiddlewareBase` has no formatter hook at all —
  only `onAgent` / `onReasoning` / `onActing` / `onModelCall` / `onSystemPrompt` — so
  `Tracer.callFormat` is the only place this stage is observable. Each call adds a `format` span
  carrying `agentscope.format.formatter`, `agentscope.format.input_count` and
  `agentscope.format.count`.

  **It is a sibling of `chat`, not its child.** Formatting happens while the model `Flux` is being
  *assembled* (`OpenAIChatModel.doStream0` calls `formatter.format(...)` in its own method body),
  which is before anything subscribes and therefore before the `chat` span exists. Both hang off
  the round's `step`, in call order. The reference java-agent ends up with the same shape — its
  model instrumentation never makes the chat span current, so its `format` span is not nested
  under it either.

  Off by default: a formatter runs on every model call, so this roughly doubles a turn's span
  count while recording a re-encoding of the messages already on the sibling `chat` span. The
  messages themselves are deliberately *not* repeated on the `format` span.

  A formatter call that cannot be attributed to a live round is dropped rather than recorded on
  its own trace — unlike a model call, where an orphan span still represents real token spend. A
  `format` span only means anything next to the `chat` it prepared, and since formatting runs on
  every call, guessing would produce a steady stream of mis-parented spans rather than one stray.

  **Known gap:** a call that bypasses the middleware chain (`gen_ai.model.call.origin=framework`)
  gets no `format` span. It never passes through `onModelCall`, so no assembly window is opened,
  and it is by definition not inside a ReAct round, so there is no step to attach to. Verified
  end-to-end: a turn issuing two ReAct calls and two framework calls yields four `chat` spans but
  two `format` spans. Pinned by `ClsFormatSpanTest.frameworkOriginCallsGetNoFormatSpan`.

- **A `chat` span now records what the call actually asked for, not just its outcome.** Both model
  paths — the middleware's `onModelCall` and the framework-level tracer — go through one shared
  helper, so they cannot drift:

  | Attribute | Why it matters |
  |---|---|
  | `gen_ai.request.temperature`, `top_p`, `top_k`, `max_tokens`, `seed`, `frequency_penalty`, `presence_penalty` | The same prompt at a different temperature is a different experiment; without these a recorded call is not reproducible. |
  | `gen_ai.tool.definitions` | The tool schemas offered to the model (name, description, parameter schema). Previously only `gen_ai.request.tool_count` was recorded, which left a model that *failed* to call an expected tool indistinguishable from one never offered it. |
  | `gen_ai.system_instructions` | The system prompt, readable without parsing `gen_ai.input.messages` (which still carries it — it remains the verbatim record of the call). |
  | `gen_ai.response.id` | The provider's own completion id — the only field that reconciles a span against the model vendor's billing/audit records. Framework path only; `ModelCallEndEvent` carries no response id. |
  | `gen_ai.response.time_per_output_token_ms` | Mean decode latency. Together with `time_to_first_token_ms` it separates a slow provider queue from slow generation, which total duration alone cannot. |
  | `gen_ai.provider.name` | Who actually served the call, normalised from the model's implementing class (`openai`, `dashscope`, `anthropic`, `gcp.gemini`, `ollama`). AgentScope exposes only a model *name* (`hy4-preview`), so without this cost and latency cannot be grouped by vendor. Unrecognised implementations yield **no** attribute rather than a guess — a wrong value silently corrupts per-provider aggregates, and an OpenAI-compatible gateway is not OpenAI. |

- **Agent and tool identity.** `gen_ai.agent.description` (the agent's configured role — what tells
  two agents apart in a trace where names are generic) and `gen_ai.tool.description`, resolved by
  name against the agent's `Toolkit` because `ActingInput` carries only `ToolUseBlock`s. The latter
  answers a question the call record alone cannot: whether the model picked the *wrong* tool, or the
  right tool whose description misled it.

  `gen_ai.agent.description` is omitted when the agent has none. `Agent.getDescription()` is a
  `default` method that synthesises `"Agent({agentId}) {name}"` when unset and so never returns
  null; copying that blindly would write a value on every agent span that merely restates
  `gen_ai.agent.id` and `gen_ai.agent.name` from the same span. Pinned by
  `ClsAgentTraceIntegrationTest.anAgentWithoutADescriptionCarriesNoDescriptionAttribute`.

  Credentials are never recorded. `GenerateOptions` exposes `getApiKey()` and `getBaseUrl()`
  alongside the sampling parameters, so the copy is a strict field whitelist and
  `ClsRequestAttributesTest.credentialsAreNeverWrittenToASpan` fails on any attribute of any span
  that contains either value.

  **What these values mean, precisely.** They are the options the *caller* passed for this call —
  for a `ReActAgent`, its `generateOptions`. A model instance may carry its own defaults
  (`OpenAIChatModel` keeps a private `configuredOptions` from its builder) and merges them inside
  `doStream`, below every observable boundary and with no accessor. So an absent attribute means
  "the caller did not specify it", *not* "the provider used its default"; configure sampling on the
  agent to make it observable.

- **Model calls that bypass the middleware chain are now traced.** The SDK registers a tracer on
  AgentScope's `TracerRegistry` — the entry point behind the `final` `ChatModelBase.stream()`, whose
  very first instruction is `TracerRegistry.get()` — so every model call is seen regardless of which
  layer above decided to make it. Such spans carry `gen_ai.model.call.origin=framework`.

  The case that shows up in practice is `agentscope-harness`, whose `MemoryFlushMiddleware` runs
  `MemoryFlushManager.flushMemories(...)` with harness's own `DEFAULT_FLUSH_PROMPT` ("You are a
  memory extraction assistant…") and never reaches `onModelCall()` — so `ClsTracingMiddleware`
  alone produced no span for it, while probes hooking the framework lower down did.

  **Verified against the real artifact, not a reproduction.** `demo/RealHarnessMemoryDemo` depends
  on `io.agentscope:agentscope-harness:2.0.1` and drives the genuine `MemoryFlushMiddleware`; an
  end-to-end run against live CLS stores 11 spans on one trace, the memory call among them as
  `framework_step → chat [origin=framework]` under `invoke_agent`, carrying the turn's
  `gen_ai.session.id` / `gen_ai.turn.id` and harness's real flush prompt.

  Two harness versions are two different shapes, and both are covered:

  | harness | shape | how it is correlated |
  |---|---|---|
  | 2.0.1 (core 2.0.1) | `next.apply(input).concatWith(Mono.defer(..).subscribeOn(boundedElastic))` — part of the main chain | Reactor Context is still visible |
  | 2.0.4 (core 2.0.4) | `doOnComplete(() -> Mono.defer(..).subscribeOn(..).subscribe(..))` — a detached subscription | the `Schedulers` hook, then the per-session index |

  The 2.0.1 shape is exercised with the real artifact; 2.0.4 pins to a different core version and
  so cannot be installed alongside it, and is reproduced by `demo/HarnessMemoryFlushDemo` plus
  `ClsHarnessMemoryFlushTest`.

  `AutoContextMemory` (1.x extension) has the same bypassing shape via its synchronous
  `callModelForSummary`, but **cannot load on core 2.0.x** at all: it implements
  `io.agentscope.core.state.StateModule` and references `core.session.Session`, neither of which
  exists there. It is therefore not a usable example for this SDK's target version.

  **Scope note:** `agentscope-core` itself does not have this problem. Verified on 2.0.1, every
  `ReActAgent` model call — including the out-of-iterations summarisation prompt from
  `summarizing()` — already flows through `MiddlewareChain` → `onModelCall`.
  `ClsSummarisationCaptureTest` pins that behaviour so an upstream change is caught.

- A `step` span is synthesised to hold such a call when it happens outside any ReAct round, carrying
  `gen_ai.step.type=framework` and an id of `{turnId}:f{N}`. This keeps the tree in the spec shape
  (`chat` always under a `step`) without disturbing `gen_ai.react.round` or the `{turnId}:s{N}` ids
  of real rounds.

- **Turn correlation across a Scheduler hop.** The framework's memory work is dispatched as a
  *detached* subscription — `Mono.defer(..).subscribeOn(..).subscribe(cb, cb)` from a
  `doOnComplete`, with nothing written to the context (verified: zero `contextWrite` calls in
  `MemoryFlushMiddleware` or `MemoryFlushManager`). By the time the model is called, Reactor
  Context, ThreadLocals and OTel `Context` are all gone, so such spans could previously only be
  attributed when a single session happened to be in flight; on a multi-user service they became
  orphan traces. The SDK now installs a `Schedulers.onScheduleHook` that snapshots the turn on the
  thread *submitting* the task — where it is still in scope — and restores it around the task on the
  worker thread, leaving that thread exactly as it was found. This is the same mechanism
  OpenTelemetry's own `reactor-3.1` instrumentation uses.

  The snapshot holds **values only**: our ids, a `SpanContext` (re-wrapped with `Span.wrap`), and
  the turn's step counter. It never references the turn's spans, so a long-delayed task cannot pin
  their attributes — whole prompts — in memory. Holding values also means the span is filed under
  the turn that *submitted* it, not whichever turn is current when it runs: looking the turn back up
  by session id would return turn N+1 once the same user sent their next message.

  Correlation is resolved through five channels, tried in order: Reactor Context → ThreadLocal →
  Scheduler-hop snapshot → OTel `Context` → per-session index of the most recent turn.

- `ClsTracingMiddleware.currentTurnOtelContext()` exposes the active turn's OTel `Context` so a host
  that dispatches memory/summarisation work onto its own executor can propagate it explicitly.

- `CLS_CAPTURE_FRAMEWORK_CALLS` / `ClsConfig.Builder.captureFrameworkModelCalls(Boolean)` to turn
  the framework-level tracer off. Default `true`.

- `CLS_PROPAGATE_ACROSS_SCHEDULERS` / `ClsConfig.Builder.propagateAcrossSchedulers(Boolean)` to skip
  the `Schedulers` hook, which is process-wide: every task submitted to any Reactor Scheduler in the
  host passes through it (tasks carrying no turn are returned unwrapped after three `ThreadLocal`
  reads). With it off, framework calls are still traced — they simply are not correlated to a turn.
  Default `true`.

- `scripts/otlp_receiver.py`, a dependency-free OTLP/HTTP receiver that decodes and prints the span
  tree the SDK actually uploads. Used to verify the export path end to end — connection,
  `Authorization` / `topic_id` headers, protobuf serialisation and attribute fidelity — without
  needing live CLS credentials. Dumps every span's attributes by default; narrow with
  `OTLP_DUMP_KINDS=chat,tool`.

- `demo/.../RealHarnessMemoryDemo`, which drives the **real** `MemoryFlushMiddleware` from
  `io.agentscope:agentscope-harness:2.0.1` rather than a reproduction, so an upstream change of
  call shape shows up here instead of being papered over.

- `demo/.../HarnessMemoryFlushDemo`, which reproduces harness **2.0.4**'s detached memory-flush
  shape across several concurrent sessions (the shape a multi-user chat bot has). Kept alongside
  the real-artifact demo because 2.0.4 pins core 2.0.4 and cannot be installed next to 2.0.1.

### Fixed

- **Spans created on a Reactor chain could be dropped when the application flushed and shut down
  immediately after its last call.** Reactor runs a `doFinally` callback *after* propagating
  `onComplete` downstream, so a blocking caller (`agent.call(...).block()`, or the imperative
  `Mono.block()` memory work uses) was released while `span.end()` was still pending on the emitting
  thread. A CLI or batch job calling `flush()` / `shutdown()` on its last line lost those spans —
  they were ended after the exporter had closed. Spans are now ended via `doOnTerminate` /
  `doOnCancel`, i.e. before the terminal signal reaches the subscriber. Pinned by
  `ClsSpanEndOrderingTest`.

- **Post-turn framework calls were filed inside the last ReAct round.** A round's `step` span is
  deliberately kept open past its own reasoning Flux so the round's tool span can nest under it, so
  "the turn has a current step" did not mean a round was in flight. `StepState` now tracks whether
  reasoning has terminated (set from `doOnTerminate`, before the signal propagates, since the flush
  is scheduled from an outer `doOnComplete`), and only a genuinely live round is adopted as parent.

### Known limitations

- **When the submitting thread carries no turn either and several sessions are tracked, a framework
  model call is deliberately NOT attributed.** Cross-attributing one tenant's memory call to
  another tenant's trace would corrupt the data, so such a span is still recorded but forms its own
  trace with no `gen_ai.session.id`. Propagate `currentTurnOtelContext()` to remove the ambiguity.
  Pinned by `ClsAsyncMemoryCorrelationTest` and `ClsHarnessMemoryFlushTest`.

### Changed

- `ClsTracingMiddleware` publishes a marker on the Reactor Context around its
  `chat` span. The framework-level tracer sees it (Reactor Contexts propagate
  downstream-to-upstream) and skips the call, so a regular agent model call
  still yields exactly one `chat` span.
- A tracer already registered with AgentScope (another vendor's probe, a user
  tracer) is kept as a delegate and still receives every callback, so
  installing CLS observability never silently disables an existing integration.
- Registering a tracer switches on a JVM-wide `Hooks.onEachOperator` that calls
  `Tracer.runWithContext` around every Reactor signal. The SDK relies on it: that
  is where the Reactor Context is bridged to ThreadLocals, which is the only way
  a `Mono.block()` caller can find its owning turn. The hook is therefore left enabled.

## [0.0.4] — 2026-10-12

### Fixed

- **Trace tree becomes a flat list of orphan spans when the application is
  launched under an OpenTelemetry Java agent (notably Aliyun ARMS
  `aliyun-java-agent.jar`).** The agent instruments
  `io.opentelemetry.api.trace.Tracer` at bytecode level and silently redirects
  `tracer.spanBuilder(name).setParent(ctx).startSpan()` calls to its own
  internal tracer, which ignores the explicit `.setParent(ctx)`. CLS used to
  receive one single-span trace per operation instead of a connected
  `enter_application → invoke_agent → react → chat / execute_tool` tree.
- Removed the need for the `-Dotel.instrumentation.opentelemetry-api.enabled=false`
  JVM workaround that was previously documented. That flag worked around our
  bytecode-hijack issue but broke AgentScope's own `OtelTracingMiddleware`
  with an NPE in the Aliyun agent's `AgentContextWrapper.get()`.

### Changed

- **The published jar now bundles the OpenTelemetry runtime, shaded to
  `com.tencentcloudapi.observability.shaded.io.opentelemetry.*`.** Transitive
  `okhttp3.*` / `okio.*` (used by the OTLP/HTTP exporter) are relocated under
  the same prefix. The Aliyun agent's bytecode matcher — which only looks for
  `io.opentelemetry.*` — can no longer find our classes, so our Tracer /
  Context / SpanBuilder operate in physical isolation.
- `ClsTracingMiddleware` uses its own Reactor Context key for OTel Context
  propagation. Our `invoke_agent → chat / execute_tool` spans form a single
  trace independent of any context propagated by AgentScope's built-in
  `OtelTracingMiddleware` or the host application.
- Published jar size: **~30 KB → ~7.6 MB** (one-time cost; OTel SDK + OTLP
  exporter + OkHttp are now bundled).
- Dependency `opentelemetry-reactor-3.1` moved from `provided` to `compile`
  so it is shaded into the jar.
- `agentscope-core` reference version aligned to **2.0.1** to match production
  deployments (API-compatible with 2.0.0; declared as `provided`, so your
  project's own `agentscope-core` version continues to be used at runtime).

### Added

- New README section **"Interop with Aliyun ARMS Java Agent"**
  ([English](./README.md) / [中文](./README.zh-CN.md)) explaining the shade
  mechanism, operational impact, caveats, and migration from 0.0.3.
- `demo/` sub-project reproducing the double-middleware scenario end to end
  (real `ReActAgent` + `ClsTracingMiddleware` + OTLP upload to CLS).
- `scripts/verify-with-aliyun-agent.sh` regression script that launches the
  demo under the Aliyun javaagent with **no workaround flag** and expects one
  connected 7-span trace in CLS.
- `.env.example` placeholder config file (no real credentials).

### Compatibility

- **Public API: fully source-compatible with 0.0.3.** The entry points
  (`ClsObservability.install()`, `ClsObservability.install(ClsConfig)`,
  `obs.newTracingMiddleware()` → cast to `MiddlewareBase`) are unchanged.
- **Caveat — internal getters now return shaded types.** The low-level
  accessors `obs.openTelemetry()`, `obs.tracerProvider()`, `obs.exporter()`,
  and `obs.tracer()` return `com.tencentcloudapi.observability.shaded.io.opentelemetry.*`
  types instead of the public `io.opentelemetry.*` types. If you were using
  these to wire additional span processors or share the OTel pipeline with
  your own code, that integration no longer compiles. These methods are
  considered internal and are retained for diagnostic use only.
- **Migration note.** If you added
  `-Dotel.instrumentation.opentelemetry-api.enabled=false` to `JAVA_OPTS` for
  0.0.3, remove it. `ClsTracingMiddleware` (reporting to CLS) and AgentScope's
  built-in `OtelTracingMiddleware` (reporting to ARMS) now coexist on the same
  middleware chain without any additional configuration.

## [0.0.3] — never released

Shipped internally for validation but **never published to Maven Central**.
The fixes it contained (clean `Resource` instead of merging `Resource.getDefault()`,
isolated OpenTelemetry context without touching `GlobalOpenTelemetry`,
`isolateContext` config switch) are fully included in 0.0.4. Skip 0.0.3 and go
straight to 0.0.4.

## [0.0.2] — 2026-09-16

Initial public release on Maven Central.

### Added

- `ClsObservability.install()` — one-shot setup for OTLP/HTTP span upload to
  Tencent Cloud CLS, with configuration resolved from builder args / env
  vars / `.env` file.
- `ClsTracingMiddleware` — AgentScope `MiddlewareBase` implementation that
  emits the `entry → agent → step → chat / tool` span tree per turn.
- Rich attribute capture: session / user identity, tool arguments and
  results, reasoning thoughts, streamed output text, token usage (including
  cache read / write).
- Human-in-the-loop (HITL) awareness: `awaiting_user_confirm`,
  `external_execution`, `denied`, `max_iters`, `interrupted`, `error` finish
  reasons; `gen_ai.incomplete` flag on cancelled spans.
- CLS GenAI Trace spec compliance (`gen_ai.*` dotted attribute keys).
- Examples: `LoggingTracingExample`, `RealAgentScopeExample`,
  `RealUploadExample`.

## [0.0.1] — 2026-08

Initial internal release.

[0.0.4]: https://github.com/TencentCloud/tencentcloud-agentobs-sdk-agentscope-java/releases/tag/v0.0.4
[0.0.2]: https://github.com/TencentCloud/tencentcloud-agentobs-sdk-agentscope-java/releases/tag/v0.0.2
[0.0.1]: https://github.com/TencentCloud/tencentcloud-agentobs-sdk-agentscope-java/releases/tag/v0.0.1
