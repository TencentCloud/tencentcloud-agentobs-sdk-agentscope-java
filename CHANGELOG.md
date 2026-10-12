# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
