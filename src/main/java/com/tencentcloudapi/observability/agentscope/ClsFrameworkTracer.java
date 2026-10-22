package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.agent.AgentBase;
import io.agentscope.core.formatter.AbstractBaseFormatter;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ThinkingBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.tool.ToolCallParam;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.core.tracing.TracerRegistry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

/**
 * Framework-level model tracer that plugs into AgentScope's {@link TracerRegistry}.
 *
 * <p><b>Why this exists.</b> {@link ClsTracingMiddleware} instruments the middleware chain, so it
 * only sees model calls routed through {@code onModelCall()}. The gap this closes, as it shows up
 * in practice, is <b>{@code agentscope-harness}</b>: its {@code MemoryFlushMiddleware} implements
 * only {@code onAgent} and, once the turn completes, hands
 * {@code MemoryFlushManager.flushMemories(...)} to a Scheduler. That call issues harness's own
 * {@code DEFAULT_FLUSH_PROMPT} ("You are a memory extraction assistant…") straight through
 * {@code Model.stream()} and never reaches {@code onModelCall()}, so without this hook it produces
 * no span at all.
 *
 * <p>{@code AutoContextMemory} (from {@code agentscope-extensions-autocontext-memory}) has the same
 * shape — a synchronous {@code callModelForSummary} with four direct {@code Model.stream()} call
 * sites and zero {@code MiddlewareChain} references, ending in {@code Mono.block()}. It is only
 * published for AgentScope 1.x though: it implements {@code io.agentscope.core.state.StateModule}
 * and references {@code core.session.Session}, neither of which exists in core 2.0.x, so a 2.x
 * application cannot load it. Both shapes are covered because the hook sits below either.
 *
 * <p>Note that {@code agentscope-core} itself does NOT have this problem: verified on 2.0.1, every
 * {@code ReActAgent} model call, including the out-of-iterations summarisation from
 * {@code summarizing()}, flows through {@code MiddlewareChain} → {@code onModelCall}.
 *
 * <p><b>How.</b> {@code ChatModelBase.stream()} is {@code final} and its very first instruction is
 * {@code TracerRegistry.get()}, funnelling every model call through
 * {@code TracerRegistry.get().callModel(...)}. Registering here therefore captures <em>all</em>
 * model traffic at the lowest possible entry point, whichever layer above initiated it.
 *
 * <p><b>No double counting.</b> Regular agent model calls pass through BOTH the middleware and
 * this tracer. {@link ClsTracingMiddleware} publishes {@link ClsTracingMiddleware#MODEL_CALL_MARK_KEY}
 * on the Reactor Context around its {@code chat} span; since a Reactor Context propagates from
 * downstream to upstream at subscription time, the model Flux created underneath the middleware
 * sees that marker and this tracer stays silent. Only unmarked calls get a span here, tagged
 * {@code gen_ai.model.call.origin=framework}.
 *
 * <p><b>Turn correlation.</b> The framework runs its memory work after the agent has replied, from
 * a callback, on another thread — harness schedules it as a detached subscription
 * ({@code Mono.defer(..).subscribeOn(..).subscribe(cb, cb)} with nothing written to the context),
 * and {@code AutoContextMemory} blocks inside one. Neither shape can be correlated through the
 * Reactor Context alone, so {@link ClsTracingMiddleware#resolveExternalCallSite} tries five
 * channels in order: Reactor Context → ThreadLocal (bridged by {@link #runWithContext}, covering
 * the {@code block()} case) → the Scheduler-hop snapshot installed by {@link #installSchedulerHook}
 * (covering the detached case) → OTel {@code Context} (covering a hop the host propagated) → the
 * per-session index of the most recent turn. Only when the submitting thread carries no turn either
 * and several sessions are in flight is attribution declined, leaving the span on its own trace
 * rather than risking cross-tenant misattribution. See {@code ClsHarnessMemoryFlushTest},
 * {@code ClsDetachedSchedulerCorrelationTest} and {@code ClsAsyncMemoryCorrelationTest}.
 *
 * <p><b>Chaining.</b> Any tracer already registered (another vendor's probe, a user tracer) is kept
 * as a delegate and still receives every callback, so installing CLS observability never disables
 * an existing integration.
 *
 * <p>Telemetry failures are swallowed: every path falls back to the untraced business Flux.
 *
 * <p><b>Compatibility.</b> AgentScope marks {@code TracerRegistry}/{@code Tracer} deprecated for
 * removal, yet {@code ChatModelBase.stream()} still routes through them in 2.0.x, so this remains
 * the only hook that sees model calls made off the middleware chain. Installation is wrapped in a
 * {@code Throwable} catch by {@link ClsObservability}, so should a future AgentScope drop these
 * types the SDK degrades to middleware-only tracing instead of failing to start.
 */
@SuppressWarnings("removal")
final class ClsFrameworkTracer implements io.agentscope.core.tracing.Tracer {

    private final Tracer otel;
    private final io.agentscope.core.tracing.Tracer delegate;
    /** See {@link #callFormat}: off by default because a formatter runs on every model call. */
    private final boolean captureFormatCalls;

    private ClsFrameworkTracer(Tracer otel, io.agentscope.core.tracing.Tracer delegate,
                               boolean captureFormatCalls) {
        this.otel = otel;
        this.delegate = delegate;
        this.captureFormatCalls = captureFormatCalls;
    }

    /**
     * Register this tracer with AgentScope, chaining onto whatever tracer is already installed.
     *
     * @param propagateAcrossSchedulers also install the {@code Schedulers} hook that carries turn
     *     context across a thread hop; see {@link #installSchedulerHook()}.
     * @param captureFormatCalls also emit a {@code format} span per prompt-formatting call; see
     *     {@link #callFormat}.
     * @return {@code true} when the hook is active (or was already installed by us).
     */
    static boolean install(Tracer otel, boolean propagateAcrossSchedulers,
                           boolean captureFormatCalls) {
        if (propagateAcrossSchedulers) {
            installSchedulerHook();
        }
        io.agentscope.core.tracing.Tracer previous = TracerRegistry.get();
        if (previous instanceof ClsFrameworkTracer) {
            return true;
        }
        TracerRegistry.register(new ClsFrameworkTracer(otel, previous, captureFormatCalls));
        // register() enables Hooks.onEachOperator, which wraps every subscriber so
        // runWithContext is called on each signal. We NEED this: it bridges the Reactor Context
        // (carrying TurnState) to ThreadLocals, so a Mono.block() caller inside signal
        // processing can find the owning turn.
        return true;
    }

    static boolean install(Tracer otel, boolean propagateAcrossSchedulers) {
        return install(otel, propagateAcrossSchedulers, false);
    }

    /** Install with Scheduler propagation enabled; used by tests and as the default. */
    static boolean install(Tracer otel) {
        return install(otel, true, false);
    }

    /** Key identifying our {@code Schedulers} hook; replacing by key makes installation idempotent. */
    private static final String SCHEDULE_HOOK_KEY =
            ClsFrameworkTracer.class.getName() + ".turnContext";

    /**
     * Carry the turn context across a Scheduler hop.
     *
     * <p>{@link #runWithContext} only bridges the Reactor Context of chains we are part of. A
     * component that hands post-turn work to a Scheduler as a <em>detached</em> subscription —
     * {@code agentscope-harness}'s {@code MemoryFlushMiddleware} does exactly
     * {@code Mono.defer(..).subscribeOn(boundedElastic()).subscribe(cb, cb)} from a
     * {@code doOnComplete}, writing nothing to the context — produces a model call on a worker
     * thread with no Reactor Context, no ThreadLocal and no OTel Context. Such a span could only
     * be attributed when a single session happened to be in flight, so on a multi-user service
     * (the normal case) the framework's memory-extraction calls became orphan traces.
     *
     * <p>{@code Schedulers.onScheduleHook} is Reactor's supported seam for precisely this: it
     * decorates every task submitted to any Scheduler. (It is also what OpenTelemetry's own
     * {@code reactor-3.1} instrumentation uses, via {@code ContextPropagationOperator}, so this is
     * the same mechanism an agent-based probe relies on.) We snapshot the turn on the submitting
     * thread — where it is still in scope, because the submission happens inside signal
     * processing — and restore it around the task on the worker thread. The worker is left exactly
     * as it was found, so pooled threads never leak state between tasks.
     *
     * <p>The snapshot holds <b>values only</b> (ids plus a {@code SpanContext}), never the live
     * turn, so a long-delayed task cannot keep a turn's spans — and their prompt attributes —
     * reachable. See {@link ClsTracingMiddleware.TurnSnapshot}.
     *
     * <p>Tasks submitted outside a turn capture nothing and are returned unwrapped.
     */
    private static void installSchedulerHook() {
        try {
            reactor.core.scheduler.Schedulers.onScheduleHook(SCHEDULE_HOOK_KEY, task -> {
                ClsTracingMiddleware.TurnSnapshot snapshot;
                try {
                    snapshot = ClsTracingMiddleware.captureTurnContextForHop();
                } catch (Throwable t) {
                    ClsTracingMiddleware.telemetryFailed("scheduleHook.capture", t);
                    return task;
                }
                if (snapshot == null) {
                    return task;
                }
                return () -> {
                    // Holder, because a captured local cannot be reassigned inside a lambda.
                    final Object[][] previous = new Object[1][];
                    try {
                        previous[0] = ClsTracingMiddleware.installTurnContext(snapshot);
                    } catch (Throwable t) {
                        ClsTracingMiddleware.telemetryFailed("scheduleHook.install", t);
                    }
                    try {
                        task.run();
                    } finally {
                        if (previous[0] != null) {
                            ClsTracingMiddleware.safely("scheduleHook.uninstall",
                                    () -> ClsTracingMiddleware.uninstallTurnContext(previous[0]));
                        }
                    }
                };
            });
        } catch (Throwable t) {
            // Telemetry must never break startup; without the hook we simply lose correlation
            // for detached post-turn model calls.
            ClsTracingMiddleware.telemetryFailed("scheduleHook.install", t);
        }
    }

    // ══════════════ traced hook ══════════════

    @Override
    public Flux<ChatResponse> callModel(
            ChatModelBase model,
            List<Msg> messages,
            List<ToolSchema> tools,
            GenerateOptions options,
            Supplier<Flux<ChatResponse>> supplier) {

        Supplier<Flux<ChatResponse>> downstream =
                () -> delegate.callModel(model, messages, tools, options, supplier);
        // Use transformDeferredContextual on the downstream Flux: this evaluates at subscribe-
        // time WITH the Reactor Context that contextWrite (in onModelCall) has published.
        // TracerRegistry's Hooks.onEachOperator does not interfere with this pattern.
        Flux<ChatResponse> inner = downstream.get();
        return inner.transformDeferredContextual((flux, cv) -> {
            try {
                if (ClsTracingMiddleware.isModelCallInstrumented(cv)) {
                    return flux;
                }
                return traceModelCall(cv, model, messages, tools, options, () -> flux);
            } catch (Throwable t) {
                ClsTracingMiddleware.telemetryFailed("frameworkTracer.callModel", t);
                return flux;
            }
        });
    }

    /** Wrap a model call that no middleware observed into a {@code chat} span. */
    private Flux<ChatResponse> traceModelCall(
            reactor.util.context.ContextView cv,
            ChatModelBase model,
            List<Msg> messages,
            List<ToolSchema> tools,
            GenerateOptions options,
            Supplier<Flux<ChatResponse>> downstream) {

        // Resolve the owning turn across all five channels: Reactor Context (reactive chain
        // inside onAgent), ThreadLocal (Mono.block() inside signal processing), the Scheduler-hop
        // snapshot (harness's detached memory flush), OTel Context, then the session index.
        ClsTracingMiddleware.ExternalCallSite site =
                ClsTracingMiddleware.resolveExternalCallSite(cv, otel);
        String modelName = (model != null) ? ClsTracingMiddleware.safe(model.getModelName()) : "";

        Span chat = otel.spanBuilder("chat " + modelName)
                .setParent(site.parent)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        chat.setAttribute("gen_ai.span.kind", "chat");
        chat.setAttribute("gen_ai.operation.name", "chat");
        // Marks a model call the agent's middleware chain never saw — e.g. the framework's own
        // memory flush / history summarisation prompt.
        chat.setAttribute("gen_ai.model.call.origin", "framework");
        if (ClsTracingMiddleware.notEmpty(site.sessionId)) {
            chat.setAttribute("gen_ai.session.id", site.sessionId);
        }
        if (ClsTracingMiddleware.notEmpty(site.turnId)) {
            chat.setAttribute("gen_ai.turn.id", site.turnId);
        }
        if (ClsTracingMiddleware.notEmpty(site.stepId)) {
            chat.setAttribute("gen_ai.step.id", site.stepId);
        }
        if (ClsTracingMiddleware.notEmpty(modelName)) {
            chat.setAttribute("gen_ai.request.model", modelName);
        }
        // Same request-side attributes as the middleware path, from the one shared helper.
        ClsTracingMiddleware.applyModelRequest(chat, messages, tools, options);
        String provider = ClsTracingMiddleware.providerName(model);
        if (ClsTracingMiddleware.notEmpty(provider)) {
            chat.setAttribute("gen_ai.provider.name", provider);
        }

        ResponseAccumulator acc = new ResponseAccumulator();
        Context chatCtx = site.parent.with(chat);
        ClsTracingMiddleware.debugSpan("chat(framework)", chat, site.parent);

        // Spans MUST be ended BEFORE the terminal signal reaches the subscriber, not after.
        // doFinally() runs its callback *after* propagating onComplete/onError downstream, so a
        // blocking caller (any Mono.block(), including the framework's synchronous memory work) is
        // released while this callback is still pending on the emitting thread. An application that
        // flushes and shuts the provider down right after that call then loses the span: it is
        // ended after the exporter is closed. doOnTerminate runs before the signal is propagated,
        // which closes that race; doOnCancel covers the only case doOnTerminate misses.
        AtomicBoolean closed = new AtomicBoolean();
        Runnable endSpans = () -> ClsTracingMiddleware.safely("frameworkTracer.end", () -> {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            acc.flushTo(chat);
            chat.end();
            // The synthesised step exists only to hold this chat span.
            if (site.syntheticStep != null) {
                site.syntheticStep.end();
            }
        });

        return downstream.get()
                .doOnNext(resp -> ClsTracingMiddleware.safely(
                        "frameworkTracer.response", () -> acc.feed(resp)))
                .doOnError(err -> ClsTracingMiddleware.safely("frameworkTracer.error", () -> {
                    chat.recordException(err);
                    chat.setStatus(StatusCode.ERROR, ClsTracingMiddleware.safe(err.getMessage()));
                    if (site.syntheticStep != null) {
                        site.syntheticStep.setStatus(StatusCode.ERROR,
                                ClsTracingMiddleware.safe(err.getMessage()));
                    }
                }))
                .doOnTerminate(endSpans)
                .doOnCancel(() -> {
                    ClsTracingMiddleware.safely("frameworkTracer.cancel",
                            () -> chat.setAttribute("gen_ai.incomplete", true));
                    endSpans.run();
                })
                .contextWrite(reactorCtx ->
                        ClsTracingMiddleware.storeOtelContext(reactorCtx, chatCtx));
    }

    /**
     * Collects the streamed {@link ChatResponse} chunks of one model call.
     *
     * <p>AgentScope model implementations differ: some emit incremental deltas, others re-emit the
     * full text accumulated so far. {@link #merge} detects the latter (new value starts with what
     * we already have) and replaces instead of appending, so both styles yield the same text.
     */
    private static final class ResponseAccumulator {
        private final AtomicReference<String> text = new AtomicReference<>("");
        private final AtomicReference<String> thinking = new AtomicReference<>("");
        private final AtomicReference<ChatUsage> usage = new AtomicReference<>();
        private final AtomicReference<String> finishReason = new AtomicReference<>("");
        /** The provider's own id for this completion — the only way to reconcile a span with the
         * model vendor's billing / audit records. */
        private final AtomicReference<String> responseId = new AtomicReference<>("");
        private final AtomicInteger toolCalls = new AtomicInteger();
        private final long startNanos = System.nanoTime();
        private final AtomicLong ttftMs = new AtomicLong(-1);

        void feed(ChatResponse resp) {
            if (resp == null) {
                return;
            }
            if (resp.getUsage() != null) {
                usage.set(resp.getUsage());
            }
            if (ClsTracingMiddleware.notEmpty(resp.getFinishReason())) {
                finishReason.set(resp.getFinishReason());
            }
            if (ClsTracingMiddleware.notEmpty(resp.getId())) {
                responseId.set(resp.getId());
            }
            // TTFT: first ChatResponse with any content marks the first token.
            if (ttftMs.get() < 0 && resp.getContent() != null && !resp.getContent().isEmpty()) {
                ttftMs.compareAndSet(-1, (System.nanoTime() - startNanos) / 1_000_000L);
            }
            List<ContentBlock> blocks = resp.getContent();
            if (blocks == null) {
                return;
            }
            int tools = 0;
            StringBuilder chunkText = new StringBuilder();
            StringBuilder chunkThinking = new StringBuilder();
            for (ContentBlock b : blocks) {
                if (b instanceof TextBlock t) {
                    chunkText.append(ClsTracingMiddleware.safe(t.getText()));
                } else if (b instanceof ThinkingBlock t) {
                    chunkThinking.append(ClsTracingMiddleware.safe(t.getThinking()));
                } else if (b instanceof ToolUseBlock) {
                    tools++;
                } else if (b instanceof ToolResultBlock t) {
                    chunkText.append(toolResultText(t));
                }
            }
            if (chunkText.length() > 0) {
                text.updateAndGet(prev -> merge(prev, chunkText.toString()));
            }
            if (chunkThinking.length() > 0) {
                thinking.updateAndGet(prev -> merge(prev, chunkThinking.toString()));
            }
            if (tools > 0) {
                toolCalls.set(Math.max(toolCalls.get(), tools));
            }
        }

        private static String merge(String accumulated, String chunk) {
            if (accumulated.isEmpty()) {
                return chunk;
            }
            // Cumulative stream: the chunk already contains everything seen so far.
            return chunk.startsWith(accumulated) ? chunk : accumulated + chunk;
        }

        private static String toolResultText(ToolResultBlock r) {
            if (r == null || r.getOutput() == null) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            for (ContentBlock b : r.getOutput()) {
                if (b instanceof TextBlock t) {
                    sb.append(ClsTracingMiddleware.safe(t.getText()));
                }
            }
            return sb.toString();
        }

        void flushTo(Span span) {
            ChatUsage u = usage.get();
            if (u != null) {
                span.setAttribute("gen_ai.usage.input_tokens", u.getInputTokens());
                span.setAttribute("gen_ai.usage.output_tokens", u.getOutputTokens());
                span.setAttribute("gen_ai.usage.total_tokens", u.getTotalTokens());
                span.setAttribute("gen_ai.usage.cache_read.input_tokens", u.getCachedTokens());
            }
            int tools = toolCalls.get();
            if (tools > 0) {
                span.setAttribute("gen_ai.response.tool_call_count", (long) tools);
            }
            String finish = finishReason.get();
            if (!ClsTracingMiddleware.notEmpty(finish)) {
                finish = tools > 0 ? "tool_calls" : "stop";
            }
            span.setAttribute("gen_ai.response.finish_reasons", "[\"" + finish + "\"]");
            span.setAttribute("gen_ai.output.messages",
                    ClsTracingMiddleware.assistantPartsToJson(thinking.get(), text.get()));
            long t = ttftMs.get();
            if (t >= 0) {
                span.setAttribute("gen_ai.response.time_to_first_token_ms", t);
            }
            String rid = responseId.get();
            if (ClsTracingMiddleware.notEmpty(rid)) {
                span.setAttribute("gen_ai.response.id", rid);
            }
            // Mean decode latency per output token. Reported in ms to match
            // gen_ai.response.time_to_first_token_ms; together the two separate a slow provider
            // queue (high TTFT, normal per-token) from slow generation (normal TTFT, high
            // per-token), which total duration alone cannot distinguish.
            if (u != null && u.getOutputTokens() > 0) {
                long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
                span.setAttribute("gen_ai.response.time_per_output_token_ms",
                        (double) elapsedMs / u.getOutputTokens());
            }
        }
    }

    // ══════════════ pass-through hooks ══════════════
    //
    // Agent and tool calls are already covered by ClsTracingMiddleware's span tree; instrumenting
    // them here too would duplicate spans. They are forwarded untouched so a previously
    // registered tracer keeps working.

    @Override
    public Mono<Msg> callAgent(AgentBase agent, List<Msg> msgs, Supplier<Mono<Msg>> supplier) {
        return delegate.callAgent(agent, msgs, supplier);
    }

    @Override
    public Mono<ToolResultBlock> callTool(
            Toolkit toolkit, ToolCallParam param, Supplier<Mono<ToolResultBlock>> supplier) {
        return delegate.callTool(toolkit, param, supplier);
    }

    /**
     * Trace prompt formatting — the step that turns the agent's {@code Msg} list into the
     * provider's own request shape. {@code MiddlewareBase} has no formatter hook at all (it only
     * exposes {@code onAgent} / {@code onReasoning} / {@code onActing} / {@code onModelCall} /
     * {@code onSystemPrompt}), so this is the only place it can be observed.
     *
     * <p><b>It is a sibling of the {@code chat} span, not its child.</b> A formatter runs inside
     * {@code ChatModelBase.stream()}'s <em>assembly</em>: {@code OpenAIChatModel.doStream0} calls
     * {@code formatter.format(...)} in its own method body, which happens while the model Flux is
     * being built — before {@link #traceModelCall} subscribes and opens the {@code chat} span.
     * Both therefore hang off the same round's {@code step}, in call order. (The reference
     * java-agent reaches the same shape: its model instrumentation never makes the chat span
     * current, so its {@code format} span is not nested under it either.)
     *
     * <p>Off by default, see {@link ClsConfig#ENV_CAPTURE_FORMAT_CALLS}: a formatter runs on every
     * model call, so this roughly doubles the span count of a turn, and the information is a
     * re-encoding of {@code gen_ai.input.messages} — valuable when debugging a provider-specific
     * prompt-assembly problem, redundant otherwise.
     */
    @Override
    public <TReq, TResp, TParams> List<TReq> callFormat(
            AbstractBaseFormatter<TReq, TResp, TParams> formatter,
            List<Msg> msgs,
            Supplier<List<TReq>> supplier) {

        if (!captureFormatCalls) {
            return delegate.callFormat(formatter, msgs, supplier);
        }
        ClsTracingMiddleware.FormatSite site;
        try {
            // Only traced while a round is in flight. A formatter call we cannot attribute is
            // dropped rather than orphaned — see currentFormatStep for why guessing is worse here
            // than for a model call.
            site = ClsTracingMiddleware.currentFormatStep();
        } catch (Throwable t) {
            ClsTracingMiddleware.telemetryFailed("frameworkTracer.formatSite", t);
            return delegate.callFormat(formatter, msgs, supplier);
        }
        if (site == null) {
            return delegate.callFormat(formatter, msgs, supplier);
        }

        Span span;
        try {
            span = startFormatSpan(formatter, msgs, site);
        } catch (Throwable t) {
            ClsTracingMiddleware.telemetryFailed("frameworkTracer.formatSpan", t);
            return delegate.callFormat(formatter, msgs, supplier);
        }

        // callFormat is synchronous — it returns List<TReq>, not a publisher — so there is no
        // Reactor signal ordering to worry about here: the span closes inside this call.
        try {
            List<TReq> formatted = delegate.callFormat(formatter, msgs, supplier);
            ClsTracingMiddleware.safely("frameworkTracer.formatEnd", () -> {
                if (formatted != null) {
                    span.setAttribute("agentscope.format.count", (long) formatted.size());
                }
            });
            return formatted;
        } catch (Throwable err) {
            ClsTracingMiddleware.safely("frameworkTracer.formatError", () -> {
                span.recordException(err);
                span.setStatus(StatusCode.ERROR, ClsTracingMiddleware.safe(err.getMessage()));
            });
            throw err;
        } finally {
            ClsTracingMiddleware.safely("frameworkTracer.formatClose", span::end);
        }
    }

    private Span startFormatSpan(
            AbstractBaseFormatter<?, ?, ?> formatter,
            List<Msg> msgs,
            ClsTracingMiddleware.FormatSite site) {

        String name = (formatter != null) ? formatter.getClass().getSimpleName() : "formatter";
        Span span = otel.spanBuilder("format " + name)
                .setParent(site.parent)
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        span.setAttribute("gen_ai.span.kind", "format");
        span.setAttribute("gen_ai.operation.name", "format");
        span.setAttribute("agentscope.format.formatter", name);
        if (ClsTracingMiddleware.notEmpty(site.sessionId)) {
            span.setAttribute("gen_ai.session.id", site.sessionId);
        }
        if (ClsTracingMiddleware.notEmpty(site.turnId)) {
            span.setAttribute("gen_ai.turn.id", site.turnId);
        }
        if (ClsTracingMiddleware.notEmpty(site.stepId)) {
            span.setAttribute("gen_ai.step.id", site.stepId);
        }
        if (site.round > 0) {
            span.setAttribute("gen_ai.react.round", site.round);
        }
        if (msgs != null) {
            // Input size only. The messages themselves are already on the sibling chat span as
            // gen_ai.input.messages; repeating them here would double the trace's payload for a
            // span that runs on every single model call.
            span.setAttribute("agentscope.format.input_count", (long) msgs.size());
        }
        ClsTracingMiddleware.debugSpan("format", span, site.parent);
        return span;
    }

    /**
     * Bridge Reactor Context → ThreadLocal on every Reactor signal.
     *
     * <p>{@code TracerRegistry}'s {@code Hooks.onEachOperator} wraps every subscriber so this
     * method is called around every {@code onNext}/{@code onError}/{@code onComplete}. We store
     * the turn state into ThreadLocals BEFORE the signal is delivered. When synchronous code
     * inside a signal callback calls {@code Mono.block()} (like {@code callModelForSummary}),
     * the fresh subscription has no Reactor Context, but the ThreadLocals are still set — so
     * {@link ClsTracingMiddleware#resolveExternalCallSite} finds the owning turn.
     */
    @Override
    public <TResp> TResp runWithContext(ContextView contextView, Supplier<TResp> supplier) {
        Object[] prev = ClsTracingMiddleware.bridgeThreadContext(contextView);
        try {
            return delegate.runWithContext(contextView, supplier);
        } finally {
            ClsTracingMiddleware.restoreThreadContext(prev);
        }
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }
}
