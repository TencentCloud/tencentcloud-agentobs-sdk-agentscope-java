package com.tencentcloudapi.observability.agentscope;

import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.AllToolsDeniedEvent;
import io.agentscope.core.event.ExceedMaxItersEvent;
import io.agentscope.core.event.ExternalExecutionResultEvent;
import io.agentscope.core.event.ModelCallEndEvent;
import io.agentscope.core.event.RequireExternalExecutionEvent;
import io.agentscope.core.event.RequireUserConfirmEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ToolCallStartEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ThinkingBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.middleware.ActingInput;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.middleware.MiddlewareBase;
import io.agentscope.core.middleware.ModelCallInput;
import io.agentscope.core.middleware.ReasoningInput;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.tool.Toolkit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.reactor.v3_1.ContextPropagationOperator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

/**
 * AgentScope middleware that turns each agent turn into a span tree aligned with the
 * <b>CLS GenAI Trace</b> spec (per-turn trace model): {@code entry → agent → step → chat / tool}.
 *
 * <p><b>Stateless design (HITL-safe).</b> This middleware holds no per-turn mutable fields;
 * a single instance is safely shared across concurrent turns and across Human-in-the-loop
 * (HITL) suspend/resume boundaries. All turn/round state lives in per-invocation
 * {@link TurnState} / {@link StepState} objects that are threaded through the <b>Reactor
 * Context</b>. Two turns (e.g. a turn that parks awaiting user confirmation and the later turn
 * that resumes it) each own an isolated state object and can never clobber one another.
 *
 * <p><b>Hierarchy mapping.</b> AgentScope exposes five hooks; four onion hooks map to the
 * spec's span kinds:
 * <ul>
 *   <li>{@link #onAgent} → one <b>entry</b> span (turn root, SERVER) wrapping one <b>agent</b>
 *       span (INTERNAL, {@code invoke_agent}). Also watches the event stream for HITL events.</li>
 *   <li>{@link #onReasoning} → one <b>step</b> span (INTERNAL, {@code react}) covering one ReAct
 *       round (input assembly → model call → streaming decode). The tool executed in the same
 *       round nests under it.</li>
 *   <li>{@link #onModelCall} → one <b>chat</b> span (CLIENT), parented at the current step.</li>
 *   <li>{@link #onActing} → one <b>tool</b> span (CLIENT, {@code execute_tool}) parented at the
 *       current step.</li>
 * </ul>
 * {@link #onSystemPrompt} is a transformer hook (prompt rewriting), not a tracing hook, so it is
 * a pass-through here.
 *
 * <p><b>ID naming chain.</b> Per spec: {@code session.id → turn.id ({sessionId}:t{N}) →
 * step.id ({turnId}:s{N})}. AgentScope's {@code RuntimeContext} has no turn id, so it is
 * synthesised here; {@code session.id} falls back to a random id when the host supplies none.
 *
 * <p><b>HITL awareness.</b> A parked/interrupted turn does not look like a normal completion:
 * <ul>
 *   <li>{@link RequireUserConfirmEvent} ⇒ {@code gen_ai.finish_reason=awaiting_user_confirm}</li>
 *   <li>{@link RequireExternalExecutionEvent} ⇒ {@code external_execution}</li>
 *   <li>{@link AllToolsDeniedEvent} ⇒ {@code denied}</li>
 *   <li>{@link ExceedMaxItersEvent} ⇒ {@code max_iters}</li>
 *   <li>Reactor {@link SignalType#CANCEL} ⇒ {@code interrupted}</li>
 *   <li>{@link ExternalExecutionResultEvent} on resume ⇒ synthesised external tool spans, since
 *       {@link #onActing} never sees externally-executed tools.</li>
 * </ul>
 *
 * <p><b>Streaming.</b> Reasoning / output text and tool-result bodies arrive as delta events; they
 * are accumulated per {@code replyId}/{@code toolCallId} on the owning state object and flushed
 * onto the span at the END event (with a defensive flush in {@code doFinally} for cancelled flows).
 */
public final class ClsTracingMiddleware implements MiddlewareBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Framework identifier reported as {@code gen_ai.agent.type}. */
    private static final String AGENT_TYPE = "agentscope";

    /** Reactor Context key under which the active {@link TurnState} is published. */
    private static final String TURN_STATE_KEY = ClsTracingMiddleware.class.getName() + ".turnState";
    /** Reactor Context key under which the active {@link StepState} is published. */
    private static final String STEP_STATE_KEY = ClsTracingMiddleware.class.getName() + ".stepState";
    /**
     * 自定义 Reactor Context key，用于直接传递 OTel Context，避免依赖
     * {@link ContextPropagationOperator} 的 store/get 机制（该机制在未初始化或
     * 与第三方框架混用时可能失效，导致 span 父子关系断裂）。
     */
    private static final String OTEL_CONTEXT_KEY = ClsTracingMiddleware.class.getName() + ".otelContext";
    /**
     * Marker published on the Reactor Context while {@link #onModelCall} owns a model call.
     *
     * <p>{@link ClsFrameworkTracer} hooks the framework's lowest-level model entry
     * ({@code ChatModelBase.stream()}), which ALSO carries the agent's regular model calls.
     * Because the Reactor Context propagates from downstream to upstream at subscription time,
     * the model Flux created inside {@code onModelCall}'s {@code next.apply(...)} sees this key
     * and the framework tracer stays silent — leaving exactly one {@code chat} span per call.
     * Model calls issued outside any middleware chain (memory flush / history summarisation,
     * which call {@code Model.stream()} directly) carry no marker and are traced by the tracer.
     */
    static final String MODEL_CALL_MARK_KEY = ClsTracingMiddleware.class.getName() + ".modelCall";

    private final Tracer tracer;

    /** Turn sequence, the only shared mutable state — a monotonic id source, safe to share. */
    private final AtomicInteger turnCounter = new AtomicInteger(0);

    // ── ThreadLocal bridge for Mono.block() callers ──
    //
    // TracerRegistry's Hooks.onEachOperator wraps every Reactor signal with
    //   tracer.runWithContext(contextView, () -> actual.onNext(t))
    // ClsFrameworkTracer.runWithContext stores the turn state + OTel context into
    // these ThreadLocals BEFORE the supplier runs. So when a component makes a
    // synchronous model call from inside signal processing — Mono.block() starts a
    // fresh subscription with no Reactor Context — the ThreadLocals are still set.
    // resolveExternalCallSite checks them as fallback. (A call dispatched onto a
    // Scheduler instead is covered by the snapshot in TurnSnapshot.)
    private static final ThreadLocal<TurnState> THREAD_TURN = new ThreadLocal<>();
    private static final ThreadLocal<StepState> THREAD_STEP = new ThreadLocal<>();

    /**
     * Turn identity carried INSIDE the OpenTelemetry Context.
     *
     * <p>{@link TurnState} lives in the Reactor Context and in ThreadLocals, neither of which
     * survives a hop onto an unrelated thread. An OTel {@link Context} does: it is immutable and
     * propagates wherever the host (or an OTel context-propagating executor, or a java agent)
     * makes it current. Publishing {@code sessionId/turnId/stepId} here means a model call that
     * reaches us with only {@code Context.current()} available — a detached memory flush, for
     * instance — can still be attributed to its turn rather than becoming an orphan trace.
     */
    private static final io.opentelemetry.context.ContextKey<String[]> OTEL_TURN_IDS_KEY =
            io.opentelemetry.context.ContextKey.named("cls.gen_ai.turn_ids");

    /** Pack turn identity into an OTel context so it survives arbitrary thread hops. */
    private static Context withTurnIds(Context ctx, String sessionId, String turnId,
                                       String stepId) {
        return ctx.with(OTEL_TURN_IDS_KEY,
                new String[]{safe(sessionId), safe(turnId), safe(stepId)});
    }

    /**
     * Last turn seen per session, kept alive AFTER the turn's Flux terminates.
     *
     * <p>Context propagation (Reactor Context / ThreadLocal / OTel Context) only works while the
     * turn is still on the stack. But the framework's memory extraction routinely runs <em>after
     * the agent has replied</em> — it summarises the conversation that just finished, often on its
     * own thread. At that point every propagation channel is already gone, which is why such spans
     * used to become orphan traces.
     *
     * <p>This index is the fallback that fixes it: the owning {@code session.id} is almost always
     * still derivable (the memory component runs inside the same session's processing), so a model
     * call with no live context is attached to that session's most recent turn. This is how a
     * post-turn memory span lands under {@code invoke_agent} instead of starting a new trace.
     *
     * <p>Bounded and self-evicting — see {@link #rememberTurn}. Entries also expire by age so a
     * long-lived process cannot accumulate stale turns, and a model call arriving long after its
     * turn is treated as unrelated rather than being misattributed.
     */
    private static final Map<String, TurnState> LAST_TURN_BY_SESSION = new ConcurrentHashMap<>();

    /** Upper bound on {@link #LAST_TURN_BY_SESSION}; oldest entries are dropped past this. */
    private static final int MAX_TRACKED_SESSIONS = 1024;

    /**
     * How long a finished turn stays attachable. Memory extraction fires seconds after the reply;
     * beyond this window a model call is almost certainly unrelated and gets its own trace.
     */
    private static final long TURN_ATTACH_TTL_NANOS = java.time.Duration.ofMinutes(2).toNanos();

    /** Record a turn as the session's most recent, evicting stale//excess entries. */
    private static void rememberTurn(TurnState ts) {
        if (!notEmpty(ts.sessionId)) {
            return;
        }
        LAST_TURN_BY_SESSION.put(ts.sessionId, ts);
        if (LAST_TURN_BY_SESSION.size() > MAX_TRACKED_SESSIONS) {
            long now = System.nanoTime();
            LAST_TURN_BY_SESSION.entrySet().removeIf(
                    e -> now - e.getValue().createdNanos > TURN_ATTACH_TTL_NANOS);
            // Still oversized (many concurrent fresh sessions): drop the oldest half.
            if (LAST_TURN_BY_SESSION.size() > MAX_TRACKED_SESSIONS) {
                LAST_TURN_BY_SESSION.entrySet().stream()
                        .sorted(java.util.Comparator.comparingLong(e -> e.getValue().createdNanos))
                        .limit(LAST_TURN_BY_SESSION.size() - MAX_TRACKED_SESSIONS / 2)
                        .map(Map.Entry::getKey)
                        .forEach(LAST_TURN_BY_SESSION::remove);
            }
        }
    }

    /** The session's most recent turn, if still within the attach window. */
    private static TurnState recentTurnForSession(String sessionId) {
        if (!notEmpty(sessionId)) {
            return null;
        }
        TurnState ts = LAST_TURN_BY_SESSION.get(sessionId);
        if (ts == null) {
            return null;
        }
        if (System.nanoTime() - ts.createdNanos > TURN_ATTACH_TTL_NANOS) {
            LAST_TURN_BY_SESSION.remove(sessionId, ts);
            return null;
        }
        return ts;
    }

    /**
     * The single turn in flight, or {@code null} when zero or several sessions are being tracked.
     *
     * <p>Used only when the thread gives no hint — e.g. memory extraction dispatched onto a pool
     * thread that never served a turn. With exactly one session active the attribution is
     * unambiguous, which covers single-user apps, CLI tools and the common single-tenant service.
     * With several concurrent sessions this deliberately returns {@code null} rather than guess,
     * so a busy multi-tenant server never cross-attributes one user's memory call to another.
     */
    private static TurnState soleTrackedTurn() {
        if (LAST_TURN_BY_SESSION.size() != 1) {
            return null;
        }
        java.util.Iterator<TurnState> it = LAST_TURN_BY_SESSION.values().iterator();
        if (!it.hasNext()) {
            return null;
        }
        TurnState ts = it.next();
        return (System.nanoTime() - ts.createdNanos > TURN_ATTACH_TTL_NANOS) ? null : ts;
    }

    /**
     * Session id of the turn most recently active on this thread, surviving turn completion.
     *
     * <p>Memory extraction usually runs on the same thread that just finished the turn (or one it
     * spawned synchronously), so this is the key that lets a post-turn call find its session.
     */
    private static final ThreadLocal<String> THREAD_LAST_SESSION = new ThreadLocal<>();

    /**
     * Drop all post-turn attachment state. Only needed by tests, which share one JVM across
     * cases and would otherwise see one case's finished turn adopt the next case's memory call.
     */
    static void resetSessionIndexForTesting() {
        LAST_TURN_BY_SESSION.clear();
        THREAD_LAST_SESSION.remove();
        THREAD_TURN.remove();
        THREAD_STEP.remove();
        THREAD_SNAPSHOT.remove();
    }

    /**
     * The OTel Context of the turn currently in scope, or {@code null} when no turn is active.
     *
     * <p>Exposed so a host that dispatches memory/summarisation work onto its own executor can
     * propagate it explicitly and keep those spans on the turn's trace:
     * <pre>{@code
     * Context turnCtx = ClsTracingMiddleware.currentTurnOtelContext();
     * executor.submit(() -> {
     *     try (Scope ignored = turnCtx.makeCurrent()) {
     *         memory.compressIfNeeded();
     *     }
     * });
     * }</pre>
     */
    public static Context currentTurnOtelContext() {
        StepState ss = THREAD_STEP.get();
        TurnState ts = THREAD_TURN.get();
        if (ss == null && ts != null) {
            ss = liveStep(ts);
        }
        if (ss != null) {
            return ss.stepCtx;
        }
        return ts != null ? ts.agentCtx : null;
    }

    public ClsTracingMiddleware(Tracer tracer) {
        this.tracer = tracer;
    }

    // ══════════════ per-invocation state ══════════════

    /** State of one {@link #onAgent} invocation (one turn). Never shared across turns. */
    private static final class TurnState {
        final String sessionId;
        final String turnId;
        final String userId;
        final Span entrySpan;
        final Span agentSpan;
        /** OTel context whose current span is the agent span; parent for step spans. */
        final Context agentCtx;

        final AtomicLong sumInputTokens = new AtomicLong();
        final AtomicLong sumOutputTokens = new AtomicLong();
        final AtomicLong sumTotalTokens = new AtomicLong();
        final AtomicLong sumCacheReadTokens = new AtomicLong();
        final AtomicInteger roundCounter = new AtomicInteger();
        final AtomicInteger toolCallCount = new AtomicInteger();
        /**
         * Sequence for steps synthesised around model calls that bypass the middleware chain.
         * Separate from {@link #roundCounter} — those calls are not ReAct rounds, and sharing the
         * counter would shift {@code gen_ai.react.round} for the real rounds.
         */
        final AtomicInteger frameworkStepCounter = new AtomicInteger();
        volatile String lastAssistantText = "";
        volatile String finishReason = "normal";

        /**
         * The step span of the round currently in flight, shared at turn scope so that
         * {@link #onActing} (which runs AFTER {@link #onReasoning}'s Flux completes and is NOT
         * on its reactive downstream) can still nest the tool span under the right ReAct round.
         * The step span is deliberately kept open across the reasoning→acting boundary and only
         * ended when the next round's step opens, or when the turn finishes.
         */
        final AtomicReference<StepState> currentStep = new AtomicReference<>();

        /** Creation timestamp, used to expire entries from {@link #LAST_TURN_BY_SESSION}. */
        final long createdNanos = System.nanoTime();

        /**
         * Precise {@code toolCallId → owning step} index. When a model call in a given round emits
         * a {@link ToolCallStartEvent}, that tool's id is bound here to the round's step. {@link
         * #onActing} then resolves the tool span's parent by id, so a round that issues MULTIPLE
         * tool calls (or interleaved/parallel execution) attributes each tool to the exact round
         * that requested it — instead of assuming "the most recent step" ({@link #currentStep}),
         * which only holds for the single-tool case. {@link #currentStep} remains the fallback.
         */
        final Map<String, StepState> toolCallStepIndex = new ConcurrentHashMap<>();

        TurnState(String sessionId, String turnId, String userId,
                  Span entrySpan, Span agentSpan, Context agentCtx) {
            this.sessionId = sessionId;
            this.turnId = turnId;
            this.userId = userId;
            this.entrySpan = entrySpan;
            this.agentSpan = agentSpan;
            this.agentCtx = agentCtx;
        }
    }

    /** State of one {@link #onReasoning} invocation (one ReAct round). Owns the step span. */
    private static final class StepState {
        final String stepId;
        final int round;
        final Span stepSpan;
        final Context stepCtx;
        /** streamed reasoning text, keyed by replyId */
        final Map<String, StringBuilder> thoughtBuffer = new ConcurrentHashMap<>();
        /** streamed output text, keyed by replyId */
        final Map<String, StringBuilder> outputBuffer = new ConcurrentHashMap<>();
        /** streamed tool result text, keyed by toolCallId */
        final Map<String, StringBuilder> toolResultBuffer = new ConcurrentHashMap<>();
        /**
         * Set once this round's {@code onReasoning} Flux has terminated.
         *
         * <p>The step span deliberately outlives that Flux — it stays open so the tool executed in
         * the same round can nest under it, and is closed only when the next round opens or the
         * turn ends. So "{@code currentStep} is non-null" does NOT mean a round is in flight: after
         * the last round, {@link TurnState#currentStep} still points at it for the whole
         * {@code doFinally}/{@code doOnComplete} tail of the turn. Work handed off there (the
         * framework's post-turn memory flush) must NOT be filed as part of that round, so this
         * flag is what distinguishes "still reasoning" from "kept open awaiting a tool".
         */
        final AtomicBoolean reasoningDone = new AtomicBoolean();

        StepState(String stepId, int round, Span stepSpan, Context stepCtx) {
            this.stepId = stepId;
            this.round = round;
            this.stepSpan = stepSpan;
            this.stepCtx = stepCtx;
        }

        /** True while this round's reasoning is still running, i.e. safe to adopt as parent. */
        boolean isLive() {
            return !reasoningDone.get();
        }
    }

    // ══════════════ entry + agent (turn level) ══════════════
    @Override
    public Flux<AgentEvent> onAgent(
            Agent agent,
            RuntimeContext ctx,
            AgentInput input,
            Function<AgentInput, Flux<AgentEvent>> next) {

        return Flux.deferContextual(contextView -> guarded(() -> {
            Context parent = getOtelContext(contextView);

            int turn = turnCounter.incrementAndGet();
            String sid = (ctx != null && notEmpty(ctx.getSessionId()))
                    ? ctx.getSessionId()
                    : "sess-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            String turnId = sid + ":t" + turn;
            String userId = ctx != null ? safe(ctx.getUserId()) : "";
            String agentName = agent != null ? safe(agent.getName()) : "main";
            String inputMsgsJson = (input != null) ? msgListToJson(input.msgs()) : "[]";

            // ── entry span (turn root, SERVER) ──
            Span entry = tracer.spanBuilder("enter_application")
                    .setParent(parent)
                    .setSpanKind(SpanKind.SERVER)
                    .startSpan();
            entry.setAttribute("gen_ai.span.kind", "entry");
            entry.setAttribute("gen_ai.operation.name", "enter_application");
            entry.setAttribute("gen_ai.session.id", sid);
            entry.setAttribute("gen_ai.turn.id", turnId);
            if (notEmpty(userId)) {
                entry.setAttribute("gen_ai.user.id", userId);
            }
            entry.setAttribute("gen_ai.input.messages", inputMsgsJson);
            Context entryCtx = parent.with(entry);

            // ── agent span (INTERNAL, invoke_agent) ──
            Span agentSpan = tracer.spanBuilder("invoke_agent " + agentName)
                    .setParent(entryCtx)
                    .setSpanKind(SpanKind.INTERNAL)
                    .startSpan();
            agentSpan.setAttribute("gen_ai.span.kind", "agent");
            agentSpan.setAttribute("gen_ai.operation.name", "invoke_agent");
            agentSpan.setAttribute("gen_ai.session.id", sid);
            agentSpan.setAttribute("gen_ai.turn.id", turnId);
            agentSpan.setAttribute("gen_ai.agent.type", AGENT_TYPE);
            agentSpan.setAttribute("gen_ai.agent.name", agentName);
            if (agent != null && notEmpty(agent.getAgentId())) {
                agentSpan.setAttribute("gen_ai.agent.id", safe(agent.getAgentId()));
            }
            if (agent != null) {
                String description = agentDescription(agent);
                if (notEmpty(description)) {
                    agentSpan.setAttribute("gen_ai.agent.description", description);
                }
            }
            if (notEmpty(userId)) {
                agentSpan.setAttribute("gen_ai.user.id", userId);
            }
            agentSpan.setAttribute("gen_ai.input.messages", inputMsgsJson);
            // Carry turn identity in the OTel Context too, so a model call that only has
            // Context.current() (detached memory flush) can still be attributed to this turn.
            Context agentCtx = withTurnIds(entryCtx.with(agentSpan), sid, turnId, "");

            TurnState ts = new TurnState(sid, turnId, userId, entry, agentSpan, agentCtx);
            // Index the turn by session so a model call arriving AFTER this turn finishes — the
            // framework's memory extraction summarises the conversation that just ended — can
            // still be attached to it instead of starting an orphan trace.
            rememberTurn(ts);
            THREAD_LAST_SESSION.set(sid);

            debugSpan("entry", entry, parent);
            debugSpan("agent", agentSpan, entryCtx);

            AtomicBoolean turnClosed = new AtomicBoolean();
            java.util.function.Consumer<Boolean> closeTurn = cancelled ->
                    safely("onAgent.end", () -> {
                        if (!turnClosed.compareAndSet(false, true)) {
                            return;
                        }
                        if (cancelled && "normal".equals(ts.finishReason)) {
                            ts.finishReason = "interrupted";
                        }
                        // Close the last round's step span, which is deliberately kept open past
                        // its own onReasoning Flux so the round's tool span could nest under it.
                        closeStep(ts, ts.currentStep.getAndSet(null), cancelled);
                        ts.toolCallStepIndex.clear();
                        boolean parked = "awaiting_user_confirm".equals(ts.finishReason)
                                || "external_execution".equals(ts.finishReason);

                        agentSpan.setAttribute("gen_ai.finish_reason", ts.finishReason);
                        agentSpan.setAttribute("gen_ai.turn.completed", !parked);
                        agentSpan.setAttribute("gen_ai.agent.message_count",
                                (long) ts.roundCounter.get());
                        agentSpan.setAttribute("gen_ai.agent.tool_call_count",
                                (long) ts.toolCallCount.get());
                        agentSpan.setAttribute("gen_ai.usage.input_tokens", ts.sumInputTokens.get());
                        agentSpan.setAttribute("gen_ai.usage.output_tokens", ts.sumOutputTokens.get());
                        agentSpan.setAttribute("gen_ai.usage.total_tokens", ts.sumTotalTokens.get());
                        agentSpan.setAttribute("gen_ai.usage.cache_read.input_tokens",
                                ts.sumCacheReadTokens.get());
                        String outJson = assistantTextToJson(ts.lastAssistantText);
                        agentSpan.setAttribute("gen_ai.output.messages", outJson);
                        agentSpan.end();

                        entry.setAttribute("gen_ai.finish_reason", ts.finishReason);
                        entry.setAttribute("gen_ai.turn.completed", !parked);
                        entry.setAttribute("gen_ai.output.messages", outJson);
                        entry.end();
                    });

            Flux<AgentEvent> flux = next.apply(input)
                    .doOnNext(ev -> safely("onAgent.event",
                            () -> observeAgentEvent(ts, agentCtx, ev)))
                    .doOnError(err -> safely("onAgent.error", () -> {
                        agentSpan.recordException(err);
                        agentSpan.setStatus(StatusCode.ERROR, safe(err.getMessage()));
                        entry.recordException(err);
                        entry.setStatus(StatusCode.ERROR, safe(err.getMessage()));
                        ts.finishReason = "error";
                    }))
                    // doOnTerminate, not doFinally: the turn's spans must be ended BEFORE
                    // agent.call(...).block() returns, otherwise an application that flushes and
                    // shuts down right after the call loses them. See endOnceBefore.
                    .doOnTerminate(() -> closeTurn.accept(false))
                    .doOnCancel(() -> closeTurn.accept(true))
                    .contextWrite(reactorCtx -> reactorCtx.put(TURN_STATE_KEY, ts))
                    .contextWrite(reactorCtx -> storeOtelContext(reactorCtx, agentCtx));
            return flux;
        }, () -> next.apply(input)));
    }

    /** Watch the turn's event stream for HITL signals and turn-level aggregates. */
    private void observeAgentEvent(TurnState ts, Context agentCtx, AgentEvent ev) {
        if (ev instanceof RequireUserConfirmEvent e) {
            ts.finishReason = "awaiting_user_confirm";
            ts.agentSpan.setAttribute("gen_ai.hitl.event", "require_user_confirm");
            ts.agentSpan.setAttribute("gen_ai.hitl.pending_tool_calls",
                    toolUseNames(e.getToolCalls()));
        } else if (ev instanceof RequireExternalExecutionEvent e) {
            ts.finishReason = "external_execution";
            ts.agentSpan.setAttribute("gen_ai.hitl.event", "require_external_execution");
            ts.agentSpan.setAttribute("gen_ai.hitl.pending_tool_calls",
                    toolUseNames(e.getToolCalls()));
        } else if (ev instanceof AllToolsDeniedEvent e) {
            ts.finishReason = "denied";
            ts.agentSpan.setAttribute("gen_ai.hitl.event", "all_tools_denied");
            ts.agentSpan.setAttribute("gen_ai.hitl.denied_tool_calls",
                    toolUseNames(e.getDeniedToolCalls()));
        } else if (ev instanceof ExceedMaxItersEvent e) {
            ts.finishReason = "max_iters";
            ts.agentSpan.setAttribute("gen_ai.react.max_iters", (long) e.getMaxIters());
            ts.agentSpan.setAttribute("gen_ai.react.current_iter", (long) e.getCurrentIter());
        } else if (ev instanceof ExternalExecutionResultEvent e) {
            // resume path: these tools ran outside the agent, onActing never saw them
            emitExternalToolSpans(ts, agentCtx, e);
        }
    }

    /** Synthesise CLIENT tool spans for tools that executed outside the agent (HITL resume). */
    private void emitExternalToolSpans(TurnState ts, Context parent, ExternalExecutionResultEvent e) {
        List<ToolResultBlock> results = e.getToolResults();
        if (results == null) {
            return;
        }
        for (ToolResultBlock r : results) {
            if (r == null) {
                continue;
            }
            Span span = tracer.spanBuilder("execute_tool " + safe(r.getName()))
                    .setParent(parent)
                    .setSpanKind(SpanKind.CLIENT)
                    .startSpan();
            span.setAttribute("gen_ai.span.kind", "tool");
            span.setAttribute("gen_ai.operation.name", "execute_tool");
            span.setAttribute("gen_ai.session.id", ts.sessionId);
            span.setAttribute("gen_ai.turn.id", ts.turnId);
            span.setAttribute("gen_ai.tool.name", safe(r.getName()));
            span.setAttribute("gen_ai.tool.type", "function");
            span.setAttribute("gen_ai.tool.call.id", safe(r.getId()));
            span.setAttribute("gen_ai.tool.execution", "external");
            String out = toolResultBlockText(r);
            if (notEmpty(out)) {
                span.setAttribute("gen_ai.tool.call.result", out);
            }
            ts.toolCallCount.incrementAndGet();
            span.end();
        }
    }

    /**
     * The agent's configured self-description — its role, which is what tells one agent apart from
     * another in a multi-agent trace where names are generic.
     *
     * <p><b>Returns {@code ""} when the agent has none.</b> {@code Agent.getDescription()} is a
     * {@code default} method that, when unset, synthesises {@code "Agent({agentId}) {name}"} — so
     * it never returns null and a naive copy writes a value for every agent. That value is pure
     * noise: it restates {@code gen_ai.agent.id} and {@code gen_ai.agent.name}, which are already
     * on the same span, while making a trace look as though the field were populated. Verified
     * against a real {@code ReActAgent} with no description configured.
     *
     * @return the configured description, or {@code ""} if absent or merely the synthesised default
     */
    private static String agentDescription(Agent agent) {
        String description = safely("onAgent.description", agent::getDescription, "");
        if (!notEmpty(description)) {
            return "";
        }
        String synthesised = "Agent(" + safe(safely("onAgent.agentId", agent::getAgentId, ""))
                + ") " + safe(safely("onAgent.name", agent::getName, ""));
        return description.equals(synthesised) ? "" : description;
    }

    /**
     * The invoked tool's declared description, looked up on the agent's toolkit.
     *
     * <p>It answers a question the call record alone cannot: whether the model picked the wrong
     * tool, or the right tool whose description misled it. Resolved by name because
     * {@link ActingInput} carries only {@link ToolUseBlock}s — the schemas live on the toolkit.
     *
     * @return the description, or {@code ""} when unavailable
     */
    private static String toolDescription(Agent agent, String toolName) {
        if (agent == null || !notEmpty(toolName)) {
            return "";
        }
        return safely("onActing.toolDescription", () -> {
            // Both getToolkit() and the per-name lookup are host code reached through a default
            // interface method, so neither is assumed to be present or well-behaved.
            Toolkit toolkit = agent.getToolkit();
            if (toolkit == null) {
                return "";
            }
            List<ToolSchema> schemas = toolkit.getToolSchemas(List.of(toolName));
            if (schemas == null) {
                return "";
            }
            for (ToolSchema s : schemas) {
                if (s != null && toolName.equals(s.getName()) && notEmpty(s.getDescription())) {
                    return s.getDescription();
                }
            }
            return "";
        }, "");
    }

    // ══════════════ step (one ReAct round) ══════════════
    @Override
    public Flux<AgentEvent> onReasoning(
            Agent agent,
            RuntimeContext ctx,
            ReasoningInput input,
            Function<ReasoningInput, Flux<AgentEvent>> next) {

        return Flux.deferContextual(contextView -> guarded(() -> {
            TurnState ts = turnState(contextView);
            Context agentCtx = getOtelContext(contextView);

            int round = (ts != null) ? ts.roundCounter.incrementAndGet() : 1;
            String turnId = (ts != null) ? ts.turnId : "";
            String sessionId = (ts != null) ? ts.sessionId : "";
            String stepId = turnId + ":s" + round;

            Span step = tracer.spanBuilder("react round_" + round)
                    .setParent(agentCtx)
                    .setSpanKind(SpanKind.INTERNAL)
                    .startSpan();
            step.setAttribute("gen_ai.span.kind", "step");
            step.setAttribute("gen_ai.operation.name", "react");
            step.setAttribute("gen_ai.session.id", sessionId);
            step.setAttribute("gen_ai.turn.id", turnId);
            step.setAttribute("gen_ai.step.id", stepId);
            step.setAttribute("gen_ai.react.round", (long) round);
            if (input != null && input.messages() != null) {
                step.setAttribute("gen_ai.input.messages", msgListToJson(input.messages()));
            }
            Context stepCtx = withTurnIds(agentCtx.with(step), sessionId, turnId, stepId);
            StepState ss = new StepState(stepId, round, step, stepCtx);

            // Promote this step to turn scope and close the PREVIOUS round's step span (kept open
            // past its own Flux so its tool could nest under it). onActing reads ts.currentStep.
            if (ts != null) {
                closeStep(ts, ts.currentStep.getAndSet(ss), false);
            }

            debugSpan("step", step, agentCtx);

            Flux<AgentEvent> flux = next.apply(input)
                    .doOnError(err -> safely("onReasoning.error", () -> {
                        step.recordException(err);
                        step.setStatus(StatusCode.ERROR, safe(err.getMessage()));
                    }))
                    // Mark reasoning finished BEFORE the terminal signal reaches downstream.
                    // doFinally would be too late: it runs after propagation, so an outer
                    // doOnComplete (which is how harness's MemoryFlushMiddleware schedules its
                    // post-turn memory flush) would still see this round as live and the flush
                    // would be filed as part of it.
                    .doOnTerminate(() -> safely("onReasoning.terminate",
                            () -> ss.reasoningDone.set(true)))
                    .doOnCancel(() -> safely("onReasoning.cancel",
                            () -> ss.reasoningDone.set(true)))
                    .doFinally(sig -> safely("onReasoning.finally", () -> {
                        // NOTE: the step span is intentionally NOT ended here. In a real ReAct
                        // loop onActing runs AFTER this reasoning Flux completes and is not on its
                        // downstream, so ending the span now would orphan the tool span onto the
                        // agent. The span is closed when the next round opens or the turn ends
                        // (see closeStep). Here we only mark cancellation and drop stream buffers.
                        ss.reasoningDone.set(true);
                        if (sig == SignalType.CANCEL) {
                            step.setAttribute("gen_ai.react.finish_reason", "interrupted");
                            step.setAttribute("gen_ai.incomplete", true);
                        }
                        ss.thoughtBuffer.clear();
                        ss.outputBuffer.clear();
                        ss.toolResultBuffer.clear();
                    }))
                    .contextWrite(reactorCtx -> reactorCtx.put(STEP_STATE_KEY, ss))
                    .contextWrite(reactorCtx -> storeOtelContext(reactorCtx, stepCtx));
            return flux;
        }, () -> next.apply(input)));
    }

    /** End a step span exactly once; tolerant of null and safe to call from turn or round scope. */
    private static void closeStep(TurnState ts, StepState ss, boolean cancelled) {
        if (ss == null) {
            return;
        }
        if (cancelled) {
            ss.stepSpan.setAttribute("gen_ai.incomplete", true);
        }
        ss.thoughtBuffer.clear();
        ss.outputBuffer.clear();
        ss.toolResultBuffer.clear();
        ss.stepSpan.end();
    }

    // ══════════════ chat (raw model call, under current step) ══════════════
    @Override
    public Flux<AgentEvent> onModelCall(
            Agent agent,
            RuntimeContext ctx,
            ModelCallInput input,
            Function<ModelCallInput, Flux<AgentEvent>> next) {

        return Flux.deferContextual(contextView -> guarded(() -> {
            TurnState ts = turnState(contextView);
            StepState ctxStep = stepState(contextView);
            final StepState ss = (ctxStep != null) ? ctxStep
                    : (ts != null) ? ts.currentStep.get() : null;
            Context stepCtx = (ss != null)
                    ? ss.stepCtx
                    : getOtelContext(contextView);

            String sessionId = (ts != null) ? ts.sessionId : "";
            String turnId = (ts != null) ? ts.turnId : "";
            String stepId = (ss != null) ? ss.stepId : "";
            long round = (ss != null) ? ss.round : 0L;
            String model = (input != null && input.model() != null)
                    ? safe(input.model().getModelName()) : "";

            Span chat = tracer.spanBuilder("chat " + model)
                    .setParent(stepCtx)
                    .setSpanKind(SpanKind.CLIENT)
                    .startSpan();
            chat.setAttribute("gen_ai.span.kind", "chat");
            chat.setAttribute("gen_ai.operation.name", "chat");
            chat.setAttribute("gen_ai.session.id", sessionId);
            chat.setAttribute("gen_ai.turn.id", turnId);
            if (notEmpty(stepId)) {
                chat.setAttribute("gen_ai.step.id", stepId);
            }
            if (round > 0) {
                chat.setAttribute("gen_ai.react.round", round);
            }
            if (notEmpty(model)) {
                chat.setAttribute("gen_ai.request.model", model);
            }
            if (input != null) {
                applyModelRequest(chat, input.messages(), input.tools(), input.options());
                String provider = providerName(input.model());
                if (notEmpty(provider)) {
                    chat.setAttribute("gen_ai.provider.name", provider);
                }
            }

            AtomicBoolean sawToolCall = new AtomicBoolean(false);
            final long chatStartNanos = System.nanoTime();
            final AtomicBoolean firstTokenSeen = new AtomicBoolean(false);

            // next.apply assembles the model Flux, and a provider formats its prompt right there
            // (synchronously, in doStream's method body). Mark the window so the formatter can
            // find this round; see enterModelAssembly.
            Object[] assemblyScope = enterModelAssembly(ts, ss);
            Flux<AgentEvent> upstream;
            try {
                upstream = next.apply(input);
            } finally {
                restoreThreadContext(assemblyScope);
            }

            Flux<AgentEvent> flux = upstream
                    .doOnNext(ev -> safely("onModelCall.event", () -> {
                        // TTFT: time from span creation to first content event.
                        if (!firstTokenSeen.get()
                                && (ev instanceof TextBlockDeltaEvent
                                    || ev instanceof ThinkingBlockDeltaEvent
                                    || ev instanceof ToolCallStartEvent)) {
                            if (firstTokenSeen.compareAndSet(false, true)) {
                                long ttftMs = (System.nanoTime() - chatStartNanos) / 1_000_000L;
                                chat.setAttribute("gen_ai.response.time_to_first_token_ms", ttftMs);
                            }
                        }
                        if (ev instanceof ToolCallStartEvent e) {
                            sawToolCall.set(true);
                            // Bind this tool call to the round that requested it, so onActing can
                            // resolve the exact owning step by id even when a round issues several
                            // tool calls or execution is interleaved. Keyed by toolCallId.
                            if (ts != null && ss != null) {
                                String cid = safe(e.getToolCallId());
                                if (notEmpty(cid)) {
                                    ts.toolCallStepIndex.put(cid, ss);
                                }
                            }
                        } else if (ev instanceof ThinkingBlockDeltaEvent e) {
                            if (ss != null) {
                                ss.thoughtBuffer
                                        .computeIfAbsent(safe(e.getReplyId()), k -> new StringBuilder())
                                        .append(safe(e.getDelta()));
                            }
                        } else if (ev instanceof TextBlockDeltaEvent e) {
                            if (ss != null) {
                                ss.outputBuffer
                                        .computeIfAbsent(safe(e.getReplyId()), k -> new StringBuilder())
                                        .append(safe(e.getDelta()));
                            }
                        } else if (ev instanceof ModelCallEndEvent e) {
                            ChatUsage u = e.getUsage();
                            if (u != null) {
                                long in = u.getInputTokens();
                                long out = u.getOutputTokens();
                                long total = u.getTotalTokens();
                                long cacheRead = u.getCachedTokens();
                                chat.setAttribute("gen_ai.usage.input_tokens", in);
                                chat.setAttribute("gen_ai.usage.output_tokens", out);
                                chat.setAttribute("gen_ai.usage.total_tokens", total);
                                chat.setAttribute("gen_ai.usage.cache_read.input_tokens", cacheRead);
                                // Mean decode latency per output token; see the note in
                                // ClsFrameworkTracer.ResponseAccumulator.flushTo.
                                if (out > 0) {
                                    long elapsedMs =
                                            (System.nanoTime() - chatStartNanos) / 1_000_000L;
                                    chat.setAttribute("gen_ai.response.time_per_output_token_ms",
                                            (double) elapsedMs / out);
                                }
                                if (ts != null) {
                                    ts.sumInputTokens.addAndGet(in);
                                    ts.sumOutputTokens.addAndGet(out);
                                    ts.sumTotalTokens.addAndGet(total);
                                    ts.sumCacheReadTokens.addAndGet(cacheRead);
                                }
                            }
                            String rid = safe(e.getReplyId());
                            String thought = "";
                            String outputText = "";
                            if (ss != null) {
                                StringBuilder th = ss.thoughtBuffer.remove(rid);
                                thought = (th != null) ? th.toString() : "";
                                StringBuilder ob = ss.outputBuffer.remove(rid);
                                outputText = (ob != null) ? ob.toString() : "";
                            }

                            String finish = sawToolCall.get() ? "tool_calls" : "stop";
                            chat.setAttribute("gen_ai.response.finish_reasons",
                                    "[\"" + finish + "\"]");
                            chat.setAttribute("gen_ai.output.messages",
                                    assistantPartsToJson(thought, outputText));

                            if (ss != null) {
                                ss.stepSpan.setAttribute("gen_ai.react.finish_reason", finish);
                                if (notEmpty(thought)) {
                                    ss.stepSpan.setAttribute("gen_ai.react.thought", thought);
                                }
                            }
                            if (notEmpty(outputText) && ts != null) {
                                ts.lastAssistantText = outputText;
                            }
                        }
                    }));

            debugSpan("chat", chat, stepCtx);
            return runChatWithSpan(chat, stepCtx, flux);
        }, () -> next.apply(input)));
    }

    // ══════════════ tool (execute_tool, under current step) ══════════════
    @Override
    public Flux<AgentEvent> onActing(
            Agent agent,
            RuntimeContext ctx,
            ActingInput input,
            Function<ActingInput, Flux<AgentEvent>> next) {

        return Flux.deferContextual(contextView -> guarded(() -> {
            TurnState ts = turnState(contextView);

            // Resolve the owning ReAct round PRECISELY by tool-call id. In a real ReAct loop
            // onActing runs AFTER the reasoning Flux completed, so it is NOT on onReasoning's
            // downstream and the reactor STEP_STATE_KEY is usually absent here. The id index
            // (populated in onModelCall on ToolCallStartEvent) maps each tool call to the exact
            // round that requested it, which is correct even when one round issues MULTIPLE tool
            // calls or execution is interleaved. Fallbacks: currentStep → reactor STEP_STATE_KEY.
            String primaryCallId = "";
            if (input != null && input.toolCalls() != null && !input.toolCalls().isEmpty()) {
                primaryCallId = safe(input.toolCalls().get(0).getId());
            }
            StepState indexed = (ts != null && notEmpty(primaryCallId))
                    ? ts.toolCallStepIndex.remove(primaryCallId)
                    : null;
            StepState ss = (indexed != null)
                    ? indexed
                    : (ts != null && ts.currentStep.get() != null)
                            ? ts.currentStep.get()
                            : stepState(contextView);
            Context parent = (ss != null)
                    ? ss.stepCtx
                    : (ts != null)
                            ? ts.agentCtx
                            : getOtelContext(contextView);

            String sessionId = (ts != null) ? ts.sessionId : "";
            String turnId = (ts != null) ? ts.turnId : "";
            String stepId = (ss != null) ? ss.stepId : "";

            String toolName = "";
            if (input != null && input.toolCalls() != null && !input.toolCalls().isEmpty()) {
                toolName = safe(input.toolCalls().get(0).getName());
            }

            Span span = tracer.spanBuilder("execute_tool " + toolName)
                    .setParent(parent)
                    .setSpanKind(SpanKind.CLIENT)
                    .startSpan();
            span.setAttribute("gen_ai.span.kind", "tool");
            span.setAttribute("gen_ai.operation.name", "execute_tool");
            span.setAttribute("gen_ai.session.id", sessionId);
            span.setAttribute("gen_ai.turn.id", turnId);
            if (notEmpty(stepId)) {
                span.setAttribute("gen_ai.step.id", stepId);
            }
            if (ctx != null && notEmpty(ctx.getUserId())) {
                span.setAttribute("gen_ai.user.id", safe(ctx.getUserId()));
            }

            if (input != null && input.toolCalls() != null && !input.toolCalls().isEmpty()) {
                ToolUseBlock first = input.toolCalls().get(0);
                if (ts != null) {
                    ts.toolCallCount.incrementAndGet();
                }
                span.setAttribute("gen_ai.tool.name", safe(first.getName()));
                span.setAttribute("gen_ai.tool.type", "function");
                span.setAttribute("gen_ai.tool.call.id", safe(first.getId()));
                span.setAttribute("gen_ai.tool.call.arguments", mapToJson(first.getInput()));
                String toolDesc = toolDescription(agent, first.getName());
                if (notEmpty(toolDesc)) {
                    span.setAttribute("gen_ai.tool.description", toolDesc);
                }
            }

            Flux<AgentEvent> flux = next.apply(input)
                    .doOnNext(ev -> safely("onActing.event", () -> {
                        if (ev instanceof ToolResultTextDeltaEvent e) {
                            if (ss != null) {
                                ss.toolResultBuffer
                                        .computeIfAbsent(safe(e.getToolCallId()), k -> new StringBuilder())
                                        .append(safe(e.getDelta()));
                            }
                        } else if (ev instanceof ToolResultEndEvent e) {
                            String cid = safe(e.getToolCallId());
                            if (ss != null) {
                                StringBuilder buf = ss.toolResultBuffer.remove(cid);
                                if (buf != null && buf.length() > 0) {
                                    span.setAttribute("gen_ai.tool.call.result", buf.toString());
                                }
                            }
                            if (e.getState() != null
                                    && "ERROR".equalsIgnoreCase(e.getState().getValue())) {
                                span.setStatus(StatusCode.ERROR);
                                span.setAttribute("error.type", "execution_error");
                                span.setAttribute("gen_ai.tool.error.type", "execution_error");
                            }
                        }
                    }));
            debugSpan("tool", span, parent);
            return runToolWithSpan(span, parent, flux);
        }, () -> next.apply(input)));
    }

    // ══════════════ system prompt (transformer, pass-through) ══════════════
    @Override
    public Mono<String> onSystemPrompt(Agent agent, RuntimeContext ctx, String systemPrompt) {
        // Not a tracing hook; leave the prompt untouched.
        return Mono.justOrEmpty(systemPrompt);
    }

    // ══════════════ span lifecycle plumbing ══════════════

    private Flux<AgentEvent> runChatWithSpan(Span chatSpan, Context stepContext, Flux<AgentEvent> flux) {
        // Ended via doOnTerminate rather than doFinally so the span is closed BEFORE the terminal
        // signal reaches the subscriber — see the note in endOnceBefore.
        AtomicBoolean closed = new AtomicBoolean();
        return flux
                .doOnError(err -> {
                    chatSpan.recordException(err);
                    chatSpan.setStatus(StatusCode.ERROR, safe(err.getMessage()));
                })
                .doOnTerminate(() -> endOnceBefore(closed, chatSpan, false))
                .doOnCancel(() -> endOnceBefore(closed, chatSpan, true))
                .contextWrite(reactorCtx -> storeOtelContext(
                        reactorCtx.put(MODEL_CALL_MARK_KEY, Boolean.TRUE), stepContext));
    }

    private Flux<AgentEvent> runToolWithSpan(Span span, Context parent, Flux<AgentEvent> flux) {
        Context spanContext = parent.with(span);
        AtomicBoolean closed = new AtomicBoolean();
        return flux
                .doOnError(err -> {
                    span.recordException(err);
                    span.setStatus(StatusCode.ERROR, safe(err.getMessage()));
                })
                .doOnTerminate(() -> endOnceBefore(closed, span, false))
                .doOnCancel(() -> endOnceBefore(closed, span, true))
                .contextWrite(reactorCtx -> storeOtelContext(reactorCtx, spanContext));
    }

    /**
     * End a span exactly once, marking it incomplete when the sequence was cancelled.
     *
     * <p><b>Why not {@code doFinally}.</b> Reactor invokes a {@code doFinally} callback
     * <em>after</em> propagating {@code onComplete}/{@code onError} downstream. A blocking caller
     * ({@code agent.call(...).block()}, or the imperative {@code Mono.block()} that memory
     * compression uses) is therefore released while the callback is still pending on the emitting
     * thread. An application that flushes and shuts the exporter down immediately after that call
     * loses those spans — they get ended after the provider is closed, so they are never exported.
     * {@code doOnTerminate} runs before the terminal signal is propagated, which removes the race;
     * {@code doOnCancel} covers the one case it does not see.
     */
    private static void endOnceBefore(AtomicBoolean closed, Span span, boolean cancelled) {
        safely("span.end", () -> {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            if (cancelled) {
                span.setAttribute("gen_ai.incomplete", true);
            }
            span.end();
        });
    }

    // ══════════════ Reactor Context accessors ══════════════

    private static TurnState turnState(reactor.util.context.ContextView cv) {
        return cv.hasKey(TURN_STATE_KEY) ? cv.get(TURN_STATE_KEY) : null;
    }

    private static StepState stepState(reactor.util.context.ContextView cv) {
        return cv.hasKey(STEP_STATE_KEY) ? cv.get(STEP_STATE_KEY) : null;
    }

    /**
     * Immutable snapshot of a turn, able to cross a thread hop without pinning any span.
     *
     * <p><b>Why values and not the live state.</b> A {@link TurnState} reference would keep the
     * turn's {@code Span} objects — and their attributes, which include whole prompts — reachable
     * for as long as the snapshot lives. That is unbounded if a host schedules a long-delayed task
     * during a turn. Storing only our ids plus an OTel {@link SpanContext} (a value type: trace
     * id, span id, flags, trace state) is enough to re-establish parentage via
     * {@link Span#wrap(SpanContext)}, which yields a non-recording span carrying just that value.
     *
     * <p><b>Why not "ids only, look the turn back up".</b> Our {@code session.id} / {@code turn.id}
     * are business identifiers, not trace identifiers — nothing in them can reconstruct the parent
     * span. Recovering it would mean re-reading {@link #LAST_TURN_BY_SESSION} on the worker thread,
     * which (a) still reaches the spans, only later, (b) is bounded by that index's TTL and size,
     * and (c) is outright wrong when the same session has already started its next turn: the index
     * holds one entry per session, so the lookup would return turn N+1 and the span would be filed
     * under the wrong turn. The snapshot is taken while the correct turn is still in hand.
     *
     * <p>{@link #frameworkStepCounter} is the one exception to "values only", and deliberately so:
     * it is the owning turn's {@code :f{N}} sequence, shared by reference. An {@link AtomicInteger}
     * is a standalone object — holding it does NOT keep the {@link TurnState} that declares it
     * reachable, so no span is retained. Sharing the counter (rather than claiming a number when
     * the snapshot is taken) matters because the Scheduler hook snapshots <em>every</em> task
     * submitted during a turn, while only the few that actually reach a model call ever synthesise
     * a step. Claiming eagerly burned a number per submitted task and left gaps — the first
     * memory flush of a turn came out as {@code :f3}. Claiming on use keeps the sequence dense.
     */
    static final class TurnSnapshot {
        final String sessionId;
        final String turnId;
        /** Step id when a ReAct round was in flight at capture time, else empty. */
        final String stepId;
        /** Parent for spans created on the far side of the hop; never holds a recording span. */
        final SpanContext parent;
        /** The owning turn's framework-step sequence, drawn from only when a step is synthesised. */
        final AtomicInteger frameworkStepCounter;

        private TurnSnapshot(String sessionId, String turnId, String stepId,
                             SpanContext parent, AtomicInteger frameworkStepCounter) {
            this.sessionId = sessionId;
            this.turnId = turnId;
            this.stepId = stepId;
            this.parent = parent;
            this.frameworkStepCounter = frameworkStepCounter;
        }

        /** OTel context whose current span is {@link #parent}, for use as a span parent. */
        Context asParentContext() {
            if (parent == null || !parent.isValid()) {
                return Context.root();
            }
            return withTurnIds(Context.root().with(Span.wrap(parent)), sessionId, turnId, stepId);
        }

        /** Next framework-step id for this turn, claimed at the moment a step is actually made. */
        String nextFrameworkStepId() {
            int seq = (frameworkStepCounter != null) ? frameworkStepCounter.incrementAndGet() : 1;
            return turnId + ":f" + seq;
        }

        boolean hasTurn() {
            return notEmpty(turnId);
        }
    }

    /**
     * Snapshot the turn held on this thread so it can travel across a Scheduler hop.
     *
     * <p>Needed because a component may hand work to a Scheduler <em>without</em> keeping it on the
     * reactive chain — {@code Mono.defer(..).subscribeOn(..).subscribe(cb, cb)} is a fresh,
     * detached subscription whose Reactor Context is empty, so neither the context nor our
     * ThreadLocals reach the worker thread on their own. The snapshot is taken on the thread that
     * <em>submits</em> the task, where the turn is still in scope.
     *
     * @return {@code null} when this thread carries no turn, so the hook can skip wrapping.
     */
    static TurnSnapshot captureTurnContextForHop() {
        TurnState ts = THREAD_TURN.get();
        // THREAD_STEP is only set to a step whose reasoning is still running (see
        // bridgeThreadContext / StepState.reasoningDone). That matters here: a post-turn task is
        // submitted from doOnComplete, after the last round, when TurnState.currentStep still
        // points at that round. Treating it as live would file the framework's memory call as
        // part of the final ReAct round instead of giving it its own framework step.
        StepState ss = THREAD_STEP.get();
        String session = THREAD_LAST_SESSION.get();

        if (ts == null) {
            // No turn, but the thread may still name the session it last served; that alone lets
            // a later call be attributed when exactly one session is tracked.
            return notEmpty(session)
                    ? new TurnSnapshot(session, "", "", null, null)
                    : null;
        }

        if (ss != null) {
            // A ReAct round is genuinely in flight: hang the far-side span off that step.
            return new TurnSnapshot(ts.sessionId, ts.turnId, ss.stepId,
                    ss.stepSpan.getSpanContext(), ts.frameworkStepCounter);
        }
        // Between rounds: the far side will synthesise a step under the agent span, drawing its
        // sequence number from the turn's counter only if it actually gets that far.
        return new TurnSnapshot(ts.sessionId, ts.turnId, "",
                ts.agentSpan.getSpanContext(), ts.frameworkStepCounter);
    }

    /** Snapshot published for the duration of a task that hopped onto this thread. */
    private static final ThreadLocal<TurnSnapshot> THREAD_SNAPSHOT = new ThreadLocal<>();

    /** Install a snapshot from {@link #captureTurnContextForHop} onto the current thread. */
    static Object[] installTurnContext(TurnSnapshot snapshot) {
        Object[] previous = new Object[]{THREAD_SNAPSHOT.get(), THREAD_LAST_SESSION.get()};
        THREAD_SNAPSHOT.set(snapshot);
        if (notEmpty(snapshot.sessionId)) {
            THREAD_LAST_SESSION.set(snapshot.sessionId);
        }
        return previous;
    }

    /** Undo {@link #installTurnContext}, leaving the worker thread exactly as it was found. */
    static void uninstallTurnContext(Object[] previous) {
        TurnSnapshot prevSnapshot = (TurnSnapshot) previous[0];
        if (prevSnapshot != null) {
            THREAD_SNAPSHOT.set(prevSnapshot);
        } else {
            THREAD_SNAPSHOT.remove();
        }
        String session = (String) previous[1];
        if (notEmpty(session)) {
            THREAD_LAST_SESSION.set(session);
        } else {
            THREAD_LAST_SESSION.remove();
        }
    }

    /**
     * Snapshot + restore helper for the ThreadLocal bridge. Called by
     * {@link ClsFrameworkTracer#runWithContext} around every Reactor signal.
     *
     * @return previous state to pass to {@link #restoreThreadContext}
     */
    static Object[] bridgeThreadContext(reactor.util.context.ContextView cv) {
        TurnState prevTs = THREAD_TURN.get();
        StepState prevSs = THREAD_STEP.get();
        TurnState ts = turnState(cv);
        StepState ss = stepState(cv);
        if (ss == null && ts != null) {
            // Only adopt the turn's current step while its reasoning is still running — see
            // StepState.reasoningDone. After the last round the step stays open to host its tool
            // span, and must not become the parent of post-turn work.
            ss = liveStep(ts);
        }
        THREAD_TURN.set(ts);
        THREAD_STEP.set(ss);
        return new Object[]{prevTs, prevSs};
    }

    /** The turn's current round, but only while that round's reasoning is still in flight. */
    private static StepState liveStep(TurnState ts) {
        if (ts == null) {
            return null;
        }
        StepState ss = ts.currentStep.get();
        return (ss != null && ss.isLive()) ? ss : null;
    }

    /**
     * Mark the window in which a model call's {@code Flux} is being assembled, so synchronous work
     * performed during assembly can still find the owning round.
     *
     * <p>Needed because the {@link ClsFrameworkTracer#runWithContext} bridge covers only signal
     * delivery, while a formatter runs during assembly — see {@link #currentFormatStep}. The
     * window is narrow and strictly nested (it ends before {@code onModelCall} returns), and the
     * previous values are restored, so pooled threads never leak state.
     *
     * @return previous state to pass to {@link #restoreThreadContext}
     */
    static Object[] enterModelAssembly(TurnState ts, StepState ss) {
        TurnState prevTs = THREAD_TURN.get();
        StepState prevSs = THREAD_STEP.get();
        THREAD_TURN.set(ts);
        THREAD_STEP.set(ss);
        return new Object[]{prevTs, prevSs};
    }

    /**
     * Resolve the step a <em>formatter</em> call belongs to.
     *
     * <p><b>Why this needs its own channel.</b> {@code Tracer.callFormat} is handed no
     * {@code ContextView}, so the Reactor Context is unreachable. The ThreadLocal bridge in
     * {@link ClsFrameworkTracer#runWithContext} is not enough either, and this is the part that is
     * easy to get wrong: that bridge is driven by {@code Hooks.onEachOperator} and so is only
     * populated while a Reactor <em>signal</em> is being delivered. A formatter runs earlier than
     * that — {@code OpenAIChatModel.doStream0} calls {@code formatter.format(...)} in its own
     * method body, i.e. while the model {@code Flux} is being <em>assembled</em>, before anything
     * subscribes. {@link #enterModelAssembly} therefore marks that assembly window explicitly;
     * without it this method returns {@code null} on every real call and no {@code format} span is
     * ever produced (pinned by {@code ClsFormatSpanTest}).
     *
     * <p>It must <b>not</b> fall back to the per-session index or synthesise a step the way a model
     * call does. A formatter runs on <em>every</em> model call, so guessing wrong would not produce
     * one stray span but a steady stream of them; and a {@code format} span detached from its turn
     * carries no diagnostic value anyway — it only means anything next to the {@code chat} span it
     * prepared. When the owning round cannot be identified the caller records nothing at all.
     *
     * @return the live round's state, or {@code null} when it cannot be identified
     */
    static FormatSite currentFormatStep() {
        TurnState ts = THREAD_TURN.get();
        StepState ss = THREAD_STEP.get();
        if (ss == null || !ss.isLive()) {
            ss = liveStep(ts);
        }
        if (ss == null || ts == null) {
            return null;
        }
        return new FormatSite(ss.stepCtx, ts.sessionId, ts.turnId, ss.stepId, ss.round);
    }

    /** Where a {@code format} span attaches, plus the identity it carries. */
    static final class FormatSite {        final Context parent;
        final String sessionId;
        final String turnId;
        final String stepId;
        final long round;

        FormatSite(Context parent, String sessionId, String turnId, String stepId, int round) {
            this.parent = parent;
            this.sessionId = sessionId;
            this.turnId = turnId;
            this.stepId = stepId;
            this.round = round;
        }
    }

    /** Restore the previous ThreadLocal state after signal processing. */
    static void restoreThreadContext(Object[] prev) {
        THREAD_TURN.set((TurnState) prev[0]);
        THREAD_STEP.set((StepState) prev[1]);
    }

    /** Package-private alias for {@link #getOtelContext} usable from {@link ClsFrameworkTracer}. */
    static Context getOtelContextFromView(reactor.util.context.ContextView cv) {
        return getOtelContext(cv);
    }

    /** True when {@link #onModelCall} already owns the model call visible on this context. */
    static boolean isModelCallInstrumented(reactor.util.context.ContextView cv) {
        return cv.hasKey(MODEL_CALL_MARK_KEY);
    }

    /**
     * Where a model call observed OUTSIDE the middleware chain should attach, plus the identity
     * it should carry. Produced by {@link #resolveExternalCallSite}.
     */
    static final class ExternalCallSite {
        /** Parent OTel context for the {@code chat} span. */
        final Context parent;
        final String sessionId;
        final String turnId;
        final String stepId;
        /**
         * Step span synthesised for this call, or {@code null} when an existing step was reused.
         * The caller MUST end it once the chat span ends.
         */
        final Span syntheticStep;

        private ExternalCallSite(Context parent, String sessionId, String turnId, String stepId,
                                 Span syntheticStep) {
            this.parent = parent;
            this.sessionId = sessionId;
            this.turnId = turnId;
            this.stepId = stepId;
            this.syntheticStep = syntheticStep;
        }
    }

    /**
     * Resolve where to hang a model call the middleware chain never saw — the framework's own
     * memory flush / history summarisation.
     *
     * <p>Per the CLS GenAI spec a {@code chat} span lives under a {@code step}, so:
     * <ul>
     *   <li><b>A ReAct round is in flight</b> ⇒ reuse its step span.</li>
     *   <li><b>A turn is active but no round is in flight</b> (the framework does its memory work
     *       at a turn boundary, before the first round or after the last) ⇒ synthesise a step
     *       span under the agent span, id {@code {turnId}:f{N}}, so the chat still nests under a
     *       step and the tree stays spec-shaped. The caller ends it with the chat span.</li>
     *   <li><b>No turn at all</b> (background job) ⇒ whatever the context carries, typically the
     *       root — the call legitimately belongs to no turn and forms its own trace.</li>
     * </ul>
     */
    static ExternalCallSite resolveExternalCallSite(
            reactor.util.context.ContextView cv, Tracer tracer) {
        TurnState ts = turnState(cv);
        StepState ss = stepState(cv);
        if (ss == null && ts != null) {
            ss = liveStep(ts);
        }

        // ThreadLocal fallback: a component making a synchronous model call from inside signal
        // processing (Mono.block()) starts a fresh subscription with no Reactor Context. The
        // tracing hook's runWithContext bridges turn state to ThreadLocals before every signal,
        // so such code can still find the owning turn.
        if (ts == null) {
            ts = THREAD_TURN.get();
            if (ts != null && ss == null) {
                ss = liveStep(ts);
            }
        }

        if (ss != null) {
            return new ExternalCallSite(ss.stepCtx,
                    ts != null ? ts.sessionId : "", ts != null ? ts.turnId : "", ss.stepId, null);
        }
        if (ts == null) {
            // No live turn on any propagation channel. Three fallbacks remain, in order.

            // (a) Scheduler-hop snapshot — the turn as it was on the thread that SUBMITTED this
            // task. Carries ids plus the parent SpanContext as values, so it is both exact and
            // free of any reference to the turn's spans. This is the channel that covers a
            // detached post-turn subscription (agentscope-harness's memory flush).
            TurnSnapshot snapshot = THREAD_SNAPSHOT.get();
            if (snapshot != null && snapshot.hasTurn()) {
                return fromSnapshot(snapshot, tracer);
            }

            // (b) OTel Context — survives arbitrary thread hops when the host propagated it
            // (explicitly, via a context-propagating executor, or via a java agent).
            Context otel = getOtelContext(cv);
            String[] ids = otel.get(OTEL_TURN_IDS_KEY);
            if (ids != null) {
                return new ExternalCallSite(otel, ids[0], ids[1], ids[2], null);
            }

            // (c) Session index — the framework's memory extraction runs AFTER the agent replied
            // (it summarises the finished conversation), by which point Reactor Context,
            // ThreadLocal turn state and OTel Context are all gone. Recover the turn via the
            // session: first the session this thread last served, else the only session in
            // flight. Attaching to an ENDED agent span is legal in OTel — parentage is a span-id
            // reference — so the memory span still renders nested, which is what a memory call
            // conceptually is.
            ts = recentTurnForSession(THREAD_LAST_SESSION.get());
            if (ts == null) {
                ts = soleTrackedTurn();
            }
            if (ts == null) {
                return new ExternalCallSite(otel, "", "", "", null);
            }
        }

        return synthesiseFrameworkStep(tracer, ts.agentCtx, ts.sessionId, ts.turnId,
                ts.turnId + ":f" + ts.frameworkStepCounter.incrementAndGet());
    }

    /**
     * Build the call site from a {@link TurnSnapshot} that crossed a Scheduler hop.
     *
     * <p>When a ReAct round was in flight at capture time the snapshot's parent IS that step, so
     * the chat span attaches straight to it. Otherwise a {@code framework_step} is synthesised
     * under the agent span, using the sequence number claimed at capture time.
     */
    private static ExternalCallSite fromSnapshot(TurnSnapshot snapshot, Tracer tracer) {
        Context parent = snapshot.asParentContext();
        if (notEmpty(snapshot.stepId)) {
            return new ExternalCallSite(parent, snapshot.sessionId, snapshot.turnId,
                    snapshot.stepId, null);
        }
        return synthesiseFrameworkStep(tracer, parent, snapshot.sessionId, snapshot.turnId,
                snapshot.nextFrameworkStepId());
    }

    /**
     * Create the {@code step} span that holds a model call the middleware chain never saw, so the
     * tree keeps the spec shape ({@code chat} always under a {@code step}) without disturbing
     * {@code gen_ai.react.round} or the {@code :s{N}} ids of real rounds.
     */
    private static ExternalCallSite synthesiseFrameworkStep(
            Tracer tracer, Context parent, String sessionId, String turnId, String stepId) {
        Span step = tracer.spanBuilder("framework_step")
                .setParent(parent)
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        step.setAttribute("gen_ai.span.kind", "step");
        step.setAttribute("gen_ai.operation.name", "step");
        step.setAttribute("gen_ai.session.id", sessionId);
        step.setAttribute("gen_ai.turn.id", turnId);
        step.setAttribute("gen_ai.step.id", stepId);
        // Distinguishes this from a ReAct round: it wraps framework-internal work (memory flush /
        // history summarisation) that the agent's middleware chain never routes through.
        step.setAttribute("gen_ai.step.type", "framework");
        debugSpan("step(framework)", step, parent);
        return new ExternalCallSite(parent.with(step), sessionId, turnId, stepId, step);
    }

    /**
     * 从 Reactor ContextView 中获取 OTel Context。
     * 优先从自定义 key {@link #OTEL_CONTEXT_KEY} 读取（不受第三方框架干扰），
     * 其次尝试 {@link ContextPropagationOperator}（兼容已正确初始化的场景），
     * 最后 fallback 到 {@link Context#current()}。
     */
    private static Context getOtelContext(reactor.util.context.ContextView cv) {
        if (cv.hasKey(OTEL_CONTEXT_KEY)) {
            return cv.get(OTEL_CONTEXT_KEY);
        }
        try {
            return ContextPropagationOperator
                    .getOpenTelemetryContextFromContextView(cv, Context.current());
        } catch (Throwable t) {
            return Context.current();
        }
    }

    /**
     * 将 OTel Context 同时存入自定义 key 和 {@link ContextPropagationOperator}。
     * 双写确保无论下游通过哪种方式读取都能拿到正确的 parent context。
     */
    static reactor.util.context.Context storeOtelContext(
            reactor.util.context.Context reactorCtx, Context otelCtx) {
        reactor.util.context.Context updated = reactorCtx.put(OTEL_CONTEXT_KEY, otelCtx);
        try {
            updated = ContextPropagationOperator.storeOpenTelemetryContext(updated, otelCtx);
        } catch (Throwable ignored) {
            // ContextPropagationOperator 未初始化时会抛异常，忽略即可
        }
        return updated;
    }

    // ══════════════ helpers ══════════════

    /**
     * Telemetry must never break the business flow. Runs the instrumentation {@code build} to
     * produce the observed Flux; if building throws, logs (rate-limited) and falls back to the
     * unobserved business Flux from {@code fallback}. Note this guards the SYNCHRONOUS span-setup
     * done inside {@code Flux.deferContextual}; downstream event callbacks are guarded separately
     * by {@link #safely}. If {@code fallback} itself somehow fails, the error propagates (that is a
     * genuine business error, not a telemetry one).
     */
    private static Flux<AgentEvent> guarded(
            java.util.function.Supplier<Flux<AgentEvent>> build,
            java.util.function.Supplier<Flux<AgentEvent>> fallback) {
        try {
            return build.get();
        } catch (Throwable t) {
            telemetryFailed("instrumentation-build", t);
            return fallback.get();
        }
    }

    /** Run a telemetry side effect, swallowing any error so it can never break the event stream. */
    static void safely(String where, Runnable action) {
        try {
            action.run();
        } catch (Throwable t) {
            telemetryFailed(where, t);
        }
    }

    /**
     * {@link #safely(String, Runnable)} for a value-returning read, used when the value comes from
     * host code that may throw — e.g. a {@code default} interface method an implementation
     * overrode badly. Telemetry must never propagate such a failure into the agent's own flow.
     */
    static <T> T safely(String where, java.util.function.Supplier<T> read, T fallback) {
        try {
            T value = read.get();
            return (value != null) ? value : fallback;
        } catch (Throwable t) {
            telemetryFailed(where, t);
            return fallback;
        }
    }

    private static final AtomicLong LAST_TELEMETRY_WARN_MINUTE = new AtomicLong(-1);
    private static final AtomicLong TELEMETRY_DROPPED = new AtomicLong();

    /** Rate-limited (once/minute) warning so a persistent telemetry fault cannot flood logs. */
    static void telemetryFailed(String where, Throwable t) {
        long dropped = TELEMETRY_DROPPED.incrementAndGet();
        long minute = System.currentTimeMillis() / 60_000L;
        long last = LAST_TELEMETRY_WARN_MINUTE.get();
        if (last != minute && LAST_TELEMETRY_WARN_MINUTE.compareAndSet(last, minute)) {
            System.err.println("[cls-tracing] telemetry error at " + where
                    + " (business unaffected; total dropped=" + dropped + "): " + t);
        }
    }

    /** Set to a non-empty value to print span linkage to stdout for verifying the trace tree. */
    private static final boolean TRACE_DEBUG =
            System.getenv("CLS_TRACE_DEBUG") != null && !System.getenv("CLS_TRACE_DEBUG").isEmpty();

    static void debugSpan(String label, Span span, Context parent) {
        if (!TRACE_DEBUG) {
            return;
        }
        String traceId = span.getSpanContext().getTraceId();
        String spanId = span.getSpanContext().getSpanId();
        String parentSpanId = Span.fromContext(parent).getSpanContext().getSpanId();
        System.out.println("[TRACE] " + label
                + " traceId=" + traceId
                + " spanId=" + spanId
                + " parentSpanId=" + parentSpanId);
    }

    static boolean notEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    static String safe(String s) {
        return s == null ? "" : s;
    }

    /** Comma-joined tool names from a list of tool-use blocks (for HITL attributes). */
    private static String toolUseNames(List<ToolUseBlock> calls) {
        if (calls == null || calls.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (ToolUseBlock c : calls) {
            if (c == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(safe(c.getName()));
        }
        return sb.toString();
    }

    /** Flatten a tool-result block's output blocks into plain text. */
    private static String toolResultBlockText(ToolResultBlock r) {
        if (r == null || r.getOutput() == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (ContentBlock b : r.getOutput()) {
            if (b instanceof TextBlock t) {
                sb.append(safe(t.getText()));
            }
        }
        return sb.toString();
    }

    /** Serialise a list of {@link Msg} into the spec's {@code [{role, parts:[{type,content}]}]} JSON. */
    static String msgListToJson(List<Msg> msgs) {
        if (msgs == null || msgs.isEmpty()) {
            return "[]";
        }
        List<Object> arr = new ArrayList<>(msgs.size());
        for (Msg m : msgs) {
            if (m != null) {
                arr.add(msgToMap(m));
            }
        }
        return writeJson(arr, "[]");
    }

    /**
     * The spec's {@code gen_ai.system_instructions}: the system prompt lifted out of the message
     * list into its own attribute, so it can be read without parsing {@code gen_ai.input.messages}.
     * Returns {@code ""} when the call carries none, in which case the caller omits the attribute.
     *
     * <p>{@code gen_ai.input.messages} deliberately still contains these messages — it is the
     * verbatim record of what was sent, and trimming it would silently change an attribute that
     * existing dashboards and tests already read.
     */
    static String systemInstructionsToJson(List<Msg> msgs) {
        if (msgs == null || msgs.isEmpty()) {
            return "";
        }
        List<Object> arr = new ArrayList<>();
        for (Msg m : msgs) {
            if (m != null && m.getRole() == MsgRole.SYSTEM) {
                arr.add(msgToMap(m));
            }
        }
        return arr.isEmpty() ? "" : writeJson(arr, "");
    }

    /**
     * The spec's {@code gen_ai.tool.definitions}: the tool schemas offered to the model on this
     * call. Without it a trace only records which tools were <em>invoked</em>, so a model that
     * failed to call an expected tool is indistinguishable from one that was never offered it.
     */
    static String toolDefinitionsToJson(List<ToolSchema> tools) {
        if (tools == null || tools.isEmpty()) {
            return "";
        }
        List<Object> arr = new ArrayList<>(tools.size());
        for (ToolSchema t : tools) {
            if (t == null) {
                continue;
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("type", "function");
            m.put("name", safe(t.getName()));
            if (notEmpty(t.getDescription())) {
                m.put("description", t.getDescription());
            }
            if (t.getParameters() != null && !t.getParameters().isEmpty()) {
                m.put("parameters", t.getParameters());
            }
            arr.add(m);
        }
        return arr.isEmpty() ? "" : writeJson(arr, "");
    }

    /**
     * Copy the sampling parameters of a model call onto its {@code chat} span. These are what makes
     * a recorded call reproducible: the same prompt at a different temperature is a different
     * experiment.
     *
     * <p><b>This is strictly a whitelist, and must stay one.</b> {@link GenerateOptions} also
     * exposes {@code getApiKey()} and {@code getBaseUrl()}; serialising the object wholesale — or
     * adding a convenient "copy everything" loop later — would write the caller's API key into
     * CLS. Only the fields enumerated below are ever read.
     *
     * <p><b>What these values are, precisely.</b> They are the options the <em>caller</em> passed
     * for this call — for a {@code ReActAgent} that is its {@code generateOptions}. A model
     * instance may additionally carry its own defaults (e.g. {@code OpenAIChatModel} keeps a
     * private {@code configuredOptions} built from its builder) which it merges in inside
     * {@code doStream}, below every observable boundary and with no accessor. So an attribute that
     * is absent here means "the caller did not specify it", <em>not</em> "the provider used its
     * own default"; and a value present here can still be overridden deeper down. Reading the
     * effective value would require reflecting into provider internals, which is not worth the
     * fragility.
     */
    static void applyRequestOptions(Span span, GenerateOptions o) {
        if (span == null || o == null) {
            return;
        }
        if (o.getTemperature() != null) {
            span.setAttribute("gen_ai.request.temperature", o.getTemperature());
        }
        if (o.getTopP() != null) {
            span.setAttribute("gen_ai.request.top_p", o.getTopP());
        }
        if (o.getTopK() != null) {
            span.setAttribute("gen_ai.request.top_k", o.getTopK().longValue());
        }
        // Newer models carry the budget in maxCompletionTokens instead; report whichever is set so
        // the attribute is not silently empty on them.
        Integer maxTokens = (o.getMaxTokens() != null)
                ? o.getMaxTokens() : o.getMaxCompletionTokens();
        if (maxTokens != null) {
            span.setAttribute("gen_ai.request.max_tokens", maxTokens.longValue());
        }
        if (o.getSeed() != null) {
            span.setAttribute("gen_ai.request.seed", o.getSeed());
        }
        if (o.getFrequencyPenalty() != null) {
            span.setAttribute("gen_ai.request.frequency_penalty", o.getFrequencyPenalty());
        }
        if (o.getPresencePenalty() != null) {
            span.setAttribute("gen_ai.request.presence_penalty", o.getPresencePenalty());
        }
    }

    /**
     * The GenAI provider behind a model instance, normalised to the spec's
     * {@code gen_ai.provider.name} values.
     *
     * <p>Derived from the implementing class, because AgentScope's {@code Model} exposes only a
     * model <em>name</em> ({@code hy4-preview}, {@code qwen-plus}) — which says nothing about who
     * served it. Without this, cost and latency cannot be grouped by vendor, and an
     * OpenAI-compatible endpoint is indistinguishable from OpenAI itself.
     *
     * <p>Returns {@code ""} for anything unrecognised rather than guessing: an attribute that is
     * absent is honest, whereas one that is wrong silently corrupts per-provider aggregates. The
     * values match the reference java-agent's mapping so traces from both are comparable.
     */
    static String providerName(Object model) {
        return (model == null) ? "" : providerNameOf(model.getClass().getName());
    }

    /**
     * The class-name → provider mapping, split out so it can be tested against the real
     * fully-qualified names ({@code io.agentscope.extensions.model.openai.OpenAIChatModel}) without
     * needing each provider on the test classpath.
     */
    static String providerNameOf(String cls) {
        if (cls == null) {
            return "";
        }
        if (cls.contains(".dashscope.") || cls.endsWith(".DashScopeChatModel")) {
            return "dashscope";
        }
        if (cls.contains(".gemini.") || cls.endsWith(".GeminiChatModel")) {
            return "gcp.gemini";
        }
        if (cls.contains(".anthropic.") || cls.endsWith(".AnthropicChatModel")) {
            return "anthropic";
        }
        if (cls.contains(".openai.") || cls.endsWith(".OpenAIChatModel")) {
            return "openai";
        }
        if (cls.contains(".ollama.") || cls.endsWith(".OllamaChatModel")) {
            return "ollama";
        }
        return "";
    }

    /**
     * Set the request-side attributes shared by both model-call paths: the middleware's
     * {@code onModelCall} and {@link ClsFrameworkTracer}'s framework-level hook. Keeping it in one
     * place is what stops the two paths from drifting apart in what they record.
     */
    static void applyModelRequest(
            Span span, List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
        if (messages != null) {
            span.setAttribute("gen_ai.input.messages", msgListToJson(messages));
            String sys = systemInstructionsToJson(messages);
            if (notEmpty(sys)) {
                span.setAttribute("gen_ai.system_instructions", sys);
            }
        }
        if (tools != null && !tools.isEmpty()) {
            span.setAttribute("gen_ai.request.tool_count", (long) tools.size());
            String defs = toolDefinitionsToJson(tools);
            if (notEmpty(defs)) {
                span.setAttribute("gen_ai.tool.definitions", defs);
            }
        }
        applyRequestOptions(span, options);
    }

    private static Map<String, Object> msgToMap(Msg m) {
        Map<String, Object> map = new LinkedHashMap<>();
        MsgRole role = m.getRole();
        map.put("role", role == null ? "user" : role.name().toLowerCase());
        List<Object> parts = new ArrayList<>();
        List<ContentBlock> blocks = m.getContent();
        if (blocks != null) {
            for (ContentBlock b : blocks) {
                Map<String, Object> part = blockToMap(b);
                if (part != null) {
                    parts.add(part);
                }
            }
        }
        if (parts.isEmpty()) {
            String text = m.getTextContent();
            if (notEmpty(text)) {
                parts.add(textPart(text));
            }
        }
        map.put("parts", parts);
        return map;
    }

    private static Map<String, Object> blockToMap(ContentBlock b) {
        if (b == null) {
            return null;
        }
        if (b instanceof TextBlock t) {
            return textPart(t.getText());
        }
        if (b instanceof ThinkingBlock t) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("type", "thinking");
            p.put("content", safe(t.getThinking()));
            return p;
        }
        if (b instanceof ToolUseBlock t) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("type", "tool_use");
            p.put("id", safe(t.getId()));
            p.put("name", safe(t.getName()));
            p.put("input", t.getInput() == null ? Map.of() : t.getInput());
            return p;
        }
        if (b instanceof ToolResultBlock t) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("type", "tool_result");
            p.put("id", safe(t.getId()));
            p.put("name", safe(t.getName()));
            List<Object> out = new ArrayList<>();
            if (t.getOutput() != null) {
                for (ContentBlock ob : t.getOutput()) {
                    Map<String, Object> obm = blockToMap(ob);
                    if (obm != null) {
                        out.add(obm);
                    }
                }
            }
            p.put("output", out);
            return p;
        }
        return null;
    }

    private static Map<String, Object> textPart(String text) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("type", "text");
        p.put("content", safe(text));
        return p;
    }

    private static String assistantTextToJson(String text) {
        return assistantPartsToJson("", text);
    }

    static String assistantPartsToJson(String thought, String text) {
        List<Object> parts = new ArrayList<>();
        if (notEmpty(thought)) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("type", "thinking");
            p.put("content", thought);
            parts.add(p);
        }
        if (notEmpty(text)) {
            parts.add(textPart(text));
        }
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("role", "assistant");
        msg.put("parts", parts);
        List<Object> arr = new ArrayList<>();
        arr.add(msg);
        return writeJson(arr, "[]");
    }

    private static String mapToJson(Map<String, Object> input) {
        if (input == null || input.isEmpty()) {
            return "{}";
        }
        return writeJson(input, "{}");
    }

    private static String writeJson(Object value, String fallback) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return fallback;
        }
    }
}
