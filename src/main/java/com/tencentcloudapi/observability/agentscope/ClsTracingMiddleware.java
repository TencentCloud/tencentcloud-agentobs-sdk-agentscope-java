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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.opentelemetry.api.trace.Span;
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

    private final Tracer tracer;

    /** Turn sequence, the only shared mutable state — a monotonic id source, safe to share. */
    private final AtomicInteger turnCounter = new AtomicInteger(0);

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

        StepState(String stepId, int round, Span stepSpan, Context stepCtx) {
            this.stepId = stepId;
            this.round = round;
            this.stepSpan = stepSpan;
            this.stepCtx = stepCtx;
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
            Context parent = ContextPropagationOperator
                    .getOpenTelemetryContextFromContextView(contextView, Context.current());

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
            if (notEmpty(userId)) {
                agentSpan.setAttribute("gen_ai.user.id", userId);
            }
            agentSpan.setAttribute("gen_ai.input.messages", inputMsgsJson);
            Context agentCtx = entryCtx.with(agentSpan);

            TurnState ts = new TurnState(sid, turnId, userId, entry, agentSpan, agentCtx);

            debugSpan("entry", entry, parent);
            debugSpan("agent", agentSpan, entryCtx);

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
                    .doFinally(sig -> safely("onAgent.finally", () -> {
                        if (sig == SignalType.CANCEL && "normal".equals(ts.finishReason)) {
                            ts.finishReason = "interrupted";
                        }
                        // Close the last round's step span, which is deliberately kept open past
                        // its own onReasoning Flux so the round's tool span could nest under it.
                        closeStep(ts, ts.currentStep.getAndSet(null),
                                sig == SignalType.CANCEL);
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
                    }))
                    .contextWrite(reactorCtx -> reactorCtx.put(TURN_STATE_KEY, ts))
                    .contextWrite(reactorCtx ->
                            ContextPropagationOperator.storeOpenTelemetryContext(reactorCtx, agentCtx));
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

    // ══════════════ step (one ReAct round) ══════════════
    @Override
    public Flux<AgentEvent> onReasoning(
            Agent agent,
            RuntimeContext ctx,
            ReasoningInput input,
            Function<ReasoningInput, Flux<AgentEvent>> next) {

        return Flux.deferContextual(contextView -> guarded(() -> {
            TurnState ts = turnState(contextView);
            Context agentCtx = ContextPropagationOperator
                    .getOpenTelemetryContextFromContextView(contextView, Context.current());

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
            Context stepCtx = agentCtx.with(step);
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
                    .doFinally(sig -> safely("onReasoning.finally", () -> {
                        // NOTE: the step span is intentionally NOT ended here. In a real ReAct
                        // loop onActing runs AFTER this reasoning Flux completes and is not on its
                        // downstream, so ending the span now would orphan the tool span onto the
                        // agent. The span is closed when the next round opens or the turn ends
                        // (see closeStep). Here we only mark cancellation and drop stream buffers.
                        if (sig == SignalType.CANCEL) {
                            step.setAttribute("gen_ai.react.finish_reason", "interrupted");
                            step.setAttribute("gen_ai.incomplete", true);
                        }
                        ss.thoughtBuffer.clear();
                        ss.outputBuffer.clear();
                        ss.toolResultBuffer.clear();
                    }))
                    .contextWrite(reactorCtx -> reactorCtx.put(STEP_STATE_KEY, ss))
                    .contextWrite(reactorCtx ->
                            ContextPropagationOperator.storeOpenTelemetryContext(reactorCtx, stepCtx));
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
                    : ContextPropagationOperator
                            .getOpenTelemetryContextFromContextView(contextView, Context.current());

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
            if (input != null && input.messages() != null) {
                chat.setAttribute("gen_ai.input.messages", msgListToJson(input.messages()));
            }

            AtomicBoolean sawToolCall = new AtomicBoolean(false);

            Flux<AgentEvent> flux = next.apply(input)
                    .doOnNext(ev -> safely("onModelCall.event", () -> {
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
                            : ContextPropagationOperator
                                    .getOpenTelemetryContextFromContextView(contextView, Context.current());

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
        return flux
                .doOnError(err -> {
                    chatSpan.recordException(err);
                    chatSpan.setStatus(StatusCode.ERROR, safe(err.getMessage()));
                })
                .doFinally(sig -> {
                    if (sig == SignalType.CANCEL) {
                        chatSpan.setAttribute("gen_ai.incomplete", true);
                    }
                    chatSpan.end();
                })
                .contextWrite(reactorCtx ->
                        ContextPropagationOperator.storeOpenTelemetryContext(reactorCtx, stepContext));
    }

    private Flux<AgentEvent> runToolWithSpan(Span span, Context parent, Flux<AgentEvent> flux) {
        Context spanContext = parent.with(span);
        return flux
                .doOnError(err -> {
                    span.recordException(err);
                    span.setStatus(StatusCode.ERROR, safe(err.getMessage()));
                })
                .doFinally(sig -> {
                    if (sig == SignalType.CANCEL) {
                        span.setAttribute("gen_ai.incomplete", true);
                    }
                    span.end();
                })
                .contextWrite(reactorCtx ->
                        ContextPropagationOperator.storeOpenTelemetryContext(reactorCtx, spanContext));
    }

    // ══════════════ Reactor Context accessors ══════════════

    private static TurnState turnState(reactor.util.context.ContextView cv) {
        return cv.hasKey(TURN_STATE_KEY) ? cv.get(TURN_STATE_KEY) : null;
    }

    private static StepState stepState(reactor.util.context.ContextView cv) {
        return cv.hasKey(STEP_STATE_KEY) ? cv.get(STEP_STATE_KEY) : null;
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
    private static void safely(String where, Runnable action) {
        try {
            action.run();
        } catch (Throwable t) {
            telemetryFailed(where, t);
        }
    }

    private static final AtomicLong LAST_TELEMETRY_WARN_MINUTE = new AtomicLong(-1);
    private static final AtomicLong TELEMETRY_DROPPED = new AtomicLong();

    /** Rate-limited (once/minute) warning so a persistent telemetry fault cannot flood logs. */
    private static void telemetryFailed(String where, Throwable t) {
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

    private static void debugSpan(String label, Span span, Context parent) {
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

    private static boolean notEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    private static String safe(String s) {
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
    private static String msgListToJson(List<Msg> msgs) {
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

    private static String assistantPartsToJson(String thought, String text) {
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
