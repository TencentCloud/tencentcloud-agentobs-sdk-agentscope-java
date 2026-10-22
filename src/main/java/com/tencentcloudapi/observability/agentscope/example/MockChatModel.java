package com.tencentcloudapi.observability.agentscope.example;

import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.publisher.Flux;

/**
 * A fully offline mock model that drives a real AgentScope {@link io.agentscope.core.ReActAgent}
 * through one realistic ReAct loop — <em>without</em> any live LLM or API key.
 *
 * <p><b>Turn 1 (reasoning + acting):</b> returns a {@link ToolUseBlock} that asks the agent to call
 * the {@code get_weather} tool. This makes the real ReActAgent actually invoke the registered tool,
 * exercising the middleware's {@code onModelCall} (chat) and {@code onActing} (execute_tool) paths.
 *
 * <p><b>Turn 2 (final answer):</b> after the tool result comes back, returns a plain {@link TextBlock}
 * with a natural-language answer and no further tool calls, so the ReAct loop terminates.
 *
 * <p>It extends {@link ChatModelBase} — the base every real AgentScope model extends — rather than
 * implementing {@link Model} directly. That matters for observability: {@code ChatModelBase.stream()}
 * is {@code final} and routes through AgentScope's {@code TracerRegistry}, which is the only entry
 * point that also sees model calls made outside the middleware chain (the framework's own memory /
 * history-summarisation prompts). Implementing {@link Model} bare-handed would bypass it.
 *
 * <p>Because it plugs into the real model base class, the surrounding agent, toolkit, middleware
 * chain, and event stream are 100% genuine AgentScope machinery — only token generation is faked.
 */
public final class MockChatModel extends ChatModelBase {

    private final String modelName;
    private final String toolName;
    private final Map<String, Object> toolArgs;
    private final String finalAnswer;

    /** Counts how many times the agent has asked the model to generate. */
    private final AtomicInteger call = new AtomicInteger(0);

    /**
     * @param toolName the tool round 1 should request; pass {@code null} or an empty string for a
     *     model that never calls tools and answers directly — the shape of an auxiliary model such
     *     as the one a memory / summarisation component uses.
     */
    public MockChatModel(String modelName,
                         String toolName,
                         Map<String, Object> toolArgs,
                         String finalAnswer) {
        this.modelName = modelName;
        this.toolName = toolName;
        this.toolArgs = toolArgs;
        this.finalAnswer = finalAnswer;
    }

    @Override
    public String getModelName() {
        return modelName;
    }

    @Override
    protected Flux<ChatResponse> doStream(List<Msg> messages,
                                          List<ToolSchema> tools,
                                          GenerateOptions options) {
        int round = call.incrementAndGet();
        if (round == 1 && toolName != null && !toolName.isEmpty()) {
            // Round 1: ask the agent to call the weather tool.
            ToolUseBlock toolUse = new ToolUseBlock(
                    "call_mock_0001",
                    toolName,
                    toolArgs);
            ChatUsage usage = new ChatUsage(42, 17, 0.12);
            ChatResponse resp = ChatResponse.builder()
                    .id("resp-round-1")
                    .content(List.<ContentBlock>of(toolUse))
                    .usage(usage)
                    .finishReason("tool_calls")
                    .build();
            return Flux.just(resp);
        }

        // Round 2+: produce the final natural-language answer, no more tools.
        TextBlock answer = TextBlock.builder().text(finalAnswer).build();
        ChatUsage usage = new ChatUsage(60, 25, 0.15);
        ChatResponse resp = ChatResponse.builder()
                .id("resp-round-2")
                .content(List.<ContentBlock>of(answer))
                .usage(usage)
                .finishReason("stop")
                .build();
        return Flux.just(resp);
    }
}
