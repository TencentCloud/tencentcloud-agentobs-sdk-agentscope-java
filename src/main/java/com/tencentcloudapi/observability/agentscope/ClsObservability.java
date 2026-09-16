package com.tencentcloudapi.observability.agentscope;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * One-shot setup entry point for CLS observability (v2).
 *
 * <p>Wiring:
 * <ol>
 *     <li>Resolve {@link ClsConfig} (builder args / env vars / {@code .env})</li>
 *     <li>Build a standard {@link OtlpHttpSpanExporter} pointed at CLS's OTLP/HTTP
 *         trace endpoint, authenticated with an HTTP Basic {@code Authorization}
 *         header and routed with a {@code topic_id} header</li>
 *     <li>Assemble a {@link SdkTracerProvider} with a {@link BatchSpanProcessor}</li>
 *     <li>Register it as the process-wide {@code GlobalOpenTelemetry}</li>
 *     <li>Register a JVM shutdown hook to flush pending spans</li>
 * </ol>
 *
 * <p><b>Difference from v1.</b> v1 relied on AgentScope's stock
 * {@code OtelTracingMiddleware}. v2 ships its own {@link ClsTracingMiddleware},
 * obtained via {@link #newTracingMiddleware()}, which captures session/user IDs,
 * tool arguments, streamed tool results, and reasoning thoughts.
 *
 * <p><b>Transport.</b> Spans are uploaded over the vendor-neutral OTLP/HTTP
 * protocol to CLS, rather than through the CLS Java SDK. The span attributes
 * use the CLS GenAI Trace spec's dotted keys ({@code gen_ai.session.id}/
 * {@code gen_ai.turn.id}/{@code gen_ai.step.id} …), which the OTLP→CLS
 * conversion promotes to first-class CLS columns server-side.
 *
 * <pre>{@code
 * ClsObservability obs = ClsObservability.install();   // or install(config)
 *
 * ReActAgent agent = ReActAgent.builder()
 *         .name("assistant")
 *         .model(model)
 *         .toolkit(toolkit)
 *         .middlewares(List.of(obs.newTracingMiddleware()))
 *         .build();
 * }</pre>
 */
public final class ClsObservability {

    private static final Logger LOG = LoggerFactory.getLogger(ClsObservability.class);
    private static final String INSTRUMENTATION_SCOPE =
            "tencentcloud-agentobs-sdk-agentscope-java";

    private final SdkTracerProvider tracerProvider;
    private final SpanExporter exporter;
    private final OpenTelemetrySdk openTelemetry;
    private final Tracer tracer;

    private ClsObservability(SdkTracerProvider tracerProvider, SpanExporter exporter,
                             OpenTelemetrySdk openTelemetry, Tracer tracer) {
        this.tracerProvider = tracerProvider;
        this.exporter = exporter;
        this.openTelemetry = openTelemetry;
        this.tracer = tracer;
    }

    /**
     * Resolve configuration from env vars / {@code .env} and install globally.
     *
     * @throws IllegalStateException if required CLS parameters are missing.
     */
    public static ClsObservability install() {
        return install(ClsConfig.builder().build());
    }

    /** Install with an explicit {@link ClsConfig} and register it globally. */
    public static ClsObservability install(ClsConfig config) {
        SpanExporter exporter = OtlpHttpSpanExporter.builder()
                .setEndpoint(config.otlpTracesUrl())
                .addHeader("Authorization", config.authorizationHeader())
                .addHeader("topic_id", config.topicId())
                .setTimeout(Duration.ofSeconds(10))
                .build();

        Resource resource = Resource.getDefault().merge(
                Resource.create(Attributes.builder()
                        .put("service.name", config.serviceName())
                        .put("telemetry.sdk.language", "java")
                        .put("telemetry.sdk.name", INSTRUMENTATION_SCOPE)
                        .build()));

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(exporter)
                        .setMaxExportBatchSize(config.batchSize())
                        .build())
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();

        Tracer tracer = sdk.getTracer(INSTRUMENTATION_SCOPE);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                tracerProvider.close();
            } catch (Exception e) {
                LOG.warn("Error closing tracer provider on shutdown: {}", e.getMessage());
            }
        }, "cls-observability-v2-shutdown"));

        LOG.info("CLS observability (v2, OTLP/HTTP) installed for AgentScope Java "
                        + "(service={}, endpoint={}, topic={}). "
                        + "Add ClsObservability.newTracingMiddleware() to your agent.",
                config.serviceName(), config.otlpTracesUrl(), config.topicId());

        return new ClsObservability(tracerProvider, exporter, sdk, tracer);
    }

    /** The registered {@link OpenTelemetry} instance. */
    public OpenTelemetry openTelemetry() {
        return openTelemetry;
    }

    public SdkTracerProvider tracerProvider() {
        return tracerProvider;
    }

    public SpanExporter exporter() {
        return exporter;
    }

    /** The {@link Tracer} that {@link ClsTracingMiddleware} should emit spans through. */
    public Tracer tracer() {
        return tracer;
    }

    /** Flush any pending spans immediately. */
    public void flush() {
        tracerProvider.forceFlush().join(10, java.util.concurrent.TimeUnit.SECONDS);
    }

    /** Shut down the tracer provider, flushing and closing the OTLP exporter. */
    public void shutdown() {
        tracerProvider.close();
    }

    /**
     * Create the v2 {@link ClsTracingMiddleware} bound to this installation's tracer.
     * Add the returned object to your agent's middleware list.
     *
     * <p>Return type is {@code Object} so this SDK need not force AgentScope onto the
     * caller's classpath at compile time; cast or pass it straight into
     * {@code .middlewares(List.of(...))} where AgentScope is present.
     */
    public Object newTracingMiddleware() {
        return new ClsTracingMiddleware(tracer);
    }
}
