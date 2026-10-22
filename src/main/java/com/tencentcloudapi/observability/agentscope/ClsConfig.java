package com.tencentcloudapi.observability.agentscope;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves Tencent Cloud CLS configuration for OTLP/HTTP trace upload.
 *
 * <p>Resolution priority (highest first):
 * <ol>
 *     <li>Explicit values passed to the builder</li>
 *     <li>Process environment variables ({@code CLS_ENDPOINT} etc.)</li>
 *     <li>An optional {@code .env} file — {@code ./.env} or
 *         {@code ~/.agentobs-agentscope/config.env}. Existing environment
 *         variables are never overridden by the file.</li>
 * </ol>
 *
 * <p>The same four fields ({@code endpoint}/{@code topicId}/{@code secretId}/
 * {@code secretKey}) that the CLS-native SDK uses are reused here: with OTLP,
 * {@code secretId}/{@code secretKey} become the HTTP Basic {@code Authorization}
 * header and {@code topicId} becomes the {@code topic_id} header.
 */
public final class ClsConfig {

    /** Environment variable names. */
    public static final String ENV_ENDPOINT = "CLS_ENDPOINT";
    public static final String ENV_TOPIC_ID = "CLS_TOPIC_ID";
    public static final String ENV_SECRET_ID = "CLS_SECRET_ID";
    public static final String ENV_SECRET_KEY = "CLS_SECRET_KEY";
    public static final String ENV_SERVICE_NAME = "CLS_SERVICE_NAME";
    public static final String ENV_SOURCE = "CLS_SOURCE";
    public static final String ENV_BATCH_SIZE = "CLS_BATCH_SIZE";
    public static final String ENV_DEBUG = "CLS_DEBUG";
    /** Set to {@code 0/false/no} to stop tracing model calls the agent middleware never sees. */
    public static final String ENV_CAPTURE_FRAMEWORK_CALLS = "CLS_CAPTURE_FRAMEWORK_CALLS";
    /**
     * Set to {@code 0/false/no} to stop carrying turn context across Reactor {@code Scheduler}
     * hops. Only affects correlation of framework model calls dispatched as detached
     * subscriptions; those calls are still traced, just not attached to the originating turn.
     */
    public static final String ENV_PROPAGATE_ACROSS_SCHEDULERS =
            "CLS_PROPAGATE_ACROSS_SCHEDULERS";
    /**
     * Set to {@code 1/true/yes} to emit a {@code format} span for each prompt-formatting call.
     *
     * <p>Off by default: a formatter runs on every model call, so enabling this roughly doubles
     * the span count of a turn, and what it records is a re-encoding of the messages already on
     * the sibling {@code chat} span. Turn it on to debug provider-specific prompt assembly.
     */
    public static final String ENV_CAPTURE_FORMAT_CALLS = "CLS_CAPTURE_FORMAT_CALLS";

    private static final String DEFAULT_SERVICE_NAME = "agentscope-app";
    private static final int DEFAULT_BATCH_SIZE = 32;
    private static final int MAX_BATCH_SIZE = 1000;

    private final String endpoint;
    private final String topicId;
    private final String secretId;
    private final String secretKey;
    private final String serviceName;
    private final String source;
    private final int batchSize;
    private final boolean debug;
    private final boolean captureFrameworkModelCalls;
    private final boolean propagateAcrossSchedulers;
    private final boolean captureFormatCalls;

    private ClsConfig(String endpoint, String topicId, String secretId, String secretKey,
                      String serviceName, String source, int batchSize, boolean debug,
                      boolean captureFrameworkModelCalls, boolean propagateAcrossSchedulers,
                      boolean captureFormatCalls) {
        this.endpoint = endpoint;
        this.topicId = topicId;
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.serviceName = serviceName;
        this.source = source;
        this.batchSize = batchSize;
        this.debug = debug;
        this.captureFrameworkModelCalls = captureFrameworkModelCalls;
        this.propagateAcrossSchedulers = propagateAcrossSchedulers;
        this.captureFormatCalls = captureFormatCalls;
    }

    public String endpoint() {
        return endpoint;
    }

    public String topicId() {
        return topicId;
    }

    public String secretId() {
        return secretId;
    }

    public String secretKey() {
        return secretKey;
    }

    public String serviceName() {
        return serviceName;
    }

    public String source() {
        return source;
    }

    public int batchSize() {
        return batchSize;
    }

    public boolean debug() {
        return debug;
    }

    /**
     * Whether model calls that bypass the agent's middleware chain — the framework's own memory
     * flush / history summarisation prompts, which call {@code Model.stream()} directly — are
     * traced by hooking AgentScope's {@code TracerRegistry}. Defaults to {@code true}.
     */
    public boolean captureFrameworkModelCalls() {
        return captureFrameworkModelCalls;
    }

    /**
     * Whether the turn context is carried across Reactor {@code Scheduler} hops, via a
     * {@code Schedulers.onScheduleHook} that snapshots the turn on the submitting thread and
     * restores it around the task. Defaults to {@code true}.
     *
     * <p>This is what correlates a framework model call dispatched as a <em>detached</em>
     * subscription — {@code Mono.defer(..).subscribeOn(..).subscribe(cb, cb)}, which
     * {@code agentscope-harness}'s memory flush uses — with the turn that triggered it. Without
     * it such calls are still traced, but form their own traces whenever more than one session is
     * in flight.
     *
     * <p>The hook is process-wide: every task submitted to any Scheduler passes through it. Tasks
     * carrying no turn are returned unwrapped after three {@code ThreadLocal} reads, so the cost
     * to unrelated application code is small but not zero. Turn it off if your deployment cannot
     * accept a hook on the shared Reactor Schedulers, and accept uncorrelated framework spans.
     */
    public boolean propagateAcrossSchedulers() {
        return propagateAcrossSchedulers;
    }

    /**
     * Whether prompt-formatting calls get their own {@code format} span.
     *
     * @see #ENV_CAPTURE_FORMAT_CALLS
     */
    public boolean captureFormatCalls() {
        return captureFormatCalls;
    }

    /**
     * Build the {@code Authorization} header value for CLS's OTLP/HTTP trace
     * endpoint: {@code Basic base64(SecretId:SecretKey)}.
     *
     * <p>CLS authenticates OTLP uploads with HTTP Basic auth rather than a
     * signed request, so the exporter attaches this header on every batch.
     */
    public String authorizationHeader() {
        String raw = secretId + ":" + secretKey;
        String encoded = java.util.Base64.getEncoder()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    /**
     * Full OTLP/HTTP traces URL for CLS: {@code <endpoint>/v1/traces}.
     *
     * <p>CLS accepts uploads on the standard OTLP/HTTP trace path {@code /v1/traces}
     * (the same path the official {@code otlptracehttp} exporter appends by default),
     * not a custom {@code /opentelemetry/...} path.
     */
    public String otlpTracesUrl() {
        String base = endpoint;
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/v1/traces";
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder that layers explicit values over env vars and a {@code .env} file. */
    public static final class Builder {
        private String endpoint;
        private String topicId;
        private String secretId;
        private String secretKey;
        private String serviceName;
        private String source;
        private Integer batchSize;
        private Boolean debug;
        private Boolean captureFrameworkModelCalls;
        private Boolean propagateAcrossSchedulers;
        private Boolean captureFormatCalls;

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder topicId(String topicId) {
            this.topicId = topicId;
            return this;
        }

        public Builder secretId(String secretId) {
            this.secretId = secretId;
            return this;
        }

        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder source(String source) {
            this.source = source;
            return this;
        }

        public Builder batchSize(Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder debug(Boolean debug) {
            this.debug = debug;
            return this;
        }

        /**
         * Trace model calls that never enter the agent's middleware chain — the framework's own
         * memory flush / history summarisation prompts. Enabled by default.
         */
        public Builder captureFrameworkModelCalls(Boolean captureFrameworkModelCalls) {
            this.captureFrameworkModelCalls = captureFrameworkModelCalls;
            return this;
        }

        /**
         * Carry the turn context across Reactor {@code Scheduler} hops, so a framework model call
         * dispatched as a detached subscription (the framework's post-turn memory flush) is still
         * attributed to the turn that triggered it. Enabled by default.
         *
         * <p>Installs a process-wide {@code Schedulers.onScheduleHook}. Disable to keep the shared
         * Reactor Schedulers untouched, at the cost of such spans forming their own traces.
         */
        public Builder propagateAcrossSchedulers(Boolean propagateAcrossSchedulers) {
            this.propagateAcrossSchedulers = propagateAcrossSchedulers;
            return this;
        }

        /**
         * Emit a {@code format} span per prompt-formatting call. Disabled by default.
         *
         * <p>A formatter runs on every model call, so this roughly doubles a turn's span count
         * while recording a re-encoding of the messages already on the sibling {@code chat} span.
         * Enable it when debugging provider-specific prompt assembly.
         */
        public Builder captureFormatCalls(Boolean captureFormatCalls) {
            this.captureFormatCalls = captureFormatCalls;
            return this;
        }

        /**
         * Resolve the final configuration.
         *
         * @throws IllegalStateException when any required field
         *     ({@code endpoint}/{@code topicId}/{@code secretId}/{@code secretKey})
         *     is missing, listing exactly which ones.
         */
        public ClsConfig build() {
            Map<String, String> envFile = loadEnvFile();

            String resolvedEndpoint = normalizeEndpoint(
                    resolve(endpoint, ENV_ENDPOINT, envFile, ""));
            String resolvedTopicId = resolve(topicId, ENV_TOPIC_ID, envFile, "");
            String resolvedSecretId = resolve(secretId, ENV_SECRET_ID, envFile, "");
            String resolvedSecretKey = resolve(secretKey, ENV_SECRET_KEY, envFile, "");
            String resolvedServiceName = resolve(serviceName, ENV_SERVICE_NAME, envFile,
                    DEFAULT_SERVICE_NAME);
            String resolvedSource = resolve(source, ENV_SOURCE, envFile, "");
            if (resolvedSource.isEmpty()) {
                resolvedSource = detectLocalIp();
            }

            int resolvedBatchSize = DEFAULT_BATCH_SIZE;
            if (batchSize != null) {
                resolvedBatchSize = batchSize;
            } else {
                String raw = resolve(null, ENV_BATCH_SIZE, envFile, "");
                if (!raw.isEmpty()) {
                    try {
                        resolvedBatchSize = Integer.parseInt(raw.trim());
                    } catch (NumberFormatException ignored) {
                        resolvedBatchSize = DEFAULT_BATCH_SIZE;
                    }
                }
            }
            resolvedBatchSize = Math.max(1, Math.min(resolvedBatchSize, MAX_BATCH_SIZE));

            boolean resolvedDebug;
            if (debug != null) {
                resolvedDebug = debug;
            } else {
                String raw = resolve(null, ENV_DEBUG, envFile, "").toLowerCase();
                resolvedDebug = raw.equals("1") || raw.equals("true") || raw.equals("yes");
            }

            boolean resolvedCaptureFrameworkCalls = true;
            if (captureFrameworkModelCalls != null) {
                resolvedCaptureFrameworkCalls = captureFrameworkModelCalls;
            } else {
                String raw = resolve(null, ENV_CAPTURE_FRAMEWORK_CALLS, envFile, "").toLowerCase();
                if (!raw.isEmpty()) {
                    resolvedCaptureFrameworkCalls =
                            !(raw.equals("0") || raw.equals("false") || raw.equals("no"));
                }
            }

            boolean resolvedPropagateAcrossSchedulers = true;
            if (propagateAcrossSchedulers != null) {
                resolvedPropagateAcrossSchedulers = propagateAcrossSchedulers;
            } else {
                String raw = resolve(null, ENV_PROPAGATE_ACROSS_SCHEDULERS, envFile, "")
                        .toLowerCase();
                if (!raw.isEmpty()) {
                    resolvedPropagateAcrossSchedulers =
                            !(raw.equals("0") || raw.equals("false") || raw.equals("no"));
                }
            }

            StringBuilder missing = new StringBuilder();
            if (resolvedEndpoint.isEmpty()) {
                missing.append("\n    ").append(ENV_ENDPOINT);
            }
            if (resolvedTopicId.isEmpty()) {
                missing.append("\n    ").append(ENV_TOPIC_ID);
            }
            if (resolvedSecretId.isEmpty()) {
                missing.append("\n    ").append(ENV_SECRET_ID);
            }
            if (resolvedSecretKey.isEmpty()) {
                missing.append("\n    ").append(ENV_SECRET_KEY);
            }
            if (missing.length() > 0) {
                throw new IllegalStateException(
                        "CLS configuration incomplete, missing required field(s):" + missing
                                + "\n\n  Provide them via one of:\n"
                                + "    1. Environment variables: export CLS_ENDPOINT=... "
                                + "CLS_TOPIC_ID=... CLS_SECRET_ID=... CLS_SECRET_KEY=...\n"
                                + "    2. A .env file (./.env or ~/.agentobs-agentscope/config.env)\n"
                                + "    3. Builder args: ClsConfig.builder().endpoint(...)"
                                + ".topicId(...).secretId(...).secretKey(...).build()");
            }

            boolean resolvedCaptureFormatCalls = false;
            if (captureFormatCalls != null) {
                resolvedCaptureFormatCalls = captureFormatCalls;
            } else {
                String raw = resolve(null, ENV_CAPTURE_FORMAT_CALLS, envFile, "").toLowerCase();
                resolvedCaptureFormatCalls =
                        raw.equals("1") || raw.equals("true") || raw.equals("yes");
            }

            return new ClsConfig(resolvedEndpoint, resolvedTopicId, resolvedSecretId,
                    resolvedSecretKey, resolvedServiceName, resolvedSource, resolvedBatchSize,
                    resolvedDebug, resolvedCaptureFrameworkCalls,
                    resolvedPropagateAcrossSchedulers, resolvedCaptureFormatCalls);
        }
    }

    // ── resolution helpers ────────────────────────────────────────────────

    private static String resolve(String explicit, String envKey,
                                  Map<String, String> envFile, String defaultValue) {
        if (explicit != null && !explicit.isEmpty()) {
            return explicit;
        }
        String fromEnv = System.getenv(envKey);
        if (fromEnv != null && !fromEnv.isEmpty()) {
            return fromEnv;
        }
        String fromFile = envFile.get(envKey);
        if (fromFile != null && !fromFile.isEmpty()) {
            return fromFile;
        }
        return defaultValue;
    }

    private static String normalizeEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isEmpty()) {
            return "";
        }
        if (endpoint.contains("://")) {
            return endpoint;
        }
        return "https://" + endpoint;
    }

    /**
     * Load {@code KEY=value} pairs from the first existing candidate file.
     * Never throws — a missing/unreadable file yields an empty map.
     */
    private static Map<String, String> loadEnvFile() {
        Map<String, String> result = new LinkedHashMap<>();
        for (Path candidate : candidatePaths()) {
            if (candidate == null || !Files.isRegularFile(candidate)) {
                continue;
            }
            try (BufferedReader reader = Files.newBufferedReader(candidate, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    parseLine(line, result);
                }
                return result;
            } catch (IOException ignored) {
                // Try the next candidate.
            }
        }
        return result;
    }

    private static Path[] candidatePaths() {
        Path cwdEnv = Paths.get(System.getProperty("user.dir", "."), ".env");
        String home = System.getProperty("user.home");
        Path homeEnv = home != null
                ? Paths.get(home, ".agentobs-agentscope", "config.env")
                : null;
        return new Path[]{cwdEnv, homeEnv};
    }

    private static void parseLine(String line, Map<String, String> out) {
        String stripped = line.trim();
        if (stripped.isEmpty() || stripped.startsWith("#") || !stripped.contains("=")) {
            return;
        }
        int eq = stripped.indexOf('=');
        String key = stripped.substring(0, eq).trim();
        if (key.isEmpty()) {
            return;
        }
        String value = stripped.substring(eq + 1).trim();
        if (value.length() >= 2
                && value.charAt(0) == value.charAt(value.length() - 1)
                && (value.charAt(0) == '\'' || value.charAt(0) == '"')) {
            value = value.substring(1, value.length() - 1);
        }
        out.put(key, value);
    }

    private static String detectLocalIp() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }
}
