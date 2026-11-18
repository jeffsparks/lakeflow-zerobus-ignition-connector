package com.example.ignition.zerobus.historian;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Deterministic historian metric identity.
 *
 * Contract (must not change once records exist):
 * <ul>
 *   <li>metric_id = lowercase hex SHA-1 of the UTF-8 bytes of identity_key.</li>
 *   <li>The metric name is the provider-free tag path, or the part below the folder-mode root when the root
 *       folder is excluded from names.</li>
 *   <li>name mode: identity_key = the metric name, exactly as configured (case-sensitive).</li>
 *   <li>source mode: identity_key = scope + "::" + source_ref, where scope is the first four
 *       non-leaf segments of the metric name joined by "/" (fewer when it is shallower) and source_ref is the
 *       value of the source reference property (opcItemPath unless another tag property is configured) exactly
 *       as configured.</li>
 * </ul>
 * Source mode applies only when the source-keyed rule is enabled, its regex matches the full tag path
 * (including the "[provider]" prefix, using {@link java.util.regex.Matcher#find()}), and a non-blank
 * source reference exists. Otherwise name mode applies.
 */
public final class MetricIdentity {
    public static final String MODE_SOURCE = "source";
    public static final String MODE_NAME = "name";

    static final int SOURCE_SCOPE_LEVELS = 4;
    static final String SOURCE_SEPARATOR = "::";

    private final String metricId;
    private final String identityMode;
    private final String identityKey;

    private MetricIdentity(String identityMode, String identityKey) {
        this.identityMode = identityMode;
        this.identityKey = identityKey;
        this.metricId = sha1Hex(identityKey);
    }

    public static MetricIdentity resolve(TagPathParts tag, String sourceRef, SourceKeyedRule rule) {
        return resolve(tag, tag.getPath(), sourceRef, rule);
    }

    /** @param name the metric name; the source-keyed regex is still matched against the full tag path */
    public static MetricIdentity resolve(TagPathParts tag, String name, String sourceRef, SourceKeyedRule rule) {
        Objects.requireNonNull(tag, "tag");
        Objects.requireNonNull(name, "name");
        if (rule != null && rule.matches(tag.toFullPath()) && sourceRef != null && !sourceRef.isBlank()) {
            return new MetricIdentity(MODE_SOURCE, sourceScope(name) + SOURCE_SEPARATOR + sourceRef);
        }
        return new MetricIdentity(MODE_NAME, name);
    }

    static String sourceScope(String path) {
        String[] segments = path.split("/", -1);
        int levels = Math.min(SOURCE_SCOPE_LEVELS, segments.length - 1);
        return levels <= 0 ? "" : String.join("/", Arrays.copyOfRange(segments, 0, levels));
    }

    static String sha1Hex(String s) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 unavailable", e);
        }
    }

    public String getMetricId() {
        return metricId;
    }

    public String getIdentityMode() {
        return identityMode;
    }

    public String getIdentityKey() {
        return identityKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricIdentity)) return false;
        MetricIdentity that = (MetricIdentity) o;
        return identityMode.equals(that.identityMode) && identityKey.equals(that.identityKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identityMode, identityKey);
    }

    @Override
    public String toString() {
        return identityMode + ":" + identityKey;
    }

    /**
     * The user-configured source-keyed selection rule. An invalid regex is retained as an error so it can be
     * reported; while invalid, the rule matches nothing (every tag falls back to name mode).
     */
    public static final class SourceKeyedRule {
        private final boolean enabled;
        private final Pattern pattern;
        private final String error;

        private SourceKeyedRule(boolean enabled, Pattern pattern, String error) {
            this.enabled = enabled;
            this.pattern = pattern;
            this.error = error;
        }

        public static SourceKeyedRule compile(boolean enabled, String regex) {
            if (!enabled) {
                return new SourceKeyedRule(false, null, null);
            }
            if (regex == null || regex.isEmpty()) {
                return new SourceKeyedRule(true, null, "source-keyed tag regex is empty");
            }
            try {
                return new SourceKeyedRule(true, Pattern.compile(regex), null);
            } catch (PatternSyntaxException e) {
                return new SourceKeyedRule(true, null, "invalid source-keyed tag regex: " + e.getDescription());
            }
        }

        public boolean matches(String fullTagPath) {
            return enabled && pattern != null && pattern.matcher(fullTagPath).find();
        }

        public boolean isEnabled() {
            return enabled;
        }

        /** @return a configuration error, or null when the rule is valid or disabled. */
        public String getError() {
            return error;
        }
    }
}
