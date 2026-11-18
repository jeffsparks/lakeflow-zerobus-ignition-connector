package com.example.ignition.zerobus;

import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.common.browsing.Results;
import com.inductiveautomation.ignition.common.tags.browsing.NodeDescription;
import com.inductiveautomation.ignition.common.tags.config.types.TagObjectType;
import com.inductiveautomation.ignition.common.tags.model.TagPath;
import com.inductiveautomation.ignition.common.tags.paths.parser.TagPathParser;
import com.inductiveautomation.ignition.gateway.tags.model.GatewayTagManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Resolves the configured tag selection (explicit, folder, or pattern) to atomic tag paths.
 */
public final class TagSelectionResolver {
    private static final Logger logger = LoggerFactory.getLogger(TagSelectionResolver.class);
    private static final int PAGE_SIZE = 5000;

    private final ConfigModel config;
    private final int maxBrowseNodes;
    private final int maxPatternMatches;

    public TagSelectionResolver(ConfigModel config, int maxBrowseNodes, int maxPatternMatches) {
        this.config = config;
        this.maxBrowseNodes = maxBrowseNodes;
        this.maxPatternMatches = maxPatternMatches;
    }

    /**
     * Result of a resolution. {@code complete} is false when browsing failed or stopped at a safety cap, in which
     * case the paths are only part of the selection.
     */
    public static final class Selection {
        public final List<TagPath> paths;
        public final boolean complete;

        Selection(List<TagPath> paths, boolean complete) {
            this.paths = paths;
            this.complete = complete;
        }
    }

    public Selection resolve(GatewayTagManager tm) {
        String mode = config.getTagSelectionMode() != null ? config.getTagSelectionMode().trim() : "explicit";

        if ("explicit".equalsIgnoreCase(mode)) {
            return resolveExplicit();
        }
        if ("folder".equalsIgnoreCase(mode)) {
            return resolveFolder(tm);
        }
        if ("pattern".equalsIgnoreCase(mode)) {
            return resolvePattern(tm);
        }

        logger.warn("Unknown tagSelectionMode='{}'. Falling back to explicit.", mode);
        return resolveExplicit();
    }

    private Selection resolveExplicit() {
        List<String> paths = config.getExplicitTagPaths();
        if (paths == null || paths.isEmpty()) {
            logger.warn("No explicitTagPaths configured; nothing to subscribe to");
            return new Selection(List.of(), true);
        }
        List<TagPath> out = new ArrayList<>();
        for (String pathStr : paths) {
            if (pathStr == null || pathStr.trim().isEmpty()) {
                continue;
            }
            try {
                out.add(TagPathParser.parse(pathStr.trim()));
            } catch (Exception parseErr) {
                logger.warn("Invalid tag path '{}': {}", pathStr, parseErr.getMessage());
            }
        }
        return new Selection(out, true);
    }

    private Selection resolveFolder(GatewayTagManager tm) {
        String folder = config.getTagFolderPath();
        if (folder == null || folder.trim().isEmpty()) {
            logger.warn("Tag folder path is empty; nothing to subscribe to");
            return new Selection(List.of(), true);
        }

        TagPath root;
        try {
            root = TagPathParser.parse(folder.trim());
        } catch (Exception e) {
            logger.warn("Invalid folder tag path '{}': {}", folder, e.getMessage());
            return new Selection(List.of(), false);
        }

        boolean recursive = config.isIncludeSubfolders();
        Selection out = browseLeafAtomicTags(tm, root, recursive);
        logger.info("Folder selection resolved {} tag(s) from root={} (includeSubfolders={})",
                out.paths.size(), folder.trim(), recursive);
        return out;
    }

    private Selection resolvePattern(GatewayTagManager tm) {
        String patternStr = config.getTagPathPattern();
        if (patternStr == null || patternStr.trim().isEmpty()) {
            logger.warn("Tag path pattern is empty; nothing to subscribe to");
            return new Selection(List.of(), true);
        }

        Pattern p;
        try {
            p = Pattern.compile(patternStr);
        } catch (PatternSyntaxException e) {
            logger.warn("Invalid regex tagPathPattern='{}': {}", patternStr, e.getDescription());
            return new Selection(List.of(), false);
        }

        // Pattern mode is evaluated over the FULL tag path string (e.g., "[renewables]Renewables/Site01/MetMast01/WindSpeed_mps").
        //
        // IMPORTANT:
        // Naively browsing ALL providers can be slow (or effectively "hang") on gateways with large tag trees.
        // If the pattern clearly restricts provider(s) at the start (e.g. "^\[(a|b)\]..."), we can browse
        // only those providers for a big performance win.
        List<String> providers = tryExtractProviderFilter(patternStr);
        if (providers == null || providers.isEmpty()) {
            try {
                providers = tm.getTagProviderNames();
            } catch (Exception e) {
                logger.warn("Unable to list tag providers for pattern mode: {}", e.getMessage());
                return new Selection(List.of(), false);
            }
            logger.info("Pattern selection browsing ALL providers ({}). pattern='{}'", providers.size(), patternStr.trim());
        } else {
            logger.info("Pattern selection browsing filtered providers ({}): {}. pattern='{}'",
                    providers.size(), providers, patternStr.trim());
        }

        List<TagPath> out = new ArrayList<>();
        boolean complete = true;

        for (String provider : providers) {
            if (provider == null || provider.isBlank()) {
                continue;
            }
            TagPath providerRoot;
            try {
                providerRoot = TagPathParser.parse("[" + provider + "]");
            } catch (Exception ignored) {
                continue;
            }

            long t0 = System.currentTimeMillis();
            Selection leafs = browseLeafAtomicTags(tm, providerRoot, true);
            complete &= leafs.complete;
            long t1 = System.currentTimeMillis();
            if (config.isDebugLogging()) {
                logger.info("Pattern browse provider='{}': leafAtomicTags={} ({}ms)", provider, leafs.paths.size(), (t1 - t0));
            }
            for (TagPath tp : leafs.paths) {
                if (tp == null) continue;
                String s = tp.toString();
                if (p.matcher(s).matches()) {
                    out.add(tp);
                    if (out.size() >= maxPatternMatches) {
                        logger.warn("Pattern selection reached safety cap ({} matches). Stopping early. pattern={}",
                                maxPatternMatches, patternStr.trim());
                        return new Selection(out, false);
                    }
                }
            }
            if (config.isDebugLogging()) {
                logger.info("Pattern match progress after provider='{}': totalMatches={}", provider, out.size());
            }
        }

        logger.info("Pattern selection resolved {} tag(s) for pattern='{}'", out.size(), patternStr.trim());
        return new Selection(out, complete);
    }

    /**
     * Best-effort: if the regex begins with a provider constraint like:
     *   ^\[(a|b|c)\]...
     *   ^\[a\]...
     *
     * then return ["a","b","c"] so we only browse those providers.
     *
     * If the pattern doesn't match this shape (or uses complex regex), return null.
     */
    static List<String> tryExtractProviderFilter(String patternStr) {
        if (patternStr == null) {
            return null;
        }
        String s = patternStr.trim();
        if (s.startsWith("^")) {
            s = s.substring(1);
        }
        if (!s.startsWith("\\[")) {
            return null;
        }
        int end = s.indexOf("\\]");
        if (end < 0) {
            return null;
        }
        String inside = s.substring(2, end); // between \[ and \]
        if (inside.startsWith("(") && inside.endsWith(")") && inside.length() >= 2) {
            inside = inside.substring(1, inside.length() - 1);
        }
        // Only accept a simple alternation of "provider-ish" tokens.
        // If it contains other regex metacharacters, we can't safely infer providers.
        if (!inside.matches("[A-Za-z0-9_\\-\\|]+")) {
            return null;
        }
        String[] parts = inside.split("\\|");
        if (parts.length == 0) {
            return null;
        }
        return Arrays.asList(parts);
    }

    private Selection browseLeafAtomicTags(GatewayTagManager tm, TagPath root, boolean recursive) {
        if (root == null) {
            return new Selection(List.of(), true);
        }
        if (tm == null) {
            logger.warn("Cannot browse tags because GatewayTagManager is not initialized (root={})", root.toString());
            return new Selection(List.of(), false);
        }

        List<TagPath> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        String continuation = null;
        int totalSeen = 0;
        int page = 0;
        long startMs = System.currentTimeMillis();

        do {
            page++;
            BrowseFilter filter = new BrowseFilter()
                    .setRecursive(recursive)
                    .setMaxResults(PAGE_SIZE)
                    .setContinuationPoint(continuation);

            CompletableFuture<Results<NodeDescription>> fut = tm.browseAsync(root, filter);
            Results<NodeDescription> res;
            try {
                // Browsing is local; use requestTimeout as a reasonable ceiling.
                res = fut.get(Math.max(250L, config.getRequestTimeoutMs()), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.warn("Browse failed for root={} (recursive={}): {}", root.toString(), recursive, e.getMessage());
                return new Selection(out, false);
            }

            Collection<NodeDescription> nodes = res != null ? res.getResults() : null;
            int nodeCount = nodes == null ? 0 : nodes.size();
            int newNodes = 0;
            if (nodes != null) {
                for (NodeDescription nd : nodes) {
                    totalSeen++;
                    if (totalSeen >= maxBrowseNodes) {
                        logger.warn("Browse reached safety cap ({} nodes) for root={}; stopping early", maxBrowseNodes, root.toString());
                        return new Selection(out, false);
                    }
                    if (nd == null) continue;
                    TagPath fp = nd.getFullPath();
                    if (fp == null || !seen.add(fp.toString())) continue;
                    newNodes++;
                    if (nd.getObjectType() == TagObjectType.AtomicTag) {
                        out.add(fp);
                    }
                }
            }

            continuation = (res != null) ? res.getContinuationPoint() : null;
            boolean more = continuation != null && !continuation.isBlank();

            if (config.isDebugLogging()) {
                long elapsed = System.currentTimeMillis() - startMs;
                logger.info("Browse progress root={} page={} nodes={} new={} atomic={} totalSeen={} cont={} elapsedMs={}",
                        root.toString(), page, nodeCount, newNodes, out.size(), totalSeen,
                        (continuation == null ? "null" : (continuation.isBlank() ? "<blank>" : "<set>")),
                        elapsed);
            }

            if (more && newNodes == 0) {
                // Some providers (e.g. MQTT Engine) keep returning a continuation point after the last page and
                // repeat nodes already returned; that is the end of the browse. A page with no nodes at all while
                // more are promised is treated as an incomplete browse.
                if (nodeCount == 0) {
                    logger.warn("Browse returned 0 nodes but continuationPoint is set for root={}; stopping early", root.toString());
                    return new Selection(out, false);
                }
                break;
            }
            if (!more) {
                break;
            }
        } while (true);

        return new Selection(out, true);
    }
}
