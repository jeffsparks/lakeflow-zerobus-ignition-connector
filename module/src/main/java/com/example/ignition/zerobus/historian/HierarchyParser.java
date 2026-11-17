package com.example.ignition.zerobus.historian;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Splits a provider-free tag path into descriptive hierarchy levels and the device_tag_name.
 *
 * The last segment is always part of device_tag_name. At most one leading segment per configured label is
 * consumed as a hierarchy level; all remaining segments form device_tag_name. Shallow paths yield fewer levels.
 */
public final class HierarchyParser {
    private final List<String> labels;

    public HierarchyParser(List<String> labels) {
        this.labels = labels == null ? List.of() : List.copyOf(labels);
    }

    public Result parse(String path) {
        String[] segments = path.split("/", -1);
        int consumed = Math.min(labels.size(), segments.length - 1);
        List<Level> levels = new ArrayList<>(Math.max(0, consumed));
        for (int i = 0; i < consumed; i++) {
            levels.add(new Level(labels.get(i), segments[i]));
        }
        String deviceTagName = String.join("/", Arrays.copyOfRange(segments, Math.max(0, consumed), segments.length));
        return new Result(Collections.unmodifiableList(levels), deviceTagName);
    }

    public List<String> getLabels() {
        return labels;
    }

    public static final class Level {
        public final String name;
        public final String value;

        Level(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }

    public static final class Result {
        public final List<Level> levels;
        public final String deviceTagName;

        Result(List<Level> levels, String deviceTagName) {
            this.levels = levels;
            this.deviceTagName = deviceTagName;
        }
    }
}
