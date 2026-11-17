package com.example.ignition.zerobus.historian;

import java.util.Objects;

/**
 * An Ignition tag path split into its provider and the provider-free logical path.
 *
 * "[default]Site/Area/Tag" -> provider "default", path "Site/Area/Tag".
 * A path without a "[provider]" prefix has a null provider.
 */
public final class TagPathParts {
    private final String provider;
    private final String path;

    private TagPathParts(String provider, String path) {
        this.provider = provider;
        this.path = path;
    }

    public static TagPathParts parse(String fullPath) {
        String s = fullPath == null ? "" : fullPath;
        if (s.startsWith("[")) {
            int end = s.indexOf(']');
            if (end > 0) {
                return new TagPathParts(s.substring(1, end), s.substring(end + 1));
            }
        }
        return new TagPathParts(null, s);
    }

    public String getProvider() {
        return provider;
    }

    public String getPath() {
        return path;
    }

    public String toFullPath() {
        return provider == null ? path : "[" + provider + "]" + path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TagPathParts)) return false;
        TagPathParts that = (TagPathParts) o;
        return Objects.equals(provider, that.provider) && path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, path);
    }

    @Override
    public String toString() {
        return toFullPath();
    }
}
