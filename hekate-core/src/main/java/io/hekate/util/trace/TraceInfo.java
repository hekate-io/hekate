package io.hekate.util.trace;

import java.util.HashMap;
import java.util.Map;

public class TraceInfo {
    private final String name;

    private Map<String, Object> tags;

    private TraceInfo(String name) {
        this.name = name;
    }

    public static TraceInfo extract(Object obj) {
        if (obj instanceof Traceable) {
            return ((Traceable)obj).traceInfo();
        } else {
            return null;
        }
    }

    public static TraceInfo of(String name) {
        return new TraceInfo(name);
    }

    public String name() {
        return name;
    }

    public Map<String, Object> tags() {
        return tags;
    }

    public TraceInfo withTag(String name, Object value) {
        if (tags == null) {
            tags = new HashMap<>();
        }

        tags.put(name, value);

        return this;
    }
}
