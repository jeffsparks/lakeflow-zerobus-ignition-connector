package com.example.ignition.zerobus.historian;

import com.example.ignition.zerobus.proto.MetricReading;
import com.google.protobuf.ByteString;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

/**
 * Builds {@link MetricReading}s, routing each value to exactly one typed column.
 *
 * Integers stay integers, floating-point values go to value_double, and structured values (arrays, datasets,
 * documents) go to value_json through a {@link JsonEncoder}. A null value produces a value-less reading.
 * Values with no supported mapping are reported rather than coerced.
 */
public final class ReadingFactory {

    /** Encodes structured values as JSON; returns null when the value is not supported. */
    public interface JsonEncoder {
        String toJson(Object value);
    }

    private static final Set<String> INTEGER_DATA_TYPES = Set.of("Int1", "Int2", "Int4", "Int8");

    private final JsonEncoder jsonEncoder;

    public ReadingFactory(JsonEncoder jsonEncoder) {
        this.jsonEncoder = jsonEncoder;
    }

    /**
     * @param qualityCode the source quality code; stored as its unsigned 32-bit value
     * @return the reading, or null when the value type is unsupported
     */
    public MetricReading create(String metricId, long timeMicros, int qualityCode, Object value) {
        MetricReading.Builder b = MetricReading.newBuilder()
                .setMetricId(metricId)
                .setTime(timeMicros)
                .setQuality(Integer.toUnsignedLong(qualityCode));
        return setValue(b, value) ? b.build() : null;
    }

    private boolean setValue(MetricReading.Builder b, Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof Boolean) {
            b.setValueBool((Boolean) value);
        } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            b.setValueInt(((Number) value).longValue());
        } else if (value instanceof Double || value instanceof Float) {
            b.setValueDouble(((Number) value).doubleValue());
        } else if (value instanceof String) {
            b.setValueText((String) value);
        } else if (value instanceof Character) {
            b.setValueText(value.toString());
        } else if (value instanceof Date) {
            b.setValueTs(((Date) value).getTime() * 1000L);
        } else if (value instanceof byte[]) {
            b.setValueBytes(ByteString.copyFrom((byte[]) value));
        } else {
            String json = jsonEncoder == null ? null : jsonEncoder.toJson(value);
            if (json == null) {
                return false;
            }
            b.setValueJson(json);
        }
        return true;
    }

    /**
     * Restores integer values that a JSON transport delivered as doubles, using the binding's Ignition data type.
     */
    public static Object coerceToDataType(Object value, String dataType) {
        if (value instanceof Double && dataType != null && INTEGER_DATA_TYPES.contains(dataType)) {
            double d = (Double) value;
            if (d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) <= 0x1p53) {
                return (long) d;
            }
        }
        return value;
    }

    /** Typical JSON encoder for arrays and lists of scalars; other values are unsupported. */
    public static String scalarArrayJson(Object value) {
        Object[] items;
        if (value instanceof Object[]) {
            items = (Object[]) value;
        } else if (value instanceof Collection) {
            items = ((Collection<?>) value).toArray();
        } else if (value != null && value.getClass().isArray()) {
            int n = Array.getLength(value);
            items = new Object[n];
            for (int i = 0; i < n; i++) {
                items[i] = Array.get(value, i);
            }
        } else {
            return null;
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < items.length; i++) {
            if (i > 0) sb.append(',');
            Object item = items[i];
            if (item == null) {
                sb.append("null");
            } else if (item instanceof Boolean || item instanceof Long || item instanceof Integer
                    || item instanceof Short || item instanceof Byte) {
                sb.append(item);
            } else if (item instanceof Double || item instanceof Float) {
                double d = ((Number) item).doubleValue();
                if (Double.isNaN(d) || Double.isInfinite(d)) {
                    sb.append('"').append(d).append('"');
                } else {
                    sb.append(item);
                }
            } else if (item instanceof String || item instanceof Character) {
                appendJsonString(sb, item.toString());
            } else if (item instanceof Date) {
                sb.append(((Date) item).getTime());
            } else {
                return null;
            }
        }
        return sb.append(']').toString();
    }

    private static void appendJsonString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }
}
