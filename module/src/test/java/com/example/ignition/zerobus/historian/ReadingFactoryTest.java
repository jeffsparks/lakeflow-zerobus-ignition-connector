package com.example.ignition.zerobus.historian;

import com.example.ignition.zerobus.proto.MetricReading;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ReadingFactoryTest {

    private final ReadingFactory factory = new ReadingFactory(ReadingFactory::scalarArrayJson);

    private MetricReading read(Object value) {
        return factory.create("m", 1_000_000L, 192, value);
    }

    @Test
    public void integersStayIntegers() {
        MetricReading r = read(42);
        assertTrue(r.hasValueInt());
        assertEquals(42L, r.getValueInt());
        assertFalse(r.hasValueDouble());
        assertEquals(Long.MAX_VALUE, read(Long.MAX_VALUE).getValueInt());
        assertTrue(read((short) 3).hasValueInt());
    }

    @Test
    public void onlyTheApplicableColumnIsSet() {
        MetricReading r = read(0.0);
        assertTrue(r.hasValueDouble());
        assertFalse(r.hasValueInt());
        assertFalse(r.hasValueBool());
        assertFalse(r.hasValueText());
        assertFalse(r.hasValueTs());
        assertFalse(r.hasValueBytes());
        assertFalse(r.hasValueJson());
    }

    @Test
    public void zeroFalseAndEmptyStringAreRealValues() {
        assertTrue(read(0).hasValueInt());
        assertTrue(read(false).hasValueBool());
        assertFalse(read(false).getValueBool());
        assertTrue(read("").hasValueText());
    }

    @Test
    public void nullValueIsValueLess() {
        MetricReading r = read(null);
        assertNotNull(r);
        assertFalse(r.hasValueInt() || r.hasValueDouble() || r.hasValueBool() || r.hasValueText()
                || r.hasValueTs() || r.hasValueBytes() || r.hasValueJson());
        assertEquals(192L, r.getQuality());
    }

    @Test
    public void qualityKeepsAllThirtyTwoBits() {
        int bad = 0x80000200;
        assertEquals(2147484160L, factory.create("m", 0L, bad, 1).getQuality());
    }

    @Test
    public void timestampsBytesAndStructuredValues() {
        assertEquals(5_000L, read(new Date(5)).getValueTs());
        assertArrayEquals(new byte[] {1, 2}, read(new byte[] {1, 2}).getValueBytes().toByteArray());
        assertEquals("[1,2,null]", read(new Integer[] {1, 2, null}).getValueJson());
        assertEquals("[\"a\\\"b\",true]", read(List.of("a\"b", true)).getValueJson());
        assertEquals("[1.5]", read(new double[] {1.5}).getValueJson());
    }

    @Test
    public void unsupportedValuesAreReportedNotCoerced() {
        assertNull(read(new Object()));
        assertNull(read(new java.math.BigDecimal("1.5")));
    }

    @Test
    public void coerceRestoresIntegersOnlyForIntegerDataTypes() {
        assertEquals(7L, ReadingFactory.coerceToDataType(7.0, "Int4"));
        assertEquals(7.0, ReadingFactory.coerceToDataType(7.0, "Float8"));
        assertEquals(7.5, ReadingFactory.coerceToDataType(7.5, "Int4"));
        assertEquals("x", ReadingFactory.coerceToDataType("x", "Int4"));
    }
}
