package com.cascada.app.adapter.in.api;

import java.math.BigDecimal;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;

/** Writes each visited cell into the current Parquet record; NULL cells are simply left unset. */
final class ParquetCellWriter implements ResultCellVisitor<RuntimeException> {
    private final int[] scales;
    private Group record;
    private int field;

    ParquetCellWriter(int[] scales) { this.scales = scales; }

    void startRecord(Group record) { this.record = record; }
    void field(int field) { this.field = field; }
    Group record() { return record; }

    public void nullValue() { }
    public void longValue(long value) { record.add(field, value); }
    public void doubleValue(double value) { record.add(field, value); }
    public void stringValue(String value) { record.add(field, value); }
    public void decimalValue(BigDecimal value) {
        record.add(field, Binary.fromConstantByteArray(value.setScale(scales[field]).unscaledValue().toByteArray()));
    }
}
