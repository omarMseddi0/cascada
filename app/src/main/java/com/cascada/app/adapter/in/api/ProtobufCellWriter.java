package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.Cell;
import java.math.BigDecimal;

/** Sets each visited value on the current protobuf cell; a NULL cell carries no value. */
final class ProtobufCellWriter implements ResultCellVisitor<RuntimeException> {
    private final Cell.Builder cell = Cell.newBuilder();

    void startCell() { cell.clear(); }
    Cell cell() { return cell.build(); }

    public void nullValue() { }
    public void longValue(long value) { cell.setLongValue(value); }
    public void doubleValue(double value) { cell.setDoubleValue(value); }
    public void stringValue(String value) { cell.setStringValue(value); }
    public void decimalValue(BigDecimal value) { cell.setDecimalValue(value.toString()); }
}
