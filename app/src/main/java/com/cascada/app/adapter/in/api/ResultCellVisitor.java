package com.cascada.app.adapter.in.api;

import java.math.BigDecimal;

/** Receives one result cell's value in its storage type; see {@link ResultCells#visit}. */
interface ResultCellVisitor<E extends Exception> {
    void nullValue() throws E;
    void longValue(long value) throws E;
    void doubleValue(double value) throws E;
    void stringValue(String value) throws E;
    void decimalValue(BigDecimal value) throws E;
}
