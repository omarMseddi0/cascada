package com.cascada.app.adapter.in.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

final class ParquetRowWriterBuilder extends ParquetWriter.Builder<Integer, ParquetRowWriterBuilder> {
    private final ParquetCellWriter support;

    ParquetRowWriterBuilder(OutputFile file, ParquetCellWriter support) {
        super(file);
        this.support = support;
    }

    @Override protected ParquetRowWriterBuilder self() {
        return this;
    }

    @Override protected WriteSupport<Integer> getWriteSupport(Configuration configuration) {
        return support;
    }
}
