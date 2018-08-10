package com.dataparse.server.service.bigquery.cache.serialization;

import com.google.cloud.bigquery.*;
import lombok.*;

import java.io.*;
import java.util.*;

@Data
public class BigQueryResult<T> implements Serializable {

    @Data
    @AllArgsConstructor
    public static class Field implements Serializable {
        String name;
        LegacySQLTypeName type;
    }

    private String tableId;
    private List<Field> fields;
    private long totalRows;
    private T values;
    private BigQueryRequest request;

    protected BigQueryResult(final List<Field> fields,
                           final long totalRows,
                           final T values) {
        this.fields = fields;
        this.totalRows = totalRows;
        this.values = values;
    }

}
