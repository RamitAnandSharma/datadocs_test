package com.dataparse.server.service.bigquery.cache.serialization;

import com.google.cloud.bigquery.*;

import java.util.*;
import java.util.stream.*;

public class BigQueryRawResult extends BigQueryResult<List<List<Object>>> {

    public BigQueryRawResult(
            final List<Field> fields, final long totalRows, final List<List<Object>> values) {
        super(fields, totalRows, values);
    }

    public static BigQueryRawResult of(final List<Field> fields,
                                       final long totalRows,
                                       final List<List<Object>> values){
        return new BigQueryRawResult(fields, totalRows, values);
    }

    public static BigQueryRawResult getResult(QueryResult result, boolean singlePage){
        QueryResult originalResult = result;
        List<List<Object>> iterable = new ArrayList<>();
        do {
            result.getValues().forEach(row -> {
                List<Object> newRow = new ArrayList<>(row.size());
                for (FieldValue value : row) {
                    newRow.add(value.getValue());
                }
                iterable.add(newRow);
            });
        } while (!singlePage && (result = result.getNextPage()) != null);

        return new BigQueryRawResult(originalResult
                                          .getSchema()
                                          .getFields()
                                          .stream()
                                          .map(f -> new Field(f.getName(), f.getType().getValue()))
                                          .collect(Collectors.toList()),
                                     originalResult.getTotalRows(),
                                     iterable);
    }

    public static BigQueryRawResult ofAllPages(QueryResult result){
        return getResult(result, false);
    }

    public static BigQueryRawResult ofSinglePage(QueryResult result) {
        return getResult(result, true);
    }

}
