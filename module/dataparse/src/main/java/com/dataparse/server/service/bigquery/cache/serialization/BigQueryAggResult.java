package com.dataparse.server.service.bigquery.cache.serialization;

import com.dataparse.server.service.visualization.*;
import lombok.*;

import java.io.*;
import java.util.*;

public class BigQueryAggResult extends BigQueryResult<BigQueryAggResult.AggResult> {

    @Data
    public static class AggResult implements Serializable {
        private Tree<Map<String, Object>> rows;
        private Tree<Map<String, Object>> cols;

        private AggResult(
                final Tree<Map<String, Object>> rows,
                final Tree<Map<String, Object>> cols) {
            this.rows = rows;
            this.cols = cols;
        }

        public static AggResult of(Tree<Map<String, Object>> rows, Tree<Map<String, Object>> cols){
            return new AggResult(rows, cols);
        }
    }

    public BigQueryAggResult(
            final List<Field> fields, final long totalRows,
            final AggResult values) {
        super(fields, totalRows, values);
    }

}
