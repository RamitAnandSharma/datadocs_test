package com.dataparse.server.service.flow.ingest;

import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.tasks.ExecutionException;
import com.dataparse.server.service.visualization.bookmark_state.state.IngestErrorMode;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class IngestPreprocessor {

    public static void withErrorsHandling(Map<AbstractParsedColumn, Object> o, IngestErrorMode ingestErrorMode,
                                          Consumer<ErrorValue> errorsCallback,
                                          Consumer<Map<AbstractParsedColumn, Object>> consumer) {
        boolean skipRow = false;

        main:
        for (AbstractParsedColumn key : o.keySet()) {
            Object value = o.get(key);
            if (value instanceof ErrorValue) {
                switch (ingestErrorMode) {
                    case SKIP_ROW:
                        errorsCallback.accept((ErrorValue) value);
                        skipRow = true;
                        break main;
                    case REPLACE_WITH_NULL:
                        o.put(key, null);
                        errorsCallback.accept((ErrorValue) value);
                        break;
                    case STOP_INGEST:
                        throw ExecutionException.of("stop_on_error", "Ingest stopped due to errors", ((ErrorValue) value).getDescription());
                }
            } else if (value instanceof List) {
                List list = ((List) value);
                for (int i = 0; i < list.size(); i++) {
                    Object listValue = list.get(i);
                    if (listValue instanceof ErrorValue) {
                        switch (ingestErrorMode) {
                            case SKIP_ROW:
                                errorsCallback.accept((ErrorValue) listValue);
                                skipRow = true;
                                break main;
                            case REPLACE_WITH_NULL:
                                errorsCallback.accept((ErrorValue) listValue);
                                list.set(i, null);
                                break;
                            case STOP_INGEST:
                                throw ExecutionException.of("stop_on_error", "Ingest stopped due to errors", ((ErrorValue) listValue).getDescription());
                        }

                    }
                }
            }
        }
        if (!skipRow) {
            consumer.accept(o);
        }
    }

}
