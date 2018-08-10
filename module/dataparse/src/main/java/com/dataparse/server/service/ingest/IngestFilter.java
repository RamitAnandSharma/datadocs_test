package com.dataparse.server.service.ingest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class IngestFilter {
    public Map<String, Object> filter(Map<String, Object> row) {
        if(row == null) {
            return null;
        }

        Map<String, Object> result = new HashMap<>(row.size());
        row.forEach((key, value) -> {
            if (value == null || shouldSkipValue(value)) {
                result.put(key, null);
                return;
            }
            result.put(key, value);
        });
        return result;
    }

    protected abstract Integer getValueLengthThreshold();
    protected abstract Integer getArrayLengthThreshold();

    private boolean shouldSkipValue(Object val) {
        if(val instanceof String) {
            return getStringLength(val) > getValueLengthThreshold();
        } else if(val instanceof List) {
            return ((List) val).size() > getArrayLengthThreshold();
        }
        return false;
    }

    private Integer getStringLength(Object obj) {
        return obj == null ? 0 : ((String) obj).length();
    }

}
