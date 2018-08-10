package com.dataparse.server.service.parser.column;

import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnsUtils {
    public static Map<AbstractParsedColumn, Object> namedColumnsFromMap(Map<String, Object> row) {
        if(row == null) {
             return null;
        }
        Map<AbstractParsedColumn, Object> result = new LinkedHashMap<>(row.size());
        row.forEach((key, val) -> result.put(new NamedParsedColumn(key), val));
        return result;
    }

}
