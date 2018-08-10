package com.dataparse.server.service.flow.transform;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.util.ListUtils;
import lombok.Data;

import java.util.Map;

@Data
public abstract class ColumnTransform implements Transform {

    private AbstractParsedColumn columnName;

    public Map<AbstractParsedColumn, Object> apply(Map<AbstractParsedColumn, Object> o) {
        if(o != null) {
            Object value = o.get(columnName);
            o.put(columnName, ListUtils.apply(value, this::apply));
        }
        return o;
    }

    abstract protected Object apply(Object o);
}
