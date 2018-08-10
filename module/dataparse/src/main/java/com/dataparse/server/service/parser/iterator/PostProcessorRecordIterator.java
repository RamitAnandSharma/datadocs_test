package com.dataparse.server.service.parser.iterator;

import com.dataparse.server.service.flow.transform.ColumnTransform;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PostProcessorRecordIterator extends FilterRecordIterator {

    private List<ColumnTransform> transforms;

    PostProcessorRecordIterator(RecordIterator it, List<ColumnTransform> transforms) {
        super(it);
        this.transforms = transforms;
    }

    @Override
    public Map<AbstractParsedColumn, Object> next() {
        Map<AbstractParsedColumn, Object> o = it.next();
        for (ColumnTransform transform : transforms) {
            o = transform.apply(new LinkedHashMap<>(o));
        }
        return o;
    }
}
