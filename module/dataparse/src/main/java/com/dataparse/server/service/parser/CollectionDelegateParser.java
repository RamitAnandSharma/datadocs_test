package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.CollectionDelegateRecordIterator;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.upload.CollectionDelegateDescriptor;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;

public class CollectionDelegateParser extends Parser {

    private CollectionDelegateDescriptor descriptor;

    public CollectionDelegateParser(CollectionDelegateDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public Pair<Long, Boolean> getRowsEstimateCount(long fileSize) {
        return Pair.of((long) Iterables.size(descriptor.getIterable()), true);
    }

    @Override
    public RecordIterator parse() throws IOException {
        Iterator<Map<AbstractParsedColumn, Object>> delegate = descriptor.getIterable().iterator() ;
        return with(new CollectionDelegateRecordIterator(delegate))
                .limited(descriptor.getLimit())
                .withTransforms(descriptor.getColumnTransforms())
                .withColumns(descriptor.getColumns())
                .interruptible()
                .build();
    }
}
