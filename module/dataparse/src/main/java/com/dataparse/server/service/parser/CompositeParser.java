package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.upload.CompositeDescriptor;
import com.dataparse.server.service.upload.Descriptor;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;
import static com.google.common.base.Preconditions.checkNotNull;

public class CompositeParser extends Parser {

    private List<Parser> parsers;

    private Descriptor descriptor;

    public CompositeParser(CompositeDescriptor descriptor, List<Parser> parsers) {
        this.descriptor = descriptor;
        this.parsers = parsers;
    }

    @Override
    public RecordIterator parse() throws IOException {

        List<RecordIterator> iterators = new ArrayList<>();
        for(Parser parser : parsers) {
            iterators.add(parser.parse());
        }
        Iterator<RecordIterator> inputs = iterators.iterator();

        return with(new RecordIterator() {
            RecordIterator current = RecordIterator.emptyIterator();
            RecordIterator removeFrom;

            @Override
            public boolean hasNext() {
                boolean currentHasNext;
                while (!(currentHasNext = checkNotNull(current).hasNext())
                        && inputs.hasNext()) {
                    current = inputs.next();
                }
                return currentHasNext;
            }

            @Override
            public Map<AbstractParsedColumn, Object> getRaw() {
                return current.getRaw();
            }

            @Override
            public Map<AbstractParsedColumn, Object> next() {
                if (!hasNext()) {
                    return null;
                }
                removeFrom = current;
                return current.next();
            }

            @Override
            public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
                return current.getSchema();
            }

            @Override
            public long getBytesCount() {
                return -1; // not implemented
            }

            @Override
            public void close() throws IOException {
                for(RecordIterator iterator : iterators){
                    iterator.close();
                }
            }
        })
                .withTransforms(descriptor.getColumnTransforms())
                .withColumns(descriptor.getColumns())
                .interruptible()
                .build();
    }

    @Override
    public Pair<Long, Boolean> getRowsEstimateCount(long fileSize) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void close() throws IOException {
        for(Parser parser : parsers){
            parser.close();
        }
    }
}
