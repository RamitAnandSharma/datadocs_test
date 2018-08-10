package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.TypeDescriptor;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * todo RecordIterator interface should have 'peek' method itself
 */
public class PeekingRecordIteratorWrapper implements RecordIterator {

    private final RecordIterator iterator;
    private boolean hasPeeked;
    private Map<AbstractParsedColumn, Object> peekedElement;

    public PeekingRecordIteratorWrapper(RecordIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return hasPeeked || iterator.hasNext();
    }

    @Override
    public Map<AbstractParsedColumn, Object> getRaw() {
        return iterator.getRaw();
    }

    @Override
    public Map<AbstractParsedColumn, Object> next() {
        if (!hasPeeked) {
            return iterator.next();
        }
        Map<AbstractParsedColumn, Object> result = peekedElement;
        hasPeeked = false;
        peekedElement = null;
        return result;
    }

    @Override
    public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
        return iterator.getSchema();
    }

    @Override
    public void remove() {
        checkState(!hasPeeked, "Can't remove after you've peeked at next");
        iterator.remove();
    }

    public Map<AbstractParsedColumn, Object> peek() {
        if (!hasPeeked) {
            peekedElement = iterator.next();
            hasPeeked = true;
        }
        return peekedElement;
    }

    @Override
    public void close() throws IOException {
        this.iterator.close();
    }

    @Override
    public long getBytesCount() {
        return this.iterator.getBytesCount();
    }
}
