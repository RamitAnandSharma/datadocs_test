package com.dataparse.server.service.parser.iterator;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.google.common.collect.Iterators;
import lombok.Data;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class BufferedRecordIterator extends FilterRecordIterator {

    @Data
    private static class Record {
        Map<AbstractParsedColumn, Object> record;
        Map<AbstractParsedColumn, Object> raw;

        private Record(final Map<AbstractParsedColumn, Object> record, final Map<AbstractParsedColumn, Object> raw) {
            this.record = record;
            this.raw = raw;
        }

        public static Record of(Map<AbstractParsedColumn, Object> o, Map<AbstractParsedColumn, Object> raw){
            return new Record(o, raw);
        }
    }

    private Deque<Record> queue;

    private Record current;

    private Integer skipAfterStart;
    private boolean skippedAfterHeader;
    private Integer skipBeforeEnd;

    private boolean bufferFilled;

    public BufferedRecordIterator(RecordIterator it, Integer skipAfterStart, Integer skipBeforeEnd) {
        super(it);
        checkArgument(skipAfterStart == null || skipAfterStart >= 0, "skipAfterStart should be greater or equal to 0");
        checkArgument(skipBeforeEnd == null || skipBeforeEnd >= 0, "skipBeforeEnd should be greater or equal to 0");
        this.skipAfterStart = skipAfterStart;
        this.skipBeforeEnd = skipBeforeEnd;
        if(this.skipAfterStart == null){
            this.skipAfterStart = 0;
        }
        if(this.skipBeforeEnd == null){
            this.skipBeforeEnd = 0;
        }
        this.queue = new ArrayDeque<>(this.skipBeforeEnd + 1);
    }

    private void prepare(){
        if(!skippedAfterHeader){
            Iterators.advance(it, skipAfterStart);
            skippedAfterHeader = true;
        }
        if(!bufferFilled){
            int i;
            for (i = 0; i < skipBeforeEnd + 1 && it.hasNext(); i++) {
                queue.add(Record.of(it.next(), it.getRaw()));
            }
            bufferFilled = true;
        }
    }

    @Override
    public boolean hasNext() {
        prepare();
        return queue.size() > skipBeforeEnd && queue.peek() != null;
    }

    @Override
    public Map<AbstractParsedColumn, Object> getRaw() {
        prepare();
        return this.current.getRaw();
    }

    @Override
    public Map<AbstractParsedColumn, Object> next() {
        prepare();
        Map<AbstractParsedColumn, Object> next = it.next();
        if(next != null){
            queue.add(Record.of(next, it.getRaw()));
        } else {
            if(queue.size() <= skipBeforeEnd){
                return null;
            }
        }
        return (this.current = queue.poll()).getRecord();
    }

    @Override
    public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
        return it.getSchema();
    }

    @Override
    public Map<AbstractParsedColumn, TypeDescriptor> getSchema(final Map<AbstractParsedColumn, TypeDescriptor> currentSchema) {
        return it.getSchema(currentSchema);
    }
}
