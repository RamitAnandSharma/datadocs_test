package com.dataparse.server.service.parser.iterator;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class InterruptibleRecordIterator extends FilterRecordIterator {

    InterruptibleRecordIterator(RecordIterator it) {
        super(it);
    }

    @Override
    public Map<AbstractParsedColumn, Object> next() {
        if (Thread.currentThread().isInterrupted()) {
            throw new RuntimeException("Parser interrupted");
        }
        return it.next();
    }
}
