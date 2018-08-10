package com.dataparse.server.service.parser.iterator;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;

import java.util.Map;

public class LimitedRecordIterator extends FilterRecordIterator {

    private int limit;
    private int currentNum;

    public LimitedRecordIterator(RecordIterator it, int limit) {
        super(it);
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        return currentNum < limit && it.hasNext();
    }

    @Override
    public Map<AbstractParsedColumn, Object> next() {
        if(currentNum >= limit){
            return null;
        }
        currentNum++;
        return it.next();
    }
}
