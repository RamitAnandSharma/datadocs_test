package com.dataparse.server.service.parser.iterator;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.type.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class CollectionDelegateRecordIterator implements RecordIterator {

    private Iterator<Map<AbstractParsedColumn, Object>> delegate;
    private Map<AbstractParsedColumn, Object> currentObj;

    public CollectionDelegateRecordIterator(Iterator<Map<AbstractParsedColumn, Object>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Map<AbstractParsedColumn, Object> next() {
        try {
            this.currentObj = delegate.next();
        } catch (NoSuchElementException e){
            return null;
        }
        return currentObj;
    }

    @Override
    public Map<AbstractParsedColumn, Object> getRaw() {
        return this.currentObj;
    }

    @Override
    public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
        Map<AbstractParsedColumn, Object> o = getRaw();
        if(o == null){
            return null;
        }
        Map<AbstractParsedColumn, TypeDescriptor> schema = new LinkedHashMap<>();
        for(Map.Entry<AbstractParsedColumn, Object> entry : o.entrySet()){
            Object value = entry.getValue();
            DataType dataType = DataType.tryGetType(value);
            TypeDescriptor type;
            if(dataType == DataType.DATE) {
                type = new DateTypeDescriptor();
            } else if(dataType == DataType.TIME) {
                type = new TimeTypeDescriptor();
            } else if(dataType == DataType.DECIMAL) {
                type = new NumberTypeDescriptor();
            } else {
                type = new TypeDescriptor(dataType);
            }
            schema.put(entry.getKey(), type);
        }
        return schema;
    }

    @Override
    public long getBytesCount() {
        return -1; // not implemented
    }

}
