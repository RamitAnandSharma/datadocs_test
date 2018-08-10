package com.dataparse.server.service.parser.iterator;

import com.dataparse.server.service.flow.transform.ColumnTransform;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.type.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface RecordIterator extends java.util.Iterator<Map<AbstractParsedColumn, Object>>, Closeable {

    static InterruptibleRecordIterator interruptible(RecordIterator it){
        return new InterruptibleRecordIterator(it);
    }

    static PostProcessorRecordIterator withTransforms(RecordIterator it, List<ColumnTransform> transforms){
        return new PostProcessorRecordIterator(it, transforms);
    }

    static ColumnPostProcessorRecordIterator withColumns(RecordIterator it, List<ColumnInfo> columnInfo){
        return new ColumnPostProcessorRecordIterator(it, columnInfo);
    }

    static BufferedRecordIterator withSkippedRows(RecordIterator it, Integer skipAfterHeader, Integer skipBeforeHeader){
        return new BufferedRecordIterator(it, skipAfterHeader, skipBeforeHeader);
    }

    static LimitedRecordIterator limited(RecordIterator it, Integer limit){
        return new LimitedRecordIterator(it, limit);
    }

    RecordIterator EMPTY_LIST_ITERATOR = new RecordIterator() {
        @Override
        public boolean hasNext() {
            return false;
        }
        @Override
        public Map<AbstractParsedColumn, Object> getRaw() {
            return null;
        }
        @Override
        public Map<AbstractParsedColumn, Object> next() {
            return null;
        }
        @Override
        public long getBytesCount() {
            return 0;
        }
        @Override
        public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
            return null;
        }
        @Override
        public void close() throws IOException {
        }
    };

    static RecordIterator emptyIterator(){
        return EMPTY_LIST_ITERATOR;
    }

    Map<AbstractParsedColumn, Object> getRaw();

    static TypeDescriptor getType(Object value){
        DataType dataType = DataType.tryGetType(value);
        if(dataType == null) {
            return DataType.tryParseType((String) value);
        }
        switch (dataType) {
            case DATE:
                return value instanceof Date ? DateTypeDescriptor.forValue((Date) value) : new DateTypeDescriptor();
            case DECIMAL:
                boolean isNumber = value != null && value instanceof Number;
                return isNumber ? NumberTypeDescriptor.forValue(((Number) value).doubleValue()) : new NumberTypeDescriptor();
            case TIME:
                return new TimeTypeDescriptor();
            case STRING:
                return DataType.tryParseType(String.valueOf(value));
            default:
                throw new IllegalArgumentException(String.format("There is no data type '%s', for value '%s'", dataType, value));
        }
    }

    default Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
        Map<AbstractParsedColumn, Object> row = getRaw();
        return getSchemaFromMap(row);
    }
    // todo refactor this
    static Map<AbstractParsedColumn, TypeDescriptor> getSchemaFromMap(Map<AbstractParsedColumn, Object> row) {
        if(row == null){
            return null;
        }
        Map<AbstractParsedColumn, TypeDescriptor> schema = new LinkedHashMap<>();
        for(Map.Entry<AbstractParsedColumn, Object> entry: row.entrySet()){
            TypeDescriptor type;
            if(entry.getValue() instanceof List){
                List<Object> l = (List) entry.getValue();
                type = l.stream().limit(10).map(RecordIterator::getType).reduce(TypeDescriptor::getCommonType).orElse(null);
            } else {
                type = getType(entry.getValue());
            }
            schema.put(entry.getKey(), type);
        }
        return schema;
    }

    default Map<AbstractParsedColumn, TypeDescriptor> getSchema(Map<AbstractParsedColumn, TypeDescriptor> currentSchema){
        return getSchema();
    }

    default long getRowNumber() {
        return -1L;
    }
    default long getRowsCount() {
        return -1L;
    }

    long getBytesCount();
}
