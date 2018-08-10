package com.dataparse.server.service.upload;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.IndexedParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@NoArgsConstructor
public class ColumnDetector {

    private long start = System.currentTimeMillis();
    private Map<AbstractParsedColumn, TypeDescriptor> schema = new LinkedHashMap<>();
    private Map<AbstractParsedColumn, Boolean> repeatedCheck = new LinkedHashMap<>();

    public ColumnDetector(Map<AbstractParsedColumn, TypeDescriptor> schema) {
        this.schema = schema;
    }

    public void processCurrentRow(RecordIterator it){
        Map<AbstractParsedColumn, TypeDescriptor> rowSchema = it.getSchema(schema);
        processRowSchema(rowSchema);
    }

    public List<ColumnInfo> processRows(List<Map<AbstractParsedColumn, Object>> rows) {
        rows.forEach(this::processCurrentRow);
        return getColumns();
    }

    private void processCurrentRow(Map<AbstractParsedColumn, Object> row) {
        Map<AbstractParsedColumn, TypeDescriptor> rowSchema = RecordIterator.getSchemaFromMap(row);
//        try to find at list one list field
        row.entrySet().stream()
                .forEach(entry -> {
                    Boolean bool = repeatedCheck.get(entry.getKey());
                    if(bool == null || Boolean.FALSE.equals(bool)) {
                        repeatedCheck.put(entry.getKey(), entry.getValue() instanceof List);
                    }
                });
        processRowSchema(rowSchema);
    }

    public List<ColumnInfo> getColumns() {
        List<ColumnInfo> columns = new ArrayList<>();
        Integer index = 0;
        for(Map.Entry<AbstractParsedColumn, TypeDescriptor> e : schema.entrySet()){
            ColumnInfo column = new ColumnInfo();
            AbstractParsedColumn key = e.getKey();
            Boolean repeated = repeatedCheck.getOrDefault(key, false);

            column.setName(key.getColumnName());
            column.setType(e.getValue());
            column.setRepeated(repeated);
            if(key instanceof IndexedParsedColumn) {
                column.setInitialIndex(((IndexedParsedColumn) key).getIndex());
            }
            column.getSettings().setIndex(index++);
            columns.add(column);
        }
        boolean pkFound = false;
        for(ColumnInfo columnInfo : columns){
            if(columnInfo.getType() == null){
                columnInfo.setType(new TypeDescriptor(DataType.STRING));
            }
            if(!pkFound && columnInfo.getName().equalsIgnoreCase("id")){
                columnInfo.setPkey(pkFound = true);
            }
        }

        log.info("Parsed columns in {}", (System.currentTimeMillis() - start));
        return columns;
    }

    private void processRowSchema(Map<AbstractParsedColumn, TypeDescriptor> rowSchema) {
        if (rowSchema != null) {
         rowSchema.forEach((col, type) -> schema.put(col, TypeDescriptor.getCommonType(schema.get(col), type)));
        }
    }
}
