package com.dataparse.server.util;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.upload.Descriptor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.dataparse.server.util.ListUtils.applyIfNotNull;

public class Transform {

    public static Map<String, Object> transform(Map<AbstractParsedColumn, Object> o, Descriptor descriptor){
        Map<String, Object> result = new HashMap<>();
        for(ColumnInfo column : descriptor.getColumns()) {
            String name = column.getAlias();
            Object value = o.get(ParsedColumnFactory.getByColumnInfo(column));
            DataType dataType = column.getType().getDataType();
            if(column.getSettings().getType() != null){
                dataType = column.getSettings().getType().getDataType();
            }
            switch (dataType) {
                case TIME:
                    result.put(name, applyIfNotNull(value, v -> {
                        Date t = ((Date) v);
                        int hour = t.getHours();
                        int minute = t.getMinutes();
                        int second = t.getSeconds();
                        return (hour * 3600 + minute * 60 + second) * 1000;
                    }));
                    break;
                case DATE:
                    result.put(name, applyIfNotNull(value, v -> ((Date) v).getTime()));
                    break;
                default:
                    result.put(name, value);
            }
        }
        return result;
    }

}
