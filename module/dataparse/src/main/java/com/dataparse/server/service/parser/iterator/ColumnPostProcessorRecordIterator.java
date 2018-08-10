package com.dataparse.server.service.parser.iterator;

import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.flow.settings.ColumnSettings;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.util.ListUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ColumnPostProcessorRecordIterator extends FilterRecordIterator {

    private static final Map<Pair<DataType, DataType>, Function<Object, Object>> COERCE_RULES = ImmutableMap.of(Pair.of(DataType.DATE, DataType.TIME), v -> v);

    private List<ColumnInfo> columnInfo;

    ColumnPostProcessorRecordIterator(RecordIterator it, List<ColumnInfo> columnInfo) {
        super(it);
        this.columnInfo = columnInfo;
    }

    private String buildErrorMessage(Object value, TypeDescriptor type) {
        String stringValue = value == null ? "NULL" : value.toString();
        String typeValue = type.getDataType().getName();
        return String.format("Some values in this column could not be converted to a %s, for example: \"%s\". According to your Ingest Settings, these values will be made NULL.", typeValue, stringValue);
    }

    @Override
    public Map<AbstractParsedColumn, Object> next() {
        Map<AbstractParsedColumn, Object> o = it.next();
        if (o != null && columnInfo != null && !columnInfo.isEmpty()) {
            Map<AbstractParsedColumn, Object> tmp = new LinkedHashMap<>();
            for (ColumnInfo info : columnInfo) {
                ColumnSettings settings = info.getSettings();
                String originalName = info.getName();

                AbstractParsedColumn key = ParsedColumnFactory.getByColumnInfo(info);
                Object value = o.get(key);
                String newName = StringUtils.isNotBlank(settings.getRename()) ? settings.getRename() : originalName;
                TypeDescriptor type = settings.getType();
                if (settings.getType() == null) {
                    type = info.getType();
                }
                value = processSplitOn(type, value, settings.getSplitOn());
                final TypeDescriptor finalType = type;
                value = ListUtils.apply(value, x -> {
                    try {
                        return coerce(x, finalType);
                    } catch (Exception e) {
                        String valueStr = StringUtils.abbreviate(String.valueOf(x), 20);
                        return new ErrorValue(buildErrorMessage(valueStr, finalType));
                    }
                });
                if (StringUtils.isNotBlank(settings.getReplaceErrors()) && (value instanceof ErrorValue)) {
                    try {
                        value = type.parse(settings.getReplaceErrors());
                    } catch (Exception e) {
                        value = null;
                    }
                }
                info.setName(newName);
                tmp.put(key, value);
            }
            o = tmp;
        }
        return o;
    }

    private static Object processSplitOn(TypeDescriptor type, Object value, String splitOn) {
        if (StringUtils.isNotBlank(splitOn)
                && type != null && value instanceof String) {
            return Lists.newArrayList(StringUtils.split(value.toString(), splitOn));
        }
        return value;
    }

    private static Object coerce(Object o, TypeDescriptor type) throws Exception {
        if (o != null && type != null && !(o instanceof ErrorValue)) {
            DataType actualType = DataType.tryGetType(o);
            if (actualType != null && actualType != type.getDataType()) {
                return coerce(o, actualType, type);
            }
        }
        return o;
    }

    private static Object coerce(Object o, DataType actualType, TypeDescriptor type) {
        Function<Object, Object> coerceRule = COERCE_RULES.getOrDefault(
                Pair.of(actualType, type.getDataType()),
                v -> {
                    try {
                        return type.parse(v.toString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return coerceRule.apply(o);
    }
}
