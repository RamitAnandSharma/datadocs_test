package com.dataparse.server.util;

import com.dataparse.server.service.flow.settings.SearchType;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.upload.Descriptor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.sql.Time;
import java.util.*;
import java.util.function.Function;

public class JSONTuplewiseTransform {

    private static final String FIELD_NAME_RAW = "field_name_raw";

    public static Map<String, Object> transform(Map<AbstractParsedColumn, Object> o, Descriptor descriptor){
        List<Map<String, Object>> tuples = new ArrayList<>();
        for(Map.Entry<AbstractParsedColumn, Object> entry : o.entrySet()){
            AbstractParsedColumn col = entry.getKey();
            Object value = entry.getValue();
            ColumnInfo columnInfo = null;
            if(descriptor != null) {
                columnInfo = descriptor.getColumns().stream().filter(c -> {
                    if(IndexUtils.getHistoryColumnDate(col.getColumnName(), c.getKey()) != null) {
                        return c.getSettings().getPreserveHistory() != null;
                    }
                    return col.equals(c.getKey());
                }).findFirst().orElse(null);
            }
            if(columnInfo != null) {
                final TypeDescriptor type;
                if(columnInfo.getSettings().getType() == null){
                    type = columnInfo.getType();
                } else {
                    type = columnInfo.getSettings().getType();
                }
                final SearchType searchType;
                if (columnInfo.getSettings().getSearchType() == null) {
                    if(type.getDataType().equals(DataType.STRING)) {
                        searchType = SearchType.EDGE;
                    } else {
                        searchType = SearchType.NONE;
                    }
                } else {
                    searchType = columnInfo.getSettings().getSearchType();
                }
                final boolean disableFacets = columnInfo.getSettings().isDisableFacets();

                ListUtils.apply(value, o1 -> {
                    Map<String, Object> tuple = getNewTuple(col.getColumnName());
                    switch (type.getDataType()){
                        case DECIMAL:
                            tuple.put("double_value", o1);
                            break;
                        case TIME:
                            tuple.put("time_value", applyIfNotNull(o1, v -> {
                                Time t = ((Time) o1);
                                int hour = t.getHours();
                                int minute = t.getMinutes();
                                int second = t.getSeconds();
                                return (hour * 3600 + minute * 60 + second) * 1000;
                            }));
                            break;
                        case DATE:
                            tuple.put("date_value", applyIfNotNull(o1, v -> ((Date) v).getTime()));
                            break;
                        case STRING:
                            tuple.put("str_value_default", applyIfNotNull(o1, v -> v.toString()));
                            if(!disableFacets) {
                                tuple.put("str_value_raw", applyIfNotNull(o1, v -> v
                                        .toString()
                                        .substring(0, Math.min(300, v.toString().length()))));
                                tuple.put("str_value_sort", applyIfNotNull(o1, v -> v
                                        .toString()
                                        .substring(0, Math.min(300, v.toString().length()))));
                            }
                            break;
                        default:
                            throw new RuntimeException("Unknown data type: " + type.getDataType());
                    }
                    switch (searchType){
                        case NONE:
                            break;
                        case EDGE:
                            tuple.put("str_value_e_ngram", applyIfNotNull(o1, Object::toString));
                            break;
                        case FULL:
                            tuple.put("str_value_ngram", applyIfNotNull(o1, Object::toString));
                            break;
                        default:
                            throw new RuntimeException("Unknown search type: " + searchType.name());
                    }
                    tuples.add(tuple);

                    return null;
                });
            }
        }
        return ImmutableMap.of("tuples", tuples);
    }

    private static Object applyIfNotNull(Object value, Function<Object, Object> fn){
        if(value == null){
            return null;
        }
        return fn.apply(value);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> restore(Map<String, Object> o){
        List<Map<String, Object>> tuples = (List<Map<String, Object>>) o.get("tuples");
        Map<String, Object> flatObject = new LinkedHashMap<>();
        for (Map<String, Object> tuple: tuples) {
            String fieldName = (String) tuple.get(FIELD_NAME_RAW);
            Object value = null;
            if(tuple.containsKey("long_value")){
                value = Math.round(Double.parseDouble(String.valueOf(tuple.get("long_value"))));
            } else if(tuple.containsKey("double_value")){
                value = Double.parseDouble(String.valueOf(tuple.get("double_value")));
            } else if(tuple.containsKey("date_value")) {
                value = tuple.get("date_value");
            } else if(tuple.containsKey("time_value")) {
                value = tuple.get("time_value");
            } else if(tuple.containsKey("location_value")){
                value = Arrays.asList(((Map) tuple.get("location_value")).get("lat"), ((Map) tuple.get("location_value")).get("lon"));
            } else if(tuple.containsKey("bool_value")){
                value = Boolean.parseBoolean(String.valueOf(tuple.get("bool_value")));
            } else if(tuple.containsKey("str_value_e_ngram")){
                value = tuple.get("str_value_e_ngram");
            } else if(tuple.containsKey("str_value_ngram")) {
                value = tuple.get("str_value_ngram");
            } else if(tuple.containsKey("str_value_raw")) {
                value = tuple.get("str_value_raw");
            }
            Object v = flatObject.get(fieldName);
            if(v != null){
                if(v instanceof List){
                    ((List) v).add(value);
                } else {
                    flatObject.put(fieldName, Lists.newArrayList(v, value));
                }
            } else {
                flatObject.put(fieldName, value);
            }
        }
        return flatObject;
    }

    private static Map<String, Object> getNewTuple(String path){
        Map<String, Object> tuple = new HashMap<>();
        tuple.put(FIELD_NAME_RAW, path);
        return tuple;
    }

}
