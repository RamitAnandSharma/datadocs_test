package com.dataparse.server.service.parser.type;

import avro.shaded.com.google.common.collect.*;
import com.dataparse.server.service.flow.*;
import com.dataparse.server.util.DateUtils;
import com.dataparse.server.util.hibernate.*;
import org.apache.commons.lang3.time.*;

import java.math.*;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.function.*;


public enum DataType implements PersistentEnum {
    DECIMAL(1, "number", s -> shouldHandleAsString(s) ? wrapEscapedData(s) : Double.parseDouble(s),
            o -> o instanceof Double || o instanceof Float || o instanceof BigDecimal || o instanceof Long || o instanceof Integer),
    TIME(3, "time", s -> {
        if(shouldHandleAsString(s)) {
            return wrapEscapedData(s);
        } else {
            FastDateFormat format = DateUtils.tryGetTimeFormat(s);
            return TimeTypeDescriptor.parse(s, format == null ? null : format.getPattern());
        }
    }, o -> o instanceof Time),
    DATE(4, "datetime", s -> {
        if(shouldHandleAsString(s)) {
            return wrapEscapedData(s);
        } else {
            FastDateFormat format = DateUtils.tryGetDateFormat(s);
            return DateTypeDescriptor.parse(s, format == null ? null : format.getPattern());
        }
    }, o -> o instanceof Date),
    STRING(8, "string", s -> s, Objects::nonNull);

    private int id;
    private String name;
    private Function<String, Object> parseFn;
    private Function<Object, Boolean> testFn;

    DataType(int id, String name, Function<String, Object> parseFn, Function<Object, Boolean> testFn) {
        this.id = id;
        this.name = name;
        this.parseFn = parseFn;
        this.testFn = testFn;
    }

    @Override
    public int getId() {
        return id;
    }

    public String getName(){
        return name;
    }

    public Object parse(String s) throws Exception {
        return parseFn.apply(s);
    }

    public Boolean isStringType() {
        return this.equals(DataType.STRING);
    }

    public Boolean isNumericType() {
        return this.equals(DataType.DECIMAL);
    }

    private static List<DataType> typesParseOrder = Lists.newArrayList(DECIMAL, TIME, DATE, STRING);

    private static TypeDescriptor tryParseType(String s, DataType dataType){
        try {
            Object value = dataType.parse(s);
            if (value != null && !(value instanceof ErrorValue)) {
                if(dataType == DECIMAL) {
                    return NumberTypeDescriptor.forValue((Double) value);
                } else if (dataType == DATE) {
                    return DateTypeDescriptor.forString(s);
                } else if (dataType == TIME) {
                    return TimeTypeDescriptor.forString(s);
                } else {
                    return new TypeDescriptor(dataType);
                }
            }
        } catch (Exception e) {
            // do nothing
        }
        return null;
    }

    public static TypeDescriptor tryParseType(String s) {
        TypeDescriptor descriptor = null;
        for (DataType dataType : typesParseOrder) {
            descriptor = tryParseType(s, dataType);
            if(descriptor != null){
                break;
            }
        }
        return descriptor;
    }

    /**
     * Try parse type, check only possible super-types of lastType.
     */
    public static TypeDescriptor tryParseType(String s, TypeDescriptor lastType) {
        int startFrom = 0;
        TypeDescriptor descriptor = null;
        if(lastType != null){
            descriptor = lastType;
            startFrom = typesParseOrder.indexOf(lastType.getDataType());
        }
        for (int i = startFrom; i < typesParseOrder.size(); i++) {
            descriptor = tryParseType(s, typesParseOrder.get(i));
            if(descriptor != null){
                break;
            }
        }
        return descriptor;
    }

    public static DataType tryGetType(Object o){
        for(DataType dataType : DataType.values()){
            if(dataType.testFn.apply(o)){
                return dataType;
            }
        }
        return null;
    }

    private static ErrorValue wrapEscapedData(String v) {
        return new ErrorValue("Value escaped by inner rules. " + v);
    }

    private static Boolean shouldHandleAsString(String v) {
        Boolean isZeroEscaped = v.length() > 1 && v.indexOf('.') == -1 && v.startsWith("0");
        return isZeroEscaped;
    }

}
