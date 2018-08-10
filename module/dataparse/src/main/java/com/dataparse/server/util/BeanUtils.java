package com.dataparse.server.util;

import org.apache.commons.beanutils.PropertyUtils;

import java.util.*;
import java.util.function.BiFunction;

public class BeanUtils {

    public static void walk(Object object, BiFunction<String, Object, Object> resetFn) {
        walk(object, resetFn, 0);
    }

    private static void walk(Object object, BiFunction<String, Object, Object> resetFn, int depth) {
        try{
            Map<String, Object> paths = org.apache.commons.beanutils.PropertyUtils.describe(object);
            for(Map.Entry<String, Object> entry: paths.entrySet()){
                String property = entry.getKey();
                Object value = entry.getValue();
                if(!"class".equals(property) && PropertyUtils.isWriteable(object, property)) {
                    PropertyUtils.setProperty(object, property, resetFn.apply(property, value));
                    if(value != null && !value.getClass().isEnum()) {
                        if(value instanceof Collection) {
                            Collection coll = (Collection) value;
                            for(Object collItem: coll) {
                                walk(collItem, resetFn, depth + 1);
                            }
                        } else {
                            walk(value, resetFn, depth + 1);
                        }
                    }
                }
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }


}