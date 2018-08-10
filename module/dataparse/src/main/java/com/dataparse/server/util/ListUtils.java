package com.dataparse.server.util;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class ListUtils {

    public static boolean isEmpty(List<?> l) {
        return l == null || l.size() == 0;
    }

    public static boolean isNotEmpty(List<?> l) {
        return l != null && l.size() > 0;
    }

    public static <T> Map<Integer, T> listToMapByIndex(List<T> list) {
        Map<Integer, T> result = new HashMap<>(list.size(), 1);
        for(int i = 0; i < list.size(); i++) {
            result.put(i, list.get(i));
        }
        return result;
    }
    public static <T> Map<T, Integer> listToMapByValueWithIndex(List<T> list) {
        Map<T, Integer> result = new HashMap<>(list.size(), 1);
        for(int i = 0; i < list.size(); i++) {
            result.put(list.get(i), i);
        }
        return result;
    }

    public static <K, V> Map<K, V> groupByKey(List<V> list, Function<V, K> f) {
        Map<K, V> result = new HashMap<>(list.size(), 1);
        list.forEach(item -> {
            result.put(f.apply(item), item);
        });
        return result;
    }


    public static Object applyIfNotNull(Object o, Function fn){
        if(o == null){
            return null;
        }
        return apply(o, fn);
    }

    public static Object apply(Object o, Function fn){
        if(o instanceof List){
            o = ((List) o).stream().map(fn).collect(Collectors.toList());
        } else {
            o = fn.apply(o);
        }
        return o;
    }

    public static <T>  T first(List<T> l) {
        if(l == null) {
            return null;
        }
        return l.size() == 0 ? null : l.get(0);
    }

    public static List<String> toStringList(List<Object> list) {
        return list.stream().map(String::valueOf).collect(Collectors.toList());
    }

    public static Object apply(Object o, BiFunction fn, Object arg){
        if(o instanceof List){
            o = ((List) o).stream().map(v -> fn.apply(v, arg)).collect(Collectors.toList());
        } else {
            o = fn.apply(o, arg);
        }
        return o;
    }

}
