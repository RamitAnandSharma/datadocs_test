package com.dataparse.server.util;


import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class FunctionUtils {

    @FunctionalInterface
    public interface SimpleFunction {
        void invoke();
    }

    public static SimpleFunction doNothing = () -> {};


    public static <K, V> Function<K, V> identity() {
        return k -> (V) k;
    }

    public static <T> Boolean isAny(T el, T ... objs) {
        if(el == null) {
            return false;
        }
        for(T obj : objs) {
            return el.equals(obj);
        }
        return false;
    }

    public static <T> Predicate<T> complement(Predicate<T> func) {
        return func.negate();
    }

    public static <T> Function<T, Boolean> complementFunc(Function<T, Boolean> func) {
        return (v) -> !func.apply(v);
    }


    public static <T> T invokeSilent(Supplier<T> func) {
        return invokeSilentWithDefault(func, null);
    }

    public static <T, R> R invokeUncaughtSilent(Function<T, R> func, T arg) {
        try {
            return func.apply(arg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    public static <T> T invokeSilentWithDefault(Supplier<T> func, T def) {
        try {
            return func.get();
        } catch (Exception e) {
//            do nothing expected
        }
        return def;
    }

    public static <T> boolean every(Collection<T> col, Predicate<T> predicate) {
        return col.stream().filter(predicate).collect(Collectors.toList()).size() == col.size();
    }

    public static <T> Object coalesce(Supplier<T>... obj) {
        for (Supplier<T> f : obj) {
            T val = invokeSilent(f);
            if(val != null) {
                return val;
            }
        }
        return null;
    }

    public static String safeToString(Object v) {
        if(v == null) {
            return null;
        }
        return v.toString();
    }

    public static Boolean safeEquals(Object f, Object s) {
        if(f == s) {
            return true;
        }
        return f != null && f.equals(s);
    }

    public static <D, R> R applyIfNotNull(D data, Function<D, R> function) {
        if(data == null) {
            return null;
        }
        return function.apply(data);
    }

    public static <T> T coalesce(T ... obj) {
        for (T o : obj) {
            if(o != null) {
                return o;
            }
        }
        return null;
    }

    public static String coalesceWithDefaultNull(Object ... obj) {
        Object[] newArr = Arrays.copyOf(obj, obj.length + 1);
        newArr[obj.length] = "NULL";
        return coalesce(newArr).toString();
    }
}
