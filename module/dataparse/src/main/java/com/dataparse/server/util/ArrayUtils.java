package com.dataparse.server.util;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class ArrayUtils {

    public static <T, K> List<T> arrayToList(K[] arr, BiFunction<Integer, K, T> valueBuilder) {
        AtomicInteger counter = new AtomicInteger();
        return Arrays.stream(arr)
                .map(val -> valueBuilder.apply(counter.getAndIncrement(), val))
                .collect(Collectors.toList());
    }

    public static <T> T[] head(T[] arr) {
        if(arr.length <= 1) {
            return arr;
        }
        return Arrays.copyOfRange(arr, 0, arr.length - 1);
    }
    public static <T> T[] tail(T[] arr) {
        if(arr.length <= 1) {
            return arr;
        }
        return Arrays.copyOfRange(arr, 1, arr.length);
    }
}
