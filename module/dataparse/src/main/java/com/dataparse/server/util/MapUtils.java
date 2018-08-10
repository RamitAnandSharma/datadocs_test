package com.dataparse.server.util;


import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MapUtils {

    public static <K, V> Map<K, V> buildMap(List<Pair<K, V>> data) {
        Map<K, V> result = new HashMap<>(data.size());
        data.forEach(d -> result.put(d.getKey(), d.getValue()));
        return result;
    }

    public static <D, K, V> Map<K, V> mapFromList(List<D> data, Function<D, K> keyGen, Function<D, V> valGen) {
        Map<K, V> result = new HashMap<>(data.size());
        data.forEach(d -> result.put(keyGen.apply(d), valGen.apply(d)));
        return result;
    }


}
