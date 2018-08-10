package com.dataparse.server.service.visualization.bookmark_state.utils;

import java.util.List;
import java.util.stream.Collectors;


public class SerializationUtils {

    // instead of custom mapper just do the job here
    public static List<Object> mapKeys(List<Object> keys){
        return keys.stream().map(key -> {
            if(key instanceof Integer){
                return ((Integer) key).longValue();
            }
            return key;
        }).collect(Collectors.toList());
    }
}
