package com.dataparse.server.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.dataparse.server.util.Flatten.escapeFieldName;

public class FlattenXml {

    public static Map<String, Object> flatten(Map<String, Object> o){
        if(o == null){
            return null;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        iterateMap("", o, (path, value) -> {
            Object v = result.get(path);
            if(v != null){
                if(v instanceof List) {
                    ((List) v).add(value);
                } else {
                    List l = new ArrayList();
                    l.add(v);
                    l.add(value);
                    result.put(path, l);
                }
            } else {
                result.put(path, value);
            }
        });
        return result;
    }

    public static void iterateList(String p, List list, BiConsumer<String, Object> callback){
        for(Object v : list){
            if(v instanceof Map){
                iterateMap(p + ".", (Map<String, Object>) v, callback);
            } else if (v instanceof List){
                iterateList(p, (List) v, callback);
            } else {
                callback.accept(p, v);
            }
        }
    }

    public static void iterateMap(String p, Map<String, Object> map, BiConsumer<String, Object> callback){
        for(Map.Entry<String, Object> o : map.entrySet()){
            String f = o.getKey();
            Object v = o.getValue();
            if(v instanceof Map){
                iterateMap(p + escapeFieldName(f) + ".", (Map<String, Object>) v, callback);
            } else if (v instanceof List){
                iterateList(p + escapeFieldName(f), (List) v, callback);
            } else {
                if(f.startsWith("@")){
                    callback.accept(p.length() > 0 ? p.substring(0, p.length() - 1) : p, v);
                } else if(f.startsWith("[") && f.endsWith("]")){
                    callback.accept(p.length() > 0 ? p.substring(0, p.length() - 1) + f : escapeFieldName(f), v);
                } else {
                    callback.accept(p + escapeFieldName(f), v);
                }
            }
        }
    }
}
