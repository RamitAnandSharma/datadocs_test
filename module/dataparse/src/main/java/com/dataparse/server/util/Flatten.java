package com.dataparse.server.util;

import java.util.*;
import java.util.function.BiConsumer;

public class Flatten {

    private static final Integer MAX_ITERATION_DEPTH = 3;

    public static Map<String, Object> flatten(Map<String, Object> o){
        if(o == null){
            return null;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        iterateMap(o, (path, value) -> {
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

    public static Map<String, Object> unflatten(Map<String, Object> o){
        if(o == null){
            return null;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for(Map.Entry<String, Object> entry : o.entrySet()){
            List<String> path = Arrays.asList(entry.getKey().split("(?<!\\\\)\\."));
            addValueToPath(result, path, entry.getValue());
        }
        return result;
    }

    private static void addValueToPath(Object target, List<String> path, Object value){
        String key = path.get(0);
        key = Flatten.unescapeFieldName(key);

        Object newTarget;
        if(target instanceof Map){
            Map t = (Map) target;
            if(t.containsKey(key)){
                Object o = t.get(key);
                if(!(o instanceof List)){
                    List l = new ArrayList<>();
                    newTarget = l;
                    l.add(o);
                    t.put(key, l);
                } else {
                    newTarget = o;
                }
            } else {
                if(path.size() > 1) {
                    Map m = new HashMap<>();
                    newTarget = m;
                    t.put(key, m);
                } else {
                    newTarget = t;
                }
            }
        } else {
            List l = (List) target;
            Map m = new HashMap<>();
            newTarget = m;
            l.add(m);
        }

        if(path.size() > 1){
            addValueToPath(newTarget, path.subList(1, path.size()), value);
        } else {
            if(newTarget instanceof Map){
                ((Map) newTarget).put(key, value);
            } else if(newTarget instanceof List) {
                ((List) newTarget).add(value);
            }
        }
    }

    public static void iterateList(String p, List list, BiConsumer<String, Object> callback){
        for(Object v : list){
            if(v instanceof Map){
                iterateMap(0, p + ".", (Map<String, Object>) v, callback);
            } else if (v instanceof List){
                iterateList(p, (List) v, callback);
            } else {
                callback.accept(p, v);
            }
        }
    }

    public static void iterateMap(Map<String, Object> map, BiConsumer<String, Object> callback) {
        iterateMap(0, "", map, callback);
    }

    public static void iterateMap(Integer iterationNumber, String p, Map<String, Object> map, BiConsumer<String, Object> callback){
        for(Map.Entry<String, Object> o : map.entrySet()){
            String f = o.getKey();
            Object v = o.getValue();
            if(v instanceof Map && iterationNumber < MAX_ITERATION_DEPTH){
                iterateMap(iterationNumber + 1, p + escapeFieldName(f) + ".", (Map<String, Object>) v, callback);
            } else if (v instanceof List){
                iterateList(p + escapeFieldName(f), (List) v, callback);
            } else if(!(v instanceof Map)) {
                callback.accept(p + escapeFieldName(f), v);
            }
        }
    }

    public static String escapeFieldName(String fieldName){
        return fieldName.replaceAll("([^>])[\\.]", "$1\\\\.");
    }

    public static String unescapeFieldName(String fieldName){
        return fieldName.replaceAll("\\\\\\.", ".");
    }

}
