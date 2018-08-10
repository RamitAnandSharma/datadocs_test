package com.dataparse.server.service.visualization;

import com.dataparse.server.service.visualization.bookmark_state.state.*;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class TreeUtils {

    private static final String JOIN_PREFIX = "_";

    public static List<Object> getParentKeysPath(Tree.Node<?> node){
        List<Object> path = new ArrayList<>();
        path.add(node.getKey());
        while(node.getParent() != null){
            node = node.getParent();
            path.add(node.getKey());
        }
        Collections.reverse(path);
        return path;
    }

    public static <T> Tree.Node<T> getIn(Tree.Node<T> node, List<Object> path){
        if(path.isEmpty() || node == null){
            return node;
        }
        Object key = path.get(0);
        Tree.Node<T> child = node.getChildren().stream().filter(c -> Objects.equals(c.getKey(), key)).findFirst().orElse(null);
        return getIn(child, path.subList(1, path.size()));
    }

    public static String getField(List<Object> path){
        return path.stream()
                .map(o -> o == null ? "" : o.toString())
                .collect(Collectors.joining(JOIN_PREFIX));
    }

    public static <T> Map<String, T> prefixMap(Map<String, T> data, List<Object> path) {
        String prefix;
        if (path.isEmpty()) {
            prefix = "";
        } else {
            prefix = getField(path) + JOIN_PREFIX;
        }
        Map<String, T> prefixedMap = new HashMap<>(data.size());
        for(Map.Entry<String, T> entry : data.entrySet()){
            prefixedMap.put(prefix + entry.getKey(), entry.getValue());
        }
        return prefixedMap;
    }

    public static void flattenNode(Tree.Node<Map<String, Object>> node, List<Agg> aggs, int level,
                                   List<Object> path,
                                   Map<String, Object> aggValues) {
        path = new ArrayList<>(path);
        if(level > 0) {
            Agg agg = aggs.get(level - 1);
            Object key = node.getKey();
            path.add(key);
            aggValues.put(agg.key(), node.getKey());
        }
        if(node.getData() != null) {
            Map<String, Object> prefixedData = prefixMap(node.getData(), path);
            aggValues.putAll(prefixedData);
        }
        if (level < aggs.size()) {
            for (Tree.Node<Map<String, Object>> child : node.getChildren()) {
                flattenNode(child, aggs, level + 1, path, aggValues);
            }
        }
    }

    public static void flattenNode(Tree.Node<Map<String, Object>> node, int level, List<Object> path, Map<String, Object> flatData) {
        path = new ArrayList<>(path);
        if(level > 0) {
            Object key = node.getKey();
            path.add(key);
        }
        if(node.getData() != null) {
            Map<String, Object> prefixedData = prefixMap(node.getData(), path);
            flatData.putAll(prefixedData);
        }
        for (Tree.Node<Map<String, Object>> child : node.getChildren()) {
            flattenNode(child, level + 1, path, flatData);
        }
    }

    public static Map<String, Object> flattenTree(Tree.Node<Map<String, Object>> node,
                                                  List<Agg> pivot) {
        Map<String, Object> row = new LinkedHashMap<>();
        flattenNode(node, pivot, 0, new ArrayList<>(), row);
        return row;
    }

    public static <T> Tree.Node<T> mapTreeKey(Tree.Node<T> node, Function<Tree.Node<T>, Object> function) {
        Tree.Node<T> copy = new Tree.Node<>();
        copy.setKey(function.apply(node));
        copy.setData(node.getData());
        copy.setChildren(node.getChildren().stream()
                                 .map(n -> {
                                     Tree.Node<T> childCopy = mapTreeKey(n, function);
                                     childCopy.setParent(copy);
                                     return childCopy;
                                 })
                                 .collect(Collectors.toList()));
        return copy;
    }

    public static <T, R> Tree.Node<R> mapTreeNode(Tree.Node<T> node, Function<T, R> function) {
        Tree.Node<R> copy = new Tree.Node<>();
        copy.setKey(node.getKey());
        copy.setData(function.apply(node.getData()));
        copy.setChildren(node.getChildren().stream()
                                 .map(n -> {
                                     Tree.Node<R> childCopy = mapTreeNode(n, function);
                                     childCopy.setParent(copy);
                                     return childCopy;
                                 })
                                 .collect(Collectors.toList()));
        return copy;
    }


    public static <T, R> Tree<R> mapData(Tree<T> tree, Function<T, R> function){
        Tree<R> treeCopy = new Tree<>();
        treeCopy.setRoot(mapTreeNode(tree.getRoot(), function));
        return treeCopy;
    }

}
