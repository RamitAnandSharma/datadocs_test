package com.dataparse.server.service.visualization.comparator;

import com.dataparse.server.service.visualization.Tree;
import com.dataparse.server.service.visualization.bookmark_state.state.Sort;
import com.dataparse.server.service.visualization.bookmark_state.state.SortDirection;
import com.google.gson.internal.LinkedTreeMap;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;


public class DataNodeComparator implements Comparator<Tree.Node> {

    private Sort sort;
    private String key;

    public DataNodeComparator(String key, Sort sort) {
        this.key = key;
        this.sort = sort;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(Tree.Node first, Tree.Node second) {

        Map<String, Object> firstData  = (Map) first.getData();
        Map<String, Object> secondData = (Map) second.getData();

        Object firstVal = firstData.get(key);
        Object secondVal = secondData.get(key);

        boolean areComparable = firstVal instanceof Comparable && secondVal instanceof Comparable;

        if(firstVal == null){
            if(secondVal == null){
                return 0;
            } else {
                return 1;
            }
        } else if(secondVal == null) {
            return -1;
        }

        if(firstVal instanceof String && secondVal instanceof String) {
            char firstStringLetter = ((String) firstVal).toLowerCase().charAt(0);
            char secondStringLetter = ((String) secondVal).toLowerCase().charAt(0);

            if(firstStringLetter != secondStringLetter) {
                firstVal = ((String) firstVal).toLowerCase();
                secondVal = ((String) secondVal).toLowerCase();
            }
        }

        if(areComparable && sort.getDirection().equals(SortDirection.ASC)) {
            return ((Comparable) firstVal).compareTo(secondVal);
        }

        if(areComparable) {
            return ((Comparable) secondVal).compareTo(firstVal);
        } else {
            return 0;
        }
    }

}