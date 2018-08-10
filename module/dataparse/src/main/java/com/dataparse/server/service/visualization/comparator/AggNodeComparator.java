package com.dataparse.server.service.visualization.comparator;

import com.dataparse.server.service.visualization.Tree;
import com.dataparse.server.service.visualization.bookmark_state.state.Sort;
import com.dataparse.server.service.visualization.bookmark_state.state.SortDirection;

import java.util.Comparator;


public class AggNodeComparator implements Comparator<Tree.Node> {

    private Sort sort;

    public AggNodeComparator(Sort sort) {
        this.sort = sort;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(Tree.Node first, Tree.Node second) {

        Object firstVal = first.getKey();
        Object secondVal = second.getKey();

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