package com.dataparse.server.service.visualization.bookmark_state.state;

import java.util.*;

public class Row extends HashMap {

    private static final String TREE_LEVEL_KEY = "__treeLevel";

    public Integer getTreeLevel(){
        return (Integer) get(TREE_LEVEL_KEY);
    }

    public void setTreeLevel(Integer level){
        put(TREE_LEVEL_KEY, level);
    }
}
