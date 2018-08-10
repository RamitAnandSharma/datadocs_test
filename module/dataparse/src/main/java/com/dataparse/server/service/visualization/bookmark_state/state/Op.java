package com.dataparse.server.service.visualization.bookmark_state.state;

import java.util.*;

public enum Op {

    VALUE(true),
    COUNT(false),
    UNIQUE_COUNT(false),
    APPROX_UNIQUE_COUNT(false),

    SUM(true),
    AVG(true),
    MIN(true),
    MAX(true);

    private boolean preservesType;

    Op(boolean preservesType) {
        this.preservesType = preservesType;
    }

    public boolean isPreservesType() {
        return preservesType;
    }

    public static List<Op> getCommonOps() {
        return Arrays.asList(VALUE, COUNT, UNIQUE_COUNT, APPROX_UNIQUE_COUNT);
    }

    public static List<Op> getNumberOps() {
        return Arrays.asList(SUM, AVG, MIN, MAX);
    }
}
