package com.dataparse.server.service.visualization.bookmark_state.state;

import java.util.*;

public enum AggOp {

    HOUR("hour"),
    DAY("day"),
    MONTH("month"),
    QUARTER("quarter"),
    YEAR("year");

    private String esOp;

    AggOp(String esOp) {
        this.esOp = esOp;
    }

    public String getEsOp() {
        return esOp;
    }

    public static List<AggOp> getDateOps() {
        return Arrays.asList(HOUR, DAY, MONTH, QUARTER, YEAR);
    }
}
