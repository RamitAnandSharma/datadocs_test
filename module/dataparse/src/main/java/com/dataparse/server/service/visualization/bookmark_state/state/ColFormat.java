package com.dataparse.server.service.visualization.bookmark_state.state;

import lombok.*;

import java.io.*;

@Data
public class ColFormat implements Serializable {

    public enum Type {
        TEXT,
        NUMBER,
        PERCENT,
        FINANCIAL,
        DATE_1,
        DATE_2,
        TIME,
        DATE_TIME,
        DURATION,
        BOOLEAN_1,
        BOOLEAN_2
    }

    private boolean possibleMillisTimestamp;
    private int defaultDecimalPlaces;
    private Boolean showThousandsSeparator = false;
    private int decimalPlaces = 0;
    private String currency = "$";
    private Type type;

}
