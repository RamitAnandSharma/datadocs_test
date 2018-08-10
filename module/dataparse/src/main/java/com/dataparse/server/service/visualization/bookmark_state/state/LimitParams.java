package com.dataparse.server.service.visualization.bookmark_state.state;

import lombok.Data;

import java.io.Serializable;

@Data
public class LimitParams implements Serializable {

    private int rawData = 10000;
    private int pageSize = 1000;
    private int aggData = 100;
    private int pivotData = 100;

    private int rawDataExport = 100_000;

}
