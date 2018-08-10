package com.dataparse.server.ingest;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class IngestInput {
    private String name;
    private Integer expectedRowsCount;
    private Integer expectedColumnsCount;
    private Integer maxTime;
    private String contentType;
}

