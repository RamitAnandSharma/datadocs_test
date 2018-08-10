package com.dataparse.server.service.bigquery.load;

import com.dataparse.server.service.tasks.*;
import lombok.*;

@Data
public class LoadResult extends IngestTaskResult {

    private String tableName;
    private String accountId;
    private Double percentComplete;
    private boolean success;
}
