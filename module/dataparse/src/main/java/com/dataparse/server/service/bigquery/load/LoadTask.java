package com.dataparse.server.service.bigquery.load;

import com.dataparse.server.service.bigquery.*;
import com.dataparse.server.service.tasks.*;
import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;
import lombok.extern.slf4j.*;
import org.springframework.beans.factory.annotation.*;

import java.util.*;

@Slf4j
public class LoadTask extends AbstractTask<LoadRequest> {

    @Autowired
    @JsonIgnore
    private BigQueryService bigQueryService;

    @Override
    public Map<TaskState, Runnable> getStates() {
        return ImmutableMap.of(LoadTaskState.LOAD, () -> {
            long start = System.currentTimeMillis();
            log.info("Creating BigQuery table...");
            String externalId = bigQueryService.createTable(getRequest().getAccountId(), getRequest().getDescriptor());
            log.info("Created BigQuery table in {}", (System.currentTimeMillis() - start));
            try {
                LoadResult finalResult = bigQueryService.doLoad(getRequest().getAccountId(), externalId,
                                                                getRequest().getDescriptor(), this::saveResult);
                saveResult(finalResult);
                if(!finalResult.isSuccess()) {
                    bigQueryService.deleteDataset(getRequest().getAccountId(), externalId);
                }
            } catch (Exception e){
                bigQueryService.deleteDataset(getRequest().getAccountId(), externalId);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void cancel() {

    }
}
