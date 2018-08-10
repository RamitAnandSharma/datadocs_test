package com.dataparse.server.service.engine;

import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.visualization.request_builder.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

@Service
public class DefaultQueryExecutorStrategy implements IQueryExecutorStrategy {

    @Autowired
    private BigQueryRequestExecutor bigQueryRequestExecutor;

    @Autowired
    private EsRequestExecutor esRequestExecutor;

    @Override
    public IQueryExecutor get(TableSchema schema) {
        EngineType engineType = schema.getEngineType();
        switch (engineType){
            case BIGQUERY:
                return bigQueryRequestExecutor;
            case ES:
                return esRequestExecutor;
            default:
                throw new RuntimeException("Unknown query executor: " + engineType);
        }
    }
}
