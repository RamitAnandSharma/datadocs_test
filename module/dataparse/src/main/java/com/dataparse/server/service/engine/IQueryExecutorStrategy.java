package com.dataparse.server.service.engine;

import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.visualization.request_builder.*;

public interface IQueryExecutorStrategy {

    IQueryExecutor get(TableSchema schema);

}
