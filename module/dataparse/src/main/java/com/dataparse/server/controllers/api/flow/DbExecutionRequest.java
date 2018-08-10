package com.dataparse.server.controllers.api.flow;

import com.dataparse.server.service.engine.EngineType;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.List;

@Data
public class DbExecutionRequest {

    @NotNull
    private Long datadocId;

    private Long sourceId;

    private List<Long> tables;

    private EngineType engineType;

}
