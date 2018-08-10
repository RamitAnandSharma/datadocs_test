package com.dataparse.server.controllers.api.flow;

import com.dataparse.server.service.engine.EngineType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Builder;

import javax.validation.constraints.NotNull;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExecutionRequest {

    @NotNull
    private Long bookmarkId;
    private Boolean userInitiated;

    private EngineType engineType;

    private boolean autoIngest;
}
