package com.dataparse.server.controllers.api.flow;

import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.UUID;

@Data
public class ValidationRequest {

    @NotNull
    private Long bookmarkId;
    @NotNull
    private UUID stateId;
}
