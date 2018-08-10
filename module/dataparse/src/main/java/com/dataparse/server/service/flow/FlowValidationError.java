package com.dataparse.server.service.flow;

import com.dataparse.server.service.tasks.*;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FlowValidationError extends ExecutionException.Error {
    private String nodeId;

    public FlowValidationError(String nodeId, String message){
        super(null, message);
        this.nodeId = nodeId;
    }

    public FlowValidationError(String nodeId, String code, String message){
        super(code, message);
        this.nodeId = nodeId;
    }
}
