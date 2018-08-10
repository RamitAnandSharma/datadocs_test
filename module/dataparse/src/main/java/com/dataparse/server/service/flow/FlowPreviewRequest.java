package com.dataparse.server.service.flow;

import lombok.Data;

@Data
public class FlowPreviewRequest extends FlowExecutionRequest {

    private Integer limit;

}
