package com.dataparse.server.service.flow;

import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.tasks.AbstractRequest;
import com.dataparse.server.service.tasks.AbstractTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Builder;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlowExecutionRequest extends AbstractRequest {

    private String flowJSON;
    private FlowSettings flowSettings;
    private String rootNode;
    private Long datadocId;
    private Boolean userInitiated;
    private Boolean scheduled = false;
    private boolean preserveHistory;
    private boolean autoIngest;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return FlowExecutionTask.class;
    }
}
