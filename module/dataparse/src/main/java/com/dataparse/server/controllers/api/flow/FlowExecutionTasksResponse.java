package com.dataparse.server.controllers.api.flow;

import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.upload.Upload;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Builder;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FlowExecutionTasksResponse {
    private TaskInfo taskInfo;
    private Datadoc datadoc;
    private Upload upload;
}
