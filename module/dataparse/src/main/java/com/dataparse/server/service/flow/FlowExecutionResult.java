package com.dataparse.server.service.flow;

import com.dataparse.server.service.flow.node.*;
import com.dataparse.server.service.tasks.TaskResult;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlowExecutionResult extends TaskResult {

    private Long bookmarkId;
    private List<Long> sourceIds;
    private Map<String, NodeState> state;

}
