package com.dataparse.server.service.flow.event;

import com.dataparse.server.service.flow.node.*;
import com.dataparse.server.service.notification.*;
import lombok.*;

import java.util.*;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlowExecutionProgressEvent extends Event {

    private String taskId;
    private Long bookmarkId;
    private String bookmarkName;
    private List<Long> sourceIds;
    private Map<String, NodeState> state;

    @Override
    public EventType getType() {
        return EventType.FLOW_EXEC_PROGRESS;
    }
}
