package com.dataparse.server.service.flow.event;

import com.dataparse.server.service.notification.Event;
import com.dataparse.server.service.notification.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlowExecutionStartEvent extends Event {

    private String taskId;
    private Long bookmarkId;
    private String bookmarkName;
    private List<Long> sourceIds;
    private Date startDate;

    @Override
    public EventType getType() {
        return EventType.FLOW_EXEC_START;
    }
}
