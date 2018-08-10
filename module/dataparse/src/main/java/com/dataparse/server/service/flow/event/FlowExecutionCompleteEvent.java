package com.dataparse.server.service.flow.event;

import com.dataparse.server.service.notification.*;
import lombok.*;
import lombok.experimental.Builder;

import java.util.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlowExecutionCompleteEvent extends Event {

    private String taskId;
    private Long datadocId;
    private Long bookmarkId;
    private String bookmarkName;
    private List<Long> sourceIds;
    private Date startDate;
    private Date finishDate;
    private boolean error;
    private boolean stopped;
    private String errorCode;
    private String errorMessage;

    @Override
    public EventType getType() {
        return EventType.FLOW_EXEC_COMPLETE;
    }

}
