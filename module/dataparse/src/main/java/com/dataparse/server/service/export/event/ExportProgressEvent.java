package com.dataparse.server.service.export.event;

import com.dataparse.server.service.notification.Event;
import com.dataparse.server.service.notification.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExportProgressEvent extends Event {

    private String taskId;
    private Long datadocId;
    private Double complete;

    @Override
    public EventType getType() {
        return EventType.EXPORT_PROGRESS;
    }
}
