package com.dataparse.server.service.export.event;

import com.dataparse.server.service.export.ExportFormat;
import com.dataparse.server.service.notification.Event;
import com.dataparse.server.service.notification.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExportStartEvent extends Event {

    private String taskId;
    private Long datadocId;
    private Date startDate;
    private ExportFormat exportFormat;

    @Override
    public EventType getType() {
        return EventType.EXPORT_START;
    }
}

