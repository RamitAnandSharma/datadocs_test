package com.dataparse.server.service.docs;

import com.dataparse.server.service.notification.EventType;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DatadocNameChangedEvent extends DatadocEvent {

    private String renameTo;

    DatadocNameChangedEvent(Long datadocId, String renameTo) {
        super(datadocId);
        this.renameTo = renameTo;
    }

    public EventType getType() {
        return EventType.DATADOC_NAME_CHANGED;
    }

}
