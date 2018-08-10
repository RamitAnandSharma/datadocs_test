package com.dataparse.server.service.docs;

import com.dataparse.server.service.notification.EventType;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ViewDatadocEvent extends DatadocEvent {

    ViewDatadocEvent(Long datadocId) {
        super(datadocId);
    }

    public EventType getType() {
        return EventType.VIEW_DATADOC;
    }

}
