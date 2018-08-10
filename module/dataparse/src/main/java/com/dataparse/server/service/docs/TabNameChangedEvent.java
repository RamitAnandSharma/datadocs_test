package com.dataparse.server.service.docs;

import com.dataparse.server.service.notification.EventType;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TabNameChangedEvent extends DatadocEvent {

    private Long tabId;
    private String renameTo;

    TabNameChangedEvent(Long datadocId, Long tabId, String renameTo) {
        super(datadocId);
        this.tabId = tabId;
        this.renameTo = renameTo;
    }

    public EventType getType() {
        return EventType.TAB_NAME_CHANGED;
    }

}
