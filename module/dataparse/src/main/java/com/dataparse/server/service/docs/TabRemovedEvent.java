package com.dataparse.server.service.docs;

import com.dataparse.server.service.notification.EventType;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TabRemovedEvent extends DatadocEvent {

    private Long tabId;
    private Integer tabIndex;

    TabRemovedEvent(Long datadocId, Long tabId, Integer tabIndex) {
        super(datadocId);
        this.tabId = tabId;
        this.tabIndex = tabIndex;
    }

    public EventType getType() {
        return EventType.TAB_REMOVED;
    }

}
