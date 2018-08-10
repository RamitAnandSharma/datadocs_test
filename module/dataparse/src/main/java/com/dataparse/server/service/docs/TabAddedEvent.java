package com.dataparse.server.service.docs;

import com.dataparse.server.service.notification.EventType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
public class TabAddedEvent extends DatadocEvent {

    private Long tabId;
    private UUID stateId;

    TabAddedEvent(Long datadocId, Long tabId, UUID stateId) {
        super(datadocId);
        this.tabId = tabId;
        this.stateId = stateId;
    }

    public EventType getType() {
        return EventType.TAB_ADDED;
    }

}
