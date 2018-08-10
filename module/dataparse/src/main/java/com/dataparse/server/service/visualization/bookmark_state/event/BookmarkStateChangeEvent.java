package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.notification.Event;
import com.dataparse.server.service.notification.EventType;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.Data;

import java.util.UUID;

@Data
public abstract class BookmarkStateChangeEvent extends StateChangeEvent<BookmarkState> {

    private Long tabId;
    private UUID stateId;

    public void doResponse(Event event){
        doResponse(event, Auth.get());
    }

    public void doResponse(Event event, Auth auth){
        String topic = String.format("/vis/event-response/%s/%s", this.getTabId(), this.getStateId());
        getSockJSService().send(auth,  topic, event);
    }

    public BookmarkStateId getBookmarkStateId() {
        return new BookmarkStateId(this.tabId, this.stateId);
    }

    @Override
    public EventType getType() {
        return EventType.TAB_STATE_CHANGED;
    }

}
