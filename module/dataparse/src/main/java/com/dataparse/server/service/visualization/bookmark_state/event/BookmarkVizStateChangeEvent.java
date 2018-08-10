package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.notification.*;
import lombok.*;

@Data
public abstract class BookmarkVizStateChangeEvent extends BookmarkStateChangeEvent {

    @Override
    public EventType getType() {
        return EventType.VIZ_STATE_CHANGED;
    }
}
