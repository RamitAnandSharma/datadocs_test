package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.notification.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.*;

@Data
public class BookmarkVizCompositeStateChangeEvent extends BookmarkStateChangeEvent {

    @Override
    public EventType getType() {
        return EventType.VIZ_STATE_CHANGED;
    }

    List<StateChangeEvent> events;

    @Override
    public void apply(final BookmarkState state) {
        for (StateChangeEvent event : events) {
            event.setParentEvent(this);
            event.apply(state);
        }
    }

}
