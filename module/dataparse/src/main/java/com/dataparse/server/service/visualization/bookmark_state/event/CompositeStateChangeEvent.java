package com.dataparse.server.service.visualization.bookmark_state.event;

import lombok.*;

import java.util.*;

@Data
public abstract class CompositeStateChangeEvent<T> extends StateChangeEvent<T> {

    List<StateChangeEvent> events;

    @Override
    public void apply(final T state) {
        for(StateChangeEvent event: events){
            event.setParentEvent(this);
            event.apply(state);
        }
    }
}
