package com.dataparse.server.service.visualization.bookmark_state.state;

import com.dataparse.server.service.visualization.bookmark_state.event.StateChangeEvent;
import lombok.Data;

@Data
public class HistoryItem {
    private BookmarkState state;
    private StateChangeEvent event;

    public HistoryItem(BookmarkState state, StateChangeEvent event) {
        this.state = state;
        this.event = event;
    }
}
