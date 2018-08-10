package com.dataparse.server.service.visualization.bookmark_state.state;

import com.dataparse.server.service.visualization.bookmark_state.event.StateChangeEvent;
import lombok.*;

@Data
public class BookmarkStateHistoryWrapper {

    private boolean canUndo;
    private boolean canRedo;
    private BookmarkState state;
    private StateChangeEvent event;

    public BookmarkStateHistoryWrapper(HistoryItem historyItem, boolean canUndo, boolean canRedo) {
        this.state = historyItem.getState();
        this.event = historyItem.getEvent();
        this.canUndo = canUndo;
        this.canRedo = canRedo;
    }

    public BookmarkStateHistoryWrapper(BookmarkState state) {
        this.state = state;
    }

}
