package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class QueryEditorHeightChangeEvent extends BookmarkVizStateChangeEvent {

    private int height;

    @Override
    public void apply(final BookmarkState state) {
        state.setQueryEditorHeight(height);
    }
}