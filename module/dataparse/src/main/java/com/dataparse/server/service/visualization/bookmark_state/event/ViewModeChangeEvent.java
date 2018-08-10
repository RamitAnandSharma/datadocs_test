package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class ViewModeChangeEvent extends BookmarkVizStateChangeEvent {

    private ViewMode viewMode;

    @Override
    public void apply(final BookmarkState state) {
        state.setViewMode(viewMode);
    }
}
