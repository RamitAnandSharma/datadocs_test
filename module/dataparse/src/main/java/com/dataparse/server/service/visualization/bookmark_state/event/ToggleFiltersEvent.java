package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class ToggleFiltersEvent extends BookmarkVizStateChangeEvent {

    private boolean show;

    @Override
    public void apply(final BookmarkState state) {
        state.setShowFilters(show);
    }
}
