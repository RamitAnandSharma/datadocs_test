package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class HighlightMatchesToggleEvent extends BookmarkStateChangeEvent {

    private boolean value;

    @Override
    public void apply(final BookmarkState state) {
        state.setHighlightMatches(value);
    }
}
