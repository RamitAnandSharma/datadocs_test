package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class PageModeChangeEvent extends BookmarkStateChangeEvent {

    private PageMode pageMode;

    @Override
    public void apply(final BookmarkState state) {
        state.setPageMode(pageMode);

        if (pageMode == PageMode.VIZ) {
            state.setFlowJSON(state.getPendingFlowJSON());
        }
    }
}
