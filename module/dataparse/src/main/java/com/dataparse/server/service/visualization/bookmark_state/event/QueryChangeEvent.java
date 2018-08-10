package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;

public abstract class QueryChangeEvent extends BookmarkVizStateChangeEvent {

    public abstract void apply(final BookmarkState state, final QueryParams params);

    @Override
    public void apply(final BookmarkState state) {
        if(state.isAutoRefresh()) {
            state.setRefreshAvailable(true);
        }
        apply(state, state.getCurrentQueryParams());
    }
}
