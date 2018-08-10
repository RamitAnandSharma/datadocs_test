package com.dataparse.server.service.visualization.bookmark_state.event.request;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.*;

@Data
public class CancelRequestEvent extends BookmarkVizStateChangeEvent {

    @Override
    public void apply(final BookmarkState state) {
        state.setPendingQueryParams(state.getQueryParams().copy());
        state.setFiltersToRefresh(new HashSet<>());
        state.setRefreshAvailable(false);
    }
}
