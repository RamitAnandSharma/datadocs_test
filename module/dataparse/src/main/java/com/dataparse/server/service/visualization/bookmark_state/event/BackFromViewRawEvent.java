package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.Data;

@Data
public class BackFromViewRawEvent extends BookmarkVizStateChangeEvent {

    @Override
    public void apply(BookmarkState state) {
        state.setQueryParams(state.getBeforeViewRawParams().copy());

        state.setBeforeViewRawParams(new QueryParams());
    }
}
