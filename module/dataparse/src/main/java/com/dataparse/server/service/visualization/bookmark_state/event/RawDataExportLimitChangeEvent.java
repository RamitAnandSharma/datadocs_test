package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class RawDataExportLimitChangeEvent extends BookmarkVizStateChangeEvent {

    private int limit;

    @Override
    public void apply(final BookmarkState state) {
        state.getCurrentQueryParams().getLimit().setRawDataExport(limit);
    }
}
