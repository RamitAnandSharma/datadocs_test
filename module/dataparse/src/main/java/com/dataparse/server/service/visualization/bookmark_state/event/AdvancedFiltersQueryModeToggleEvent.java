package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class AdvancedFiltersQueryModeToggleEvent extends QueryChangeEvent {

    boolean advancedMode;

    @Override
    public void apply(final BookmarkState state, QueryParams params) {
        params.setAdvancedMode(advancedMode);
    }
}
