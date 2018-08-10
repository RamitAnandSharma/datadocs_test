package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class AdvancedFilterQueryChangeEvent  extends QueryChangeEvent {

    private String query;

    @Override
    public void apply(final BookmarkState state, QueryParams params) {
        params.setAdvancedFilterQuery(query);
    }
}
