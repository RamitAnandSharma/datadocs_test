package com.dataparse.server.service.visualization.bookmark_state.event.limit;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class LimitRawDataChangedEvent extends QueryChangeEvent {

    private int limit;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        if(limit > 0 && limit <= 10_000) {
            params.getLimit().setRawData(limit);
        }
    }
}
