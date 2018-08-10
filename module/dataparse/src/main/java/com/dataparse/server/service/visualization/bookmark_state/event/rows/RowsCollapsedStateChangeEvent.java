package com.dataparse.server.service.visualization.bookmark_state.event.rows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RowsCollapsedStateChangeEvent extends BookmarkVizStateChangeEvent {

    String key;
    Boolean value;

    @Override
    public void apply(BookmarkState state) {
        state.getRowsCollapsedState().put(key, value);
    }

}
