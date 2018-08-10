package com.dataparse.server.service.visualization.bookmark_state.event.pivot_collapsed_state;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PivotCollapsedStateAddEvent extends BookmarkVizStateChangeEvent {

    String key;
    Boolean value;

    @Override
    public void apply(BookmarkState state) {
        state.getPivotCollapsedState().put(key, value);
    }

}
