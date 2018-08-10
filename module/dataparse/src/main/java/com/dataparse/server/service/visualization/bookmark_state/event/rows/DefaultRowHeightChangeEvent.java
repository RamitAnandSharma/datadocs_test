package com.dataparse.server.service.visualization.bookmark_state.event.rows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DefaultRowHeightChangeEvent extends BookmarkVizStateChangeEvent {

    Integer defaultRowHeight;

    @Override
    public void apply(BookmarkState state) {
        state.setDefaultRowHeight(defaultRowHeight);
    }
}
