package com.dataparse.server.service.visualization.bookmark_state.event.cols;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColsPinnedCountChangedEvent extends BookmarkVizStateChangeEvent {

    Integer colsCount;

    @Override
    public void apply(final BookmarkState state) {
        state.setPinnedColsCount(colsCount);
    }
}
