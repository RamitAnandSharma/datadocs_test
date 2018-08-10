package com.dataparse.server.service.visualization.bookmark_state.event.rows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RowsPinnedCountChangedEvent extends BookmarkVizStateChangeEvent {

    Integer rowsCount;

    @Override
    public void apply(final BookmarkState state) {
        state.setPinnedRowsCount(rowsCount);
    }
}
