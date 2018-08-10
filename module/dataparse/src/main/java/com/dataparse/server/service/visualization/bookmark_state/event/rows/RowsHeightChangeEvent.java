package com.dataparse.server.service.visualization.bookmark_state.event.rows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RowsHeightChangeEvent extends BookmarkVizStateChangeEvent {

    HashMap<String, Integer> rowsHeight;

    @Override
    public void apply(BookmarkState state) {
        state.setRowsHeight(rowsHeight);
    }
}
