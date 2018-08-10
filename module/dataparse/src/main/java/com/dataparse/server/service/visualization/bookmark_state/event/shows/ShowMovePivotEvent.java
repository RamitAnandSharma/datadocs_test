package com.dataparse.server.service.visualization.bookmark_state.event.shows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShowMovePivotEvent extends BookmarkVizStateChangeEvent {

    List<String> pivotOrder;

    @Override
    public void apply(BookmarkState state) {
        state.getCurrentQueryParams().setPivotOrder(pivotOrder);
    }
}
