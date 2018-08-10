package com.dataparse.server.service.visualization.bookmark_state.event.show_totals;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShowTotalsChangeEvent extends BookmarkVizStateChangeEvent {

    Boolean showTotals;

    @Override
    public void apply(BookmarkState state) {
        state.setShowTotals(showTotals);
    }

}
