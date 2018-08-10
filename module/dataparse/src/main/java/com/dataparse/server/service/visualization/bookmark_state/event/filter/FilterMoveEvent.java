package com.dataparse.server.service.visualization.bookmark_state.event.filter;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.Data;

@Data
public class FilterMoveEvent extends BookmarkVizStateChangeEvent {

    String field;
    Integer toPosition;

    @Override
    public void apply(BookmarkState state) {
        Filter filterToMove = state.getCurrentQueryParams().getFilters().stream().filter(f -> f.getField().equals(field)).findFirst().orElse(null);
        if(filterToMove != null) {
            state.getCurrentQueryParams().getFilters().remove(filterToMove);
            state.getCurrentQueryParams().getFilters().add(toPosition, filterToMove);
        }
    }

}
