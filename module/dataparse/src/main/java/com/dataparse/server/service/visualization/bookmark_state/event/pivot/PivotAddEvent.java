package com.dataparse.server.service.visualization.bookmark_state.event.pivot;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PivotAddEvent extends QueryChangeEvent {

    Agg key;
    Integer toPosition;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        if(params.getPivot().isEmpty()
           && params.getAggs().isEmpty()){
            params.getShows().clear();
            state.setViewMode(ViewMode.TABLE);
        }
        if(toPosition == null){
            params.getPivot().add(key);
        } else {
            params.getPivot().add(toPosition, key);
        }
    }

}
