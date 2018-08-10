package com.dataparse.server.service.visualization.bookmark_state.event.aggs;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggAddEvent extends QueryChangeEvent {

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
            params.getAggs().add(key);
        } else {
            params.getAggs().add(toPosition, key);
        }
    }

}
