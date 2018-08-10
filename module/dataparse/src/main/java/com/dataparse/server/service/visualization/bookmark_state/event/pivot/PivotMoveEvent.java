package com.dataparse.server.service.visualization.bookmark_state.event.pivot;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import javax.management.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PivotMoveEvent extends QueryChangeEvent {

    Agg key;
    Integer toPosition;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        Agg pivot = state.getCurrentQueryParams().getPivot().stream().filter(a -> a.equals(key)).findFirst().orElse(null);
        if(pivot != null){
            state.getCurrentQueryParams().getPivot().remove(pivot);
            state.getCurrentQueryParams().getPivot().add(toPosition, pivot);
        }
    }

}
