package com.dataparse.server.service.visualization.bookmark_state.event.aggs;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggMoveEvent extends QueryChangeEvent {

    Agg key;
    Integer toPosition;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        Agg agg = params.getAggs().stream().filter(a -> a.equals(key)).findFirst().orElse(null);
        if(agg != null){
            params.getAggs().remove(agg);
            params.getAggs().add(toPosition, agg);
        }
    }

}
