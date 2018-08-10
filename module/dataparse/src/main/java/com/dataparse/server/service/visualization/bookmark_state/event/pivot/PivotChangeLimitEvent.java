package com.dataparse.server.service.visualization.bookmark_state.event.pivot;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PivotChangeLimitEvent extends QueryChangeEvent {

    Agg key;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        Agg pivot = params.getPivot().stream().filter(a -> a.equals(key)).findFirst().orElse(null);
        if(pivot != null){
            pivot.getSettings().setLimit(key.getSettings().getLimit());
        }
    }

}
