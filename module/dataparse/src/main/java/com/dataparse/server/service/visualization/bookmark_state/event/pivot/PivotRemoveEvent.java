package com.dataparse.server.service.visualization.bookmark_state.event.pivot;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.stream.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PivotRemoveEvent extends QueryChangeEvent {

    Agg key;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        params.getPivot().remove(key);
        for(Agg agg : params.getAggs()){
            if(agg.getSettings().getSort().getType().equals(SortType.BY_VALUE)) {
                agg.getSettings().setSort(new AggSort());
            }
        }
        if(params.getAggs().isEmpty()
           && params.getPivot().isEmpty()){
            params.setShows(
                    state.getShowList().stream()
                            .filter(s -> s.getOp() == null)
                            .limit(10)
                            .collect(Collectors.toList()));
        }

    }

}
