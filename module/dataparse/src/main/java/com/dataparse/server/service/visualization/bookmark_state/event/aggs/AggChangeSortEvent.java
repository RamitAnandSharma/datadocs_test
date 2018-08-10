package com.dataparse.server.service.visualization.bookmark_state.event.aggs;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.service.visualization.bookmark_state.utils.SerializationUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggChangeSortEvent extends QueryChangeEvent {

    Agg key;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        Agg agg = params.getAggs().stream().filter(a -> a.equals(key)).findFirst().orElse(null);
        if(agg != null){
            AggSort sort = key.getSettings().getSort();
            if(sort.getAggKeyPath() != null){
                sort.setAggKeyPath(SerializationUtils.mapKeys(sort.getAggKeyPath()));
            }
            agg.getSettings().setSort(sort);
        }
    }

}
