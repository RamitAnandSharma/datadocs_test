package com.dataparse.server.service.visualization.bookmark_state.event.shows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShowChangeSortEvent extends QueryChangeEvent {
    Show key;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        Show showInQuery = params.getShows().stream().filter(s -> s.equals(key)).findFirst().orElse(null);
        if(showInQuery != null){
            showInQuery.getSettings().setSort(key.getSettings().getSort());
        }
    }
}
