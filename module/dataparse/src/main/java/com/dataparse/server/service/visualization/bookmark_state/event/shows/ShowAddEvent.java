package com.dataparse.server.service.visualization.bookmark_state.event.shows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShowAddEvent extends QueryChangeEvent {

    Show key;
    Integer toPosition;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        if(toPosition == null) {
            params.getShows().add(key);
        } else {
            params.getShows().add(toPosition, key);
        }
    }
}
