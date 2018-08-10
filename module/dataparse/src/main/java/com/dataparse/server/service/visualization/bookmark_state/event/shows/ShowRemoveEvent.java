package com.dataparse.server.service.visualization.bookmark_state.event.shows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShowRemoveEvent extends QueryChangeEvent {

    Show key;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        params.getShows().remove(key);
    }
}
