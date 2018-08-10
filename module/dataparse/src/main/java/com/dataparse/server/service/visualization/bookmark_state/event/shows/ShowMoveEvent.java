package com.dataparse.server.service.visualization.bookmark_state.event.shows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShowMoveEvent extends BookmarkVizStateChangeEvent {

    Show key;
    Integer toPosition;

    @Override
    public void apply(BookmarkState state) {
        Show show = state.getCurrentQueryParams().getShows().stream().filter(s -> s.equals(key)).findFirst().orElse(null);
        if(show != null){
            state.getCurrentQueryParams().getShows().remove(show);
            state.getCurrentQueryParams().getShows().add(toPosition, show);
        }
    }
}
