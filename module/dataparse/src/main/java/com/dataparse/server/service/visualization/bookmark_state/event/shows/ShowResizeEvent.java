package com.dataparse.server.service.visualization.bookmark_state.event.shows;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.dataparse.server.service.visualization.bookmark_state.state.Show;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShowResizeEvent extends BookmarkVizStateChangeEvent {

    Show key;
    Integer width;

    @Override
    public void apply(BookmarkState state) {
        Show showInQuery = state.getCurrentQueryParams().getShows().stream().filter(s -> s.equals(key)).findFirst().orElse(null);
        Show showInList = state.getShowList().stream().filter(s -> s.equals(key)).findFirst().orElse(null);
        if(showInQuery != null){
            showInQuery.getSettings().setWidth(width);
        }
        if(showInList != null) {
            showInList.getSettings().setWidth(width);
        }
    }
}
