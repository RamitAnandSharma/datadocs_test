package com.dataparse.server.service.visualization.bookmark_state.event.raw;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawShowChangeEvent extends BookmarkVizStateChangeEvent {

    RawShow show;

    @Override
    public void apply(BookmarkState state) {
        RawShow rawShow = state.getRawShowList().stream().filter(s -> s.equals(show)).findFirst().orElse(null);
        if (rawShow != null) {
            rawShow.setSelected(show.getSelected());
            rawShow.setSort(show.getSort());
        }
    }
}
