package com.dataparse.server.service.visualization.bookmark_state.event.raw;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawShowMoveEvent extends BookmarkVizStateChangeEvent {

    RawShow key;
    Integer toPosition;

    @Override
    public void apply(BookmarkState state) {
        RawShow show = state.getRawShowList().stream().filter(s -> s.equals(key)).findFirst().orElse(null);
        if(show != null){
            state.getRawShowList().remove(show);
            state.getRawShowList().add(toPosition, show);
        }
    }
}
