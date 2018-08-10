package com.dataparse.server.service.visualization.bookmark_state.event.raw;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawShowToggleAllEvent extends BookmarkVizStateChangeEvent {

    Boolean selected;

    @Override
    public void apply(BookmarkState state) {
        state.getRawShowList().stream().forEach(s -> s.setSelected(selected));
    }
}
