package com.dataparse.server.service.visualization.bookmark_state.event.cols;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColRenameEvent extends BookmarkVizStateChangeEvent {

    String field;
    String renameTo;

    @Override
    public void apply(final BookmarkState state) {
        Col col = state.getColumnList().stream().filter(c -> c.getField().equals(field)).findFirst().orElse(null);
        if(col != null){
            col.setName(renameTo);
        }
    }
}
