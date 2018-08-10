package com.dataparse.server.service.visualization.bookmark_state.event.ingest;

import com.dataparse.server.service.visualization.bookmark_state.event.BookmarkStateChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.dataparse.server.service.visualization.bookmark_state.state.ColNameFormat;
import lombok.Data;

@Data
public class ColNameFormatChangeEvent extends BookmarkStateChangeEvent {
    private ColNameFormat colNameFormat;

    public void apply(BookmarkState state) {
        state.setColNameFormat(colNameFormat);
    }
}
