package com.dataparse.server.service.visualization.bookmark_state.event.ingest;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ResetIngestSettings extends BookmarkStateChangeEvent {

    @Override
    public void apply(final BookmarkState state) {
        state.setFlowJSON(state.getPendingFlowJSON());
    }
}
