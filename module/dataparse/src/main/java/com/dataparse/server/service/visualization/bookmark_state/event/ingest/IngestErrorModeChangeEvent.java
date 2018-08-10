package com.dataparse.server.service.visualization.bookmark_state.event.ingest;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class IngestErrorModeChangeEvent extends BookmarkStateChangeEvent {

    private IngestErrorMode ingestErrorMode;

    @Override
    public void apply(final BookmarkState state) {
        state.getFlowSettings().setIngestErrorMode(ingestErrorMode);
    }
}
