package com.dataparse.server.service.visualization.bookmark_state.event.ingest;

import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class EngineSelectionStrategyChangeEvent extends BookmarkStateChangeEvent {

    private EngineSelectionStrategy engineSelectionStrategy;

    @Override
    public void apply(final BookmarkState state) {
        state.getFlowSettings().setEngineSelectionStrategy(engineSelectionStrategy);
    }
}
