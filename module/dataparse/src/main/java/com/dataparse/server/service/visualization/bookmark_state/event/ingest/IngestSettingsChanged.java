package com.dataparse.server.service.visualization.bookmark_state.event.ingest;

import com.dataparse.server.service.notification.*;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
public class IngestSettingsChanged extends BookmarkStateChangeEvent {

    public IngestSettingsChanged(final String flowJSON, Long tabId) {
        this.flowJSON = flowJSON;
        this.setTabId(tabId);
    }

    private boolean noReset;
    private String flowJSON;

    @Override
    public void apply(final BookmarkState state) {
        state.setFlowJSON(flowJSON);
        if(noReset){
            state.setPendingFlowJSON(flowJSON);
        }
    }

    @Override
    public EventType getType() {
        return EventType.INGEST_STATE_CHANGED;
    }
}
