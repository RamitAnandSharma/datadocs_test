package com.dataparse.server.service.visualization.bookmark_state.event.ingest;

import com.dataparse.server.service.notification.EventType;
import com.dataparse.server.service.visualization.bookmark_state.event.BookmarkStateChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class LastSourceSelectedIdChangeEvent extends BookmarkStateChangeEvent {

    private Long lastSourceSelectedId;

    @Override
    public void apply(final BookmarkState state) {
        state.setLastSourceSelectedId(lastSourceSelectedId);
    }
}
