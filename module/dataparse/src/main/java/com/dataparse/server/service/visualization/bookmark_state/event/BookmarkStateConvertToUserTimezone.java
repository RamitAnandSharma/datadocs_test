package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class BookmarkStateConvertToUserTimezone extends BookmarkStateChangeEvent {

    private Boolean convertToUserTimezone;

    @Override
    public void apply(final BookmarkState state) {
        state.setConvertToUserTimezone(convertToUserTimezone);
    }
}