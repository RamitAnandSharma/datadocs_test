package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.TimeZone;

@Data
public class BookmarkTimezoneChangeEvent extends BookmarkStateChangeEvent {

    private TimeZone timezone;

    @Override
    public void apply(final BookmarkState state) {
        state.setTimezone(timezone);
    }
}