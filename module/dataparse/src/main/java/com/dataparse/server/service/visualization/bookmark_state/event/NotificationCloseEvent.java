package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.*;

@Data
public class NotificationCloseEvent extends BookmarkStateChangeEvent {

    private String messageId;

    @Override
    public void apply(final BookmarkState state) {
        state.getNotifications().removeIf(n -> Objects.equals(n.getId(), messageId));
    }
}
