package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.dataparse.server.service.visualization.bookmark_state.state.Notification;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationAddEvent extends BookmarkStateChangeEvent {
    private Notification notification;

    @Override
    public void apply(BookmarkState state) {
        state.getNotifications().add(notification);
    }
}
