package com.dataparse.server.service.user.user_state.event;

import com.dataparse.server.service.notification.*;
import com.dataparse.server.service.user.user_state.state.*;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import lombok.*;

@Data
public abstract class UserStateChangeEvent extends StateChangeEvent<UserState> {

    Long userId;

    @Override
    public EventType getType() {
        return EventType.USER_STATE_CHANGED;
    }
}
