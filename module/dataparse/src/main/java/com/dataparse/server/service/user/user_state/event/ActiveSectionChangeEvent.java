package com.dataparse.server.service.user.user_state.event;

import com.dataparse.server.service.user.user_state.state.*;
import lombok.*;

@Data
public class ActiveSectionChangeEvent extends UserStateChangeEvent {

    private Section activeSection;

    @Override
    public void apply(final UserState state) {
        state.setActiveSection(activeSection);
    }
}
