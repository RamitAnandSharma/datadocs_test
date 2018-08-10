package com.dataparse.server.service.visualization.bookmark_state.event.response;

import com.dataparse.server.service.notification.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RequestAppliedEvent extends Event {

    private QueryParams queryParams;

    @Override
    public EventType getType() {
        return EventType.REQUEST_APPLIED;
    }
}
