package com.dataparse.server.service.visualization.bookmark_state.event.response;

import com.dataparse.server.service.notification.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FilterRefreshEvent extends Event {

    String srcEventId;
    Filter filter;

    @Override
    public EventType getType() {
        return EventType.FILTER_REFRESH;
    }
}
