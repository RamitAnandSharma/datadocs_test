package com.dataparse.server.service.visualization.bookmark_state.event.response;

import com.dataparse.server.service.notification.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FiltersToRefreshEvent extends Event {

    private List<String> fields;

    public FiltersToRefreshEvent(final Set<String> fields) {
        this.fields = new ArrayList<>(fields);
    }

    @Override
    public EventType getType() {
        return EventType.FILTERS_TO_REFRESH;
    }
}
