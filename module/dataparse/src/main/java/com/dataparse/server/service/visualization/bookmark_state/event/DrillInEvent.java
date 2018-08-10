package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.event.response.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;
import org.springframework.beans.factory.annotation.*;

import java.util.*;
import java.util.stream.*;

@Data
public class DrillInEvent extends BookmarkVizCompositeStateChangeEvent {

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    @Override
    public void apply(final BookmarkState state) {
        state.setRefreshAvailable(true);
        super.apply(state);
        if(state.isAutoRefresh()) {
            bookmarkBuilderFactory.create(getTabId())
                    .updateFilters(state, new ArrayList<>(), state.getColumnList(),
                                   filter -> {
                                       doResponse(new FilterRefreshEvent(getId(), filter));
                                   }, false);
        } else {
            state.getFiltersToRefresh().addAll(state.getColumnList()
                                                       .stream()
                                                       .map(Col::getField)
                                                       .collect(Collectors.toList()));
            doResponse(new FiltersToRefreshEvent(state.getFiltersToRefresh()));
        }
    }
}
