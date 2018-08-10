package com.dataparse.server.service.visualization.bookmark_state.event.filter;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.event.response.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;
import org.apache.commons.beanutils.*;
import org.springframework.beans.factory.annotation.*;

import java.lang.reflect.*;
import java.util.*;

@Data
public class FilterSearchEvent extends BookmarkVizStateChangeEvent {

    @Autowired
    BookmarkBuilderFactory bookmarkBuilderFactory;

    private Filter filter;

    @Override
    public void apply(final BookmarkState state) {
        Filter oldFilter = state
                .getCurrentQueryParams()
                .getFilters()
                .stream()
                .filter(f -> f.getField().equals(filter.getField()))
                .findFirst()
                .orElse(null);
        if (oldFilter != null) {
            try {
                BeanUtils.copyProperties(oldFilter, filter);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            if(!state.isAutoRefresh()) {
                doResponse(new FiltersToRefreshEvent(Collections.singletonList(oldFilter.getField())));
            }
            bookmarkBuilderFactory.create(getTabId())
                    .updateFilter(state.getCurrentQueryParams(), state.getColumnList(), oldFilter, getTabId().toString(),
                                  filter -> {
                                      doResponse(new FilterRefreshEvent(getId(), filter));
                                  }, true);
        }
    }
}
