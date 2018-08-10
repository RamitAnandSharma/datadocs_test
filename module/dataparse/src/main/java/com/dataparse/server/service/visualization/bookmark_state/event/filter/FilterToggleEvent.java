package com.dataparse.server.service.visualization.bookmark_state.event.filter;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.visualization.VisualizationService;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkBuilderFactory;
import com.dataparse.server.service.visualization.bookmark_state.event.BookmarkVizStateChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.response.FilterRefreshEvent;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

@Data
public class FilterToggleEvent extends BookmarkVizStateChangeEvent {

    @Autowired
    VisualizationService visualizationService;

    @Autowired
    BookmarkBuilderFactory bookmarkBuilderFactory;

    String field;
    Boolean selected;

    @Override
    public void apply(final BookmarkState state) {
        Filter filter = state
                .getCurrentQueryParams()
                .getFilters()
                .stream()
                .filter(f -> f.getField().equals(field))
                .findFirst()
                .orElse(null);
        if (filter != null) {
            filter.setSelected(selected);

            if (selected) {
                Auth auth = Auth.get();
                bookmarkBuilderFactory.create(getTabId())
                        .updateFilter(state.getCurrentQueryParams(), state.getColumnList(), filter, getTabId().toString(),
                                      updatedFilter -> {
                                          doResponse(new FilterRefreshEvent(getId(), updatedFilter), auth);
                                      }, true);
            }
        }
    }
}
