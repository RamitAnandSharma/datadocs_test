package com.dataparse.server.service.visualization.bookmark_state.event.request;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkBuilderFactory;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateBuilder;
import com.dataparse.server.service.visualization.bookmark_state.event.BookmarkVizStateChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.response.FilterRefreshEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.response.RequestAppliedEvent;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashSet;

@Data
public class ApplyRequestEvent extends BookmarkVizStateChangeEvent {

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    @Override
    public void apply(final BookmarkState state) {
        BookmarkStateBuilder builder = bookmarkBuilderFactory.create(getTabId());
        if(!state.isAutoRefresh()){
            state.setQueryParams(state.getPendingQueryParams().copy());
            state.getFiltersToRefresh().forEach(filter -> {

                Auth auth = Auth.get();
                Filter filterObj = state.getQueryParams().getFilters().stream().filter(f -> f.getField().equals(filter)).findFirst().orElse(null);
                builder.updateFilter(state.getQueryParams(), state.getColumnList(), filterObj, getTabId().toString(), refreshedFilter -> {
                    doResponse(new FilterRefreshEvent(null, refreshedFilter), auth);
                }, false);
            });
            state.setFiltersToRefresh(new HashSet<>());
            state.setRefreshAvailable(false);
        }
        doResponse(new RequestAppliedEvent(state.getQueryParams()));
    }
}
