package com.dataparse.server.service.visualization.bookmark_state.event.filter;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkBuilderFactory;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.event.response.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.stream.*;

@Data
public class SearchChangeEvent extends QueryChangeEvent {

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    String search;
    boolean force;

    @Override
    public void apply(final BookmarkState state, QueryParams params) {
        params.setSearch(search);

        if(state.isAutoRefresh()) {
            if(state.isInstantSearch() || force) {
                Auth auth = Auth.get();
                bookmarkBuilderFactory.create(getTabId())
                        .updateFilters(state, new ArrayList<>(), state.getColumnList(),
                                       filter -> {
                                           doResponse(new FilterRefreshEvent(getId(), filter), auth);
                                       }, false);
            }
        } else {
            state.getFiltersToRefresh().addAll(state.getColumnList()
                                                       .stream()
                                                       .map(Col::getField)
                                                       .collect(Collectors.toList()));
            doResponse(new FiltersToRefreshEvent(state.getFiltersToRefresh()));
        }
    }
}
