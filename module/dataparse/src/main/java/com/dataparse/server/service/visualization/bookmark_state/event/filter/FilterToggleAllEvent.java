package com.dataparse.server.service.visualization.bookmark_state.event.filter;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkBuilderFactory;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.event.response.FilterRefreshEvent;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;

@Data
public class FilterToggleAllEvent extends BookmarkVizStateChangeEvent {

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    Boolean selected;

    @Override
    public void apply(BookmarkState state) {
        state.getCurrentQueryParams().getFilters().forEach(s -> s.setSelected(selected));
        if (selected) {
            Auth auth = Auth.get();
            bookmarkBuilderFactory.create(getTabId())
                    .updateFilters(state, new ArrayList<>(), state.getColumnList(),
                                   filter -> {
                                       doResponse(new FilterRefreshEvent(getId(), filter), auth);
                                   }, true);
        }
    }
}

