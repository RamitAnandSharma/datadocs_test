package com.dataparse.server.service.visualization.bookmark_state.event.filter;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.event.response.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;
import org.springframework.beans.factory.annotation.*;

@Data
@NoArgsConstructor
public class FilterResetAllEvent extends BookmarkVizStateChangeEvent {

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    @Override
    public void apply(final BookmarkState state) {
        Auth auth = Auth.get();
        bookmarkBuilderFactory.create(getTabId())
                .resetFilters(state).forEach(filter -> {
            doResponse(new FilterRefreshEvent(getId(), filter), auth);
        });
    }
}
