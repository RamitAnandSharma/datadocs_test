package com.dataparse.server.service.visualization.bookmark_state.event.cols;

import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class ColSearchTypeChangeEvent extends QueryChangeEvent {

    private String field;
    private SearchType searchType;

    @Override
    public void apply(final BookmarkState state, final QueryParams params) {
        Col col = state.getColumnList().stream().filter(c -> c.getField().equals(field)).findFirst().orElse(null);
        if(col != null){
            col.getSettings().setSearchType(searchType);
        }
    }
}
