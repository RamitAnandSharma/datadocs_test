package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.event.response.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;
import org.springframework.beans.factory.annotation.*;

import java.util.*;
import java.util.stream.*;

@Data
public class ViewRawEvent extends BookmarkVizCompositeStateChangeEvent {

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    @Override
    public void apply(BookmarkState state) {
        state.setBeforeViewRawParams(state.getCurrentQueryParams().copy());
        state.setRefreshAvailable(true);
        super.apply(state);

        List<RawShow> selectedRawShows = new ArrayList<>(state
                                                                 .getRawShowList()
                                                                 .stream()
                                                                 .filter(RawShow::getSelected)
                                                                 .collect(Collectors.toList()));

        state.getCurrentQueryParams().setShows(selectedRawShows.stream().map(rs -> {
            Show showTmp = new Show(rs.getField());
            Show newShow = state.getShowList().stream().filter(s -> s.equals(showTmp)).findFirst().orElse(null);

            if (newShow != null && rs.getSort() != null) {
                newShow.getSettings().setSort(new Sort(rs));
            }

            return newShow;
        }).collect(Collectors.toList()));

        state.getCurrentQueryParams().setAggs(new ArrayList<>());
        state.getCurrentQueryParams().setPivot(new ArrayList<>());

        if(state.isAutoRefresh()) {
            bookmarkBuilderFactory.create(getTabId())
                    .updateFilters(state, new ArrayList<>(), state.getColumnList(),
                                   filter -> {
                                       doResponse(new FilterRefreshEvent(getId(), filter));
                                   }, true);
        } else {
            state.getFiltersToRefresh().addAll(state.getColumnList()
                                                       .stream()
                                                       .map(Col::getField)
                                                       .collect(Collectors.toList()));
            doResponse(new FiltersToRefreshEvent(state.getFiltersToRefresh()));
        }
    }
}
