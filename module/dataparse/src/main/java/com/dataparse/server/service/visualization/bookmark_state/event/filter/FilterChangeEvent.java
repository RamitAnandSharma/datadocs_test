package com.dataparse.server.service.visualization.bookmark_state.event.filter;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.event.response.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;
import org.apache.commons.beanutils.*;
import org.springframework.beans.factory.annotation.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.stream.*;

@Data
public class FilterChangeEvent extends QueryChangeEvent {

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    Filter filter;

    @Override
    public void apply(BookmarkState state, QueryParams params) {
        Filter oldFilter = params
                .getFilters().stream()
                .filter(f -> f.getField().equals(filter.getField()))
                .findFirst()
                .orElse(null);
        if (oldFilter != null) {
            try {
                BeanUtils.copyProperties(oldFilter, filter);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            // let composite events update filters by themselves
            if (parentEvent == null) {
                if (state.isAutoRefresh()) {
                    Auth auth = Auth.get();
                    BookmarkStateBuilder bookmarkStateBuilder = bookmarkBuilderFactory.create(getTabId());
                    if(bookmarkStateBuilder.isFastFilterExecutor()) {
                        return;
                    }

                    bookmarkStateBuilder.updateFilters(state, Collections.singletonList(filter.getField()), state.getColumnList(),
                                           filter -> doResponse(new FilterRefreshEvent(getId(), filter), auth), false);

                } else {
                    state.getFiltersToRefresh().addAll(state.getColumnList()
                                                               .stream()
                                                               .filter(col -> !col.getField().equals(filter.getField()))
                                                               .map(Col::getField)
                                                               .collect(Collectors.toList()));
                    doResponse(new FiltersToRefreshEvent(state.getFiltersToRefresh()));
                }
            }
        }
    }
}
