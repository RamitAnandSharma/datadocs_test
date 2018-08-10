package com.dataparse.server.service.visualization.bookmark_state;

import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.FilterQueryExecutor;
import com.dataparse.server.service.visualization.request_builder.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class BookmarkBuilderFactory {

    @Autowired
    @Qualifier("defaultQueryExecutorStrategy")
    private IQueryExecutorStrategy queryExecutorStrategy;

    @Autowired
    private FilterQueryExecutor filterQueryExecutor;

    @Autowired
    private TableRepository tableRepository;

    public BookmarkStateBuilder create(Long tabId) {
        TableBookmark bookmark = tableRepository.getTableBookmark(tabId);
        return create(bookmark);
    }

    public BookmarkStateBuilder create(TableBookmark bookmark) {
        IQueryExecutor queryExecutor = null;
        if(bookmark.getTableSchema().getCommitted() != null){
            queryExecutor = queryExecutorStrategy.get(bookmark.getTableSchema());
        }
        return BookmarkStateBuilder.create(queryExecutor, filterQueryExecutor, bookmark);
    }

}
