package com.dataparse.server.service.visualization;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.service.engine.IQueryExecutorStrategy;
import com.dataparse.server.service.parser.iterator.ProgressAwareIterator;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableSchema;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkBuilderFactory;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateBuilder;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.service.visualization.comparator.*;
import com.dataparse.server.service.visualization.request.SearchRequest;
import com.dataparse.server.service.visualization.request_builder.ExecutorOptions;
import com.dataparse.server.service.visualization.request_builder.IQueryExecutor;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class VisualizationService {

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private BookmarkStateStorage stateStorage;

    @Autowired
    @Qualifier("defaultQueryExecutorStrategy")
    private IQueryExecutorStrategy queryExecutorStrategy;

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    public ProgressAwareIterator<Map<String, Object>> getIterator(SearchIndexRequest request, BookmarkState state) {
        if(request.getParams().isRaw()){
            TableBookmark bookmark = tableRepository.getTableBookmark(request.getTableBookmarkId());
            TableSchema schema = bookmark.getTableSchema();
            Map<String, Object> options = new HashMap<>();
            options.put(ExecutorOptions.USE_TT, schema.getDescriptor().isUseTuplewiseTransform());
            options.put(ExecutorOptions.FROM, request.getFrom());
            SearchRequest r = new SearchRequest(bookmark.getDatadoc().getId(),
                                                bookmark.getId(),
                                                schema.getId(),
                                                schema.getAccountId(),
                                                schema.getExternalId(),
                                                state.getColumnList(),
                                                state.getQueryParams(),
                                                options);
            return queryExecutorStrategy.get(schema).scroll(r);
        } else {
            Map<String, Boolean> rowsExpandedState = TreeUtils.prefixMap(state.getRowsCollapsedState(), Collections.singletonList(Tree.ROOT_KEY));
            rowsExpandedState.put(Tree.ROOT_KEY, true);
            return scrollTree(request, rowsExpandedState);
        }
    }

    public ProgressAwareIterator<Map<String, Object>> scrollTree(SearchIndexRequest request, Map<String, Boolean> rowsExpandedState) {
        Tree<Map<String, Object>> tree = new Tree<>();
        tree.getRoot().setData(new HashMap<>());

        SearchIndexResponse response = search(request);
        Iterator<Tree.Node<Map<String, Object>>> treeIterator = response.getData().iterator(rowsExpandedState);

        return new ProgressAwareIterator<Map<String, Object>>() {

            @Override
            public Tree.Node<Map<String, Object>> getHeaders() {
                return response.getHeaders();
            }

            @Override
            public Map<String, Object> getTotals() {
                return response.getTotals();
            }

            @Override
            public Double getComplete() {
                return 0.;
            }

            @Override
            public boolean hasNext() {
                return treeIterator.hasNext();
            }

            @Override
            public Map<String, Object> next() {
                Tree.Node<Map<String, Object>> next = treeIterator.next();
                if(next == null){
                    return null;
                }
                Map<String, Object> result = new HashMap<>(next.getData());
                if(!request.getParams().getAggs().isEmpty()) {
                    int i = 0;
                    for (Tree.Node node : next.path()) {
                        if (i > 0) {
                            result.put(request.getParams().getAggs().get(i - 1).key(), node.getKey());
                        }
                        i++;
                    }
                }
                return result;
            }
        };
    }

//todo refactor
    public List<Filter> refreshFilters(RefreshFiltersBulkRequest request) {
        TableBookmark bookmark = tableRepository.getTableBookmark(request.getTableBookmarkId());
        List<Col> cols = bookmark.getState().getColumnList();
        QueryParams params = bookmark.getState().getQueryParams();
        String tabId = String.valueOf(bookmark.getId());

        BookmarkStateBuilder builder = bookmarkBuilderFactory.create(bookmark);
        return request.getFilters().stream().map(filterName -> {
            Filter filter = params.getFilters().stream().filter(f -> f.getField().equals(filterName)).findFirst().get();
            builder.updateFilter(params, cols, filter, tabId, null, true);
            return filter;
        }).collect(Collectors.toList());
    }

    public Filter refreshFilter(RefreshFilterRequest request){
        QueryParams params;
        TableBookmark bookmark = tableRepository.getTableBookmark(request.getTableBookmarkId());
        List<Col> cols = bookmark.getState().getColumnList();
        String tabId;
        if(request.getParams() == null){
            params = bookmark.getState().getQueryParams();
            tabId = bookmark.getId().toString();
        } else {
            params = request.getParams();
            tabId = Auth.get().getSessionId();
        }
        BookmarkStateBuilder builder = bookmarkBuilderFactory.create(bookmark);
        Filter filter = params.getFilters().stream().filter(f -> f.getField().equals(request.getFilter())).findFirst().get();
        builder.updateFilter(params, cols, filter, tabId, null, true);
        return filter;
    }

    @SuppressWarnings("unchecked")
    public SearchIndexResponse search(SearchIndexRequest request) {
        QueryParams params;
        TableBookmark bookmark = tableRepository.getTableBookmark(request.getTableBookmarkId(), request.getStateId());
        bookmark.setCurrentState(request.getStateId());
        TableSchema schema = bookmark.getTableSchema();
        BookmarkState state = bookmark.getState();
        // it means that bookmark creation is in progress
        if(state == null) {
            return new SearchIndexResponse();
        }
        List<Col> columns = state.getColumnList();
        if(request.getParams() == null) {
            params = bookmark.getState().getQueryParams();
        } else {
            params = request.getParams();
        }
        Map<String, Object> options = new HashMap<>();
        options.put(ExecutorOptions.USE_TT, schema.getDescriptor().isUseTuplewiseTransform());
        options.put(ExecutorOptions.FROM, request.getFrom());
        options.put(ExecutorOptions.REQUEST_UID, request.getScrollId());
        SearchRequest r = new SearchRequest(bookmark.getDatadoc().getId(),
                                            bookmark.getId(),
                                            schema.getId(),
                                            schema.getAccountId(),
                                            schema.getExternalId(),
                                            columns,
                                            params,
                                            options);

        // please don't curse me
        // after data has been retrieved from ES/BQ we sum up values of fields having VALUE aggregation function for grand total on back-end
        // https://trello.com/c/vEIFpOAP/1522-i-added-a-new-dataset-a-few-fixes
        IQueryExecutor queryExecutor = queryExecutorStrategy.get(schema);
        SearchIndexResponse response = queryExecutor.search(r);
        if(!params.isRaw()){
            Map<String, Col> columnsMap = Maps.uniqueIndex(columns, c -> c.getField());

            List<String> keys = params.getShows()
                    .stream()
                    .filter(s -> s.getOp().equals(Op.VALUE) && columnsMap.get(s.getField()).getType().equals(DataType.DECIMAL))
                    .map(s -> s.key())
                    .collect(Collectors.toList());


            Map<String, Double> values = new HashMap<>();
            if(!keys.isEmpty()){
                // it's better to use Tree.Node.iterator() here (Post-Order/Post-Walk) in order to sum across all tree levels
                // but we will do it only for grand total
                for(String key : keys) {
                    for(Tree.Node<Map<String,Object>> node : response.getData().getChildren()){
                        values.compute(key, (k, v) -> {
                             Object childValue = node.getData().get(k);
                             if(childValue != null){
                                 if(v == null){
                                     v = 0.;
                                 }
                                 return v + (Double) childValue;
                             }
                             return v;
                        });
                    }
                }
            }
            Map<String, Object> data = response.getData().getData();
            if(data == null && values.size() > 0) {
                response.getData().setData((Map) values);
            } else if(values.size() > 0) {
                response.getData().getData().putAll(values);
            }
        }

        if(params.getShows() != null && params.getShows().size() != 0) {
            sortResponseDataNode(response.getData().getChildren(), params.getShows());
        } else if(params.getAggs() != null && params.getAggs().size() != 0) {
            sortResponseAggNodes(response.getData(), params.getAggs());
        }

        return response;
    }

    private void sortResponseDataNode(List<Tree.Node<Map<String, Object>>> data, List<Show> shows) {
        List<Show> showsByPriority = shows.stream()
                .filter(show -> show.getSettings().getSort() != null)
                .sorted(Comparator.comparing(s1 -> -s1.getSettings().getSort().getPriority()))
                .collect(Collectors.toList());

        for(Show show : showsByPriority) {
            DataNodeComparator dataNodeComparator = new DataNodeComparator(show.key(), show.getSettings().getSort());
            data.sort(dataNodeComparator);
        }
    }

    @SuppressWarnings("unchecked")
    private void sortResponseAggNodes(Tree.Node aggNode, List<Agg> aggs) {
        if(aggNode.depth() - 1 < aggs.size()) {

            // sort in depth
            List<Tree.Node> dataList = aggNode.getChildren();
            for(Tree.Node node : dataList) {
                sortResponseAggNodes(node, aggs);
            }

            // sort current node data
            Agg agg = aggs.get(aggNode.depth() - 1);
            AggNodeComparator aggNodeComparator = new AggNodeComparator(agg.getSettings().getSort());
            dataList.sort(aggNodeComparator);
        }
    }

    public String autocomplete(AdvancedFiltersAutoCompleteRequest request) {
        BookmarkState state = stateStorage.get(request.getBookmarkStateId(), true).getState();
        TableSchema schema = tableRepository.getSchemaByBookmarkId(request.getTabId());
        return queryExecutorStrategy.get(schema).getAutoCompleteList(
                request.getQuery(),
                request.getCursor(),
                state.getColumnList(),
                state.getQueryParams().getFilters());
    }

    public String getQueryFromFilters(FromFiltersRequest request) {
        BookmarkState state = stateStorage.get(request.getBookmarkStateId(), true).getState();
        TableSchema schema = tableRepository.getSchemaByBookmarkId(request.getTabId());
        return queryExecutorStrategy.get(schema).fromFilters(state.getColumnList(), state.getQueryParams().getFilters());
    }

    public String getFiltersFromQuery(ToFiltersRequest request) {
        BookmarkState state = bookmarkBuilderFactory.create(request.getTableBookmarkId()).build();
        TableSchema schema = tableRepository.getSchemaByBookmarkId(request.getTableBookmarkId());
        return queryExecutorStrategy.get(schema).toFilters(
                request.getQuery(),
                state.getColumnList(),
                state.getQueryParams().getFilters());
    }
}
