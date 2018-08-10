package com.dataparse.server.service.visualization.bookmark_state;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.service.visualization.request.*;
import com.dataparse.server.service.visualization.request_builder.*;
import com.dataparse.server.util.stripedexecutor.*;
import lombok.extern.slf4j.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

@Slf4j
public class BookmarkStateBuilder {

    private static final Integer CARDINALITY_TOP_COUNT = 5;
    public static final String DEFAULT_STATE_NAME = "Default";

    private TableBookmark bookmark;

    private IQueryExecutor queryExecutor;

    private FilterQueryExecutor filterQueryExecutor;

    private BookmarkStateBuilder(IQueryExecutor queryExecutor,
                                 FilterQueryExecutor filterQueryExecutor,
                                 TableBookmark bookmark) {
        this.queryExecutor = queryExecutor;
        this.filterQueryExecutor = filterQueryExecutor;
        this.bookmark = bookmark;
    }

    public Boolean isFastFilterExecutor() {
        return queryExecutor instanceof IFastFilters;
    }

    public static BookmarkStateBuilder create(IQueryExecutor queryExecutor,
                                              FilterQueryExecutor filterQueryExecutor,
                                              TableBookmark bookmark) {
        return new BookmarkStateBuilder(queryExecutor, filterQueryExecutor, bookmark);
    }

    public BookmarkState build() {
        BookmarkState state = new BookmarkState();
        BookmarkStateId stateId = new BookmarkStateId(bookmark.getId(), UUID.randomUUID(), Auth.get().getUserId());
        state.setBookmarkStateId(stateId);
        state.setChangeable(true);
        state.setStateName(DEFAULT_STATE_NAME);
        return state;
    }

    private List<ColumnInfo> getAddedColumns(List<ColumnInfo> oldColumns, List<ColumnInfo> newColumns){
        List<ColumnInfo> addedColumns = new ArrayList<>(newColumns);
        for(ColumnInfo newC : newColumns){
            Optional<ColumnInfo> oldC = oldColumns.stream()
                    .filter(c -> c.getName().equals(newC.getName()) && c.getType().equals(newC.getType()))
                    .findFirst();
            oldC.ifPresent(columnInfo -> addedColumns.remove(newC));
        }
        return addedColumns;
    }

    private List<ColumnInfo> getRemovedColumns(List<ColumnInfo> oldColumns, List<ColumnInfo> newColumns){
        List<ColumnInfo> removedColumns = new ArrayList<>(oldColumns);
        for(ColumnInfo oldC : oldColumns){
            Optional<ColumnInfo> newC = newColumns.stream()
                    .filter(c -> c.getName().equals(oldC.getName()) && c.getType().equals(oldC.getType()))
                    .findFirst();
            newC.ifPresent(columnInfo -> removedColumns.remove(oldC));
        }
        return removedColumns;
    }

    private List<Col> getColumnsUsedInQueryParams(QueryParams params, List<Col> columns){
        List<Col> columnsUsedInQueryParams = new ArrayList<>();
        for(Col col: columns){
            if(params.getShows().stream().anyMatch(s -> s.getField().equals(col.getField()))){
                columnsUsedInQueryParams.add(col);
                continue;
            }
            if(params.getAggs().stream().anyMatch(a -> a.getField().equals(col.getField()))){
                columnsUsedInQueryParams.add(col);
                continue;
            }
            if(params.getPivot().stream().anyMatch(p -> p.getField().equals(col.getField()))){
                columnsUsedInQueryParams.add(col);
                continue;
            }
            if(params.getFilters().stream().anyMatch(f -> f.getField().equals(col.getField()) && f.isActive())){
                columnsUsedInQueryParams.add(col);
            }
        }
        return columnsUsedInQueryParams;
    }

    private List<Col> getColumnsUsedInQuery(BookmarkState state, List<ColumnInfo> columns){
        List<Col> cols = Col.from(columns);
        List<Col> columnsUsedInPendingParams = getColumnsUsedInQueryParams(state.getPendingQueryParams(), cols);
        if(!state.isAutoRefresh() && !columnsUsedInPendingParams.isEmpty()){
            return columnsUsedInPendingParams;
        }
        return getColumnsUsedInQueryParams(state.getQueryParams(), cols);
    }

    public static void removeColumns(BookmarkState state, List<ColumnInfo> columns){
        List<Col> removed = new ArrayList<>();
        for(Col col: state.getColumnList()){
            if(columns.stream().anyMatch(c -> c.getName().equals(col.getOriginalField()))){
                removed.add(col);
                state.getQueryParams().getFilters().removeIf(f -> f.getField().equals(col.getField()));
                state.getRawShowList().removeIf(s -> s.getField().equals(col.getField()));
            }
        }
        state.getColumnList().removeAll(removed);
        repopulateDictionary(state);
    }

    public static void removeColumnsByNativeOne(BookmarkState state, List<Col> columns){
        List<Col> removed = new ArrayList<>();
        for(Col col: state.getColumnList()){
            if(columns.stream().anyMatch(c -> c.getName().equals(col.getOriginalField()))){
                removed.add(col);
                state.getQueryParams().getFilters().removeIf(f -> f.getField().equals(col.getField()));
                state.getRawShowList().removeIf(s -> s.getField().equals(col.getField()));
            }
        }
        state.getColumnList().removeAll(removed);
        repopulateDictionary(state);
    }

    public void addColumns(BookmarkState state, List<ColumnInfo> columns){
        List<Col> added = new ArrayList<>();
        for(ColumnInfo column : columns){
            if(state.getColumnList().stream().noneMatch(c -> c.getOriginalField().equals(column.getName()))) {
                added.add(Col.from(column));
            }
        }
        List<Filter> filters = createFilters(added, state.getTabId().toString());
        state.getColumnList().addAll(added);
        state.getQueryParams().getFilters().addAll(filters);
        state.getRawShowList().addAll(added.stream().map(s -> new RawShow(s.getField())).collect(Collectors.toList()));
        repopulateDictionary(state);
    }

    private void updateColumns(BookmarkState state, List<ColumnInfo> columns){
        for(Col col: state.getColumnList()){
            ColumnInfo match = columns.stream().filter(c -> c.getName().equals(col.getOriginalField())).findFirst().orElse(null);
            if(match != null){
                col.updateFrom(match);
            }
        }
    }

    private void sortShows(BookmarkState state, List<ColumnInfo> columns){
        Map<String, Integer> columnPosition = columns.stream().collect(Collectors.toMap(c -> c.getName(), c -> columns.indexOf(c)));
        Map<String, String> fieldToOriginalField = state.getColumnList().stream().collect(Collectors.toMap(c -> c.getField(), c -> c.getOriginalField()));
        state.getQueryParams().getShows().sort((o1, o2) -> {
            Integer posC1 = columnPosition.get(fieldToOriginalField.get(o1.getField()));
            Integer posC2 = columnPosition.get(fieldToOriginalField.get(o2.getField()));
            return posC1 == null || posC2 == null ? -1 : posC1.compareTo(posC2);
        });
        state.getPendingQueryParams().getShows().sort((o1, o2) -> {
            Integer posC1 = columnPosition.get(fieldToOriginalField.get(o1.getField()));
            Integer posC2 = columnPosition.get(fieldToOriginalField.get(o2.getField()));
            return posC1 == null || posC2 == null ? -1 : posC1.compareTo(posC2);
        });
    }

    private void sortColumns(BookmarkState state, List<ColumnInfo> columns){
        Map<String, Integer> columnPosition = columns.stream().collect(Collectors.toMap(c -> c.getName(), c -> columns.indexOf(c)));
        state.getColumnList().sort((o1, o2) -> {
            Integer posC1 = columnPosition.get(o1.getOriginalField());
            Integer posC2 = columnPosition.get(o2.getOriginalField());
            return posC1 == null || posC2 == null ? -1 : posC1.compareTo(posC2);
        });
    }

    private void reset(BookmarkState state, List<Col> cols){
        state.setInstantSearch(bookmark.getTableSchema().getEngineType().equals(EngineType.ES));
        state.setColumnList(cols);
        state.setQueryParams(new QueryParams());
        state.setPendingQueryParams(new QueryParams());
        state.setBeforeViewRawParams(new QueryParams());
        repopulateDictionary(state);
        populateLists(state);
    }

    public BookmarkState update(BookmarkState state, Descriptor newDescriptor, boolean sourceChanged) {
        state = state.copy();
        if(bookmark.getTableSchema().getDescriptor() == null) {
            log.info("Creating state for bookmark[id={}]...", state.getTabId());
            reset(state, Col.from(newDescriptor.getColumns()));
        } else {
            state.getNotifications().clear();
            if(!sourceChanged) {
                List<ColumnInfo> removedColumns = getRemovedColumns(bookmark.getTableSchema().getDescriptor().getColumns(),
                                                                    newDescriptor.getColumns());
                List<Col> columnsUsedInQuery = getColumnsUsedInQuery(state, removedColumns);
                if (!columnsUsedInQuery.isEmpty()) {
                    reset(state, Col.from(newDescriptor.getColumns()));
                } else {
                    log.info("Updating state of bookmark[id={}]...", state.getTabId());
                    removeColumns(state, removedColumns);
                    updateColumns(state, newDescriptor.getColumns());
                    List<ColumnInfo> addedColumns = getAddedColumns(bookmark.getTableSchema().getDescriptor().getColumns(),
                                                                    newDescriptor.getColumns());
                    addColumns(state, addedColumns);
                    sortColumns(state, newDescriptor.getColumns());
                    repopulateDictionary(state);
                    if(state.getQueryParams().isRaw()
                       && state.getQueryParams().getShows().size() == newDescriptor.getColumns().size()
                            && state.getPendingQueryParams().isRaw()
                            && state.getPendingQueryParams().getShows().size() == newDescriptor.getColumns().size()){
                        sortShows(state, newDescriptor.getColumns());
                    }
                    updateFilters(state);
                }
            } else {
                reset(state, Col.from(newDescriptor.getColumns()));
            }
        }
        return state;
    }

    public static void repopulateDictionary(BookmarkState state) {
        state.getShowList().clear();
        state.getAggList().clear();
        state.getSortList().clear();

        state.getPivotSortList().clear();
        state.getAggSortList().clear();

        state.getColumnList().forEach(col -> {
            List<Op> showOps = new ArrayList<>();
            showOps.add(null);
            if (col.getType().equals(DataType.DECIMAL)) {
                showOps.addAll(Op.getCommonOps());
                showOps.addAll(Op.getNumberOps());
            } else if (col.getType().equals(DataType.DATE)) {
                showOps.addAll(Op.getCommonOps());
            } else {
                showOps.addAll(Op.getCommonOps());
            }
            List<Show> shows = new ArrayList<>();
            showOps.forEach(op -> shows.add(new Show(col.getField(), op)));
            state.getShowList().addAll(shows);

            List<AggOp> aggOps;
            if(!col.isDisableFacets()) {
                if (col.getType().equals(DataType.DATE)) {
                    aggOps = AggOp.getDateOps();
                } else {
                    aggOps = Collections.singletonList(null);
                }
                aggOps.forEach(op -> {
                    Agg agg = new Agg(col.getField(), op);
                    state.getAggList().add(agg);
                });
            }
        });
    }

    private List<Filter> createFilters(List<Col> columns, String tabId){
        List<Filter> filters = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(columns.size());
        columns.forEach(c -> {
            createFilter(c, tabId, filter -> {
                filter.setShowSearch(filter.getList().size() >= 10);
                filter.setSelected(false);
                filters.add(filter);
                latch.countDown();
            });
        });
        try {
            latch.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        Map<String, Col> columnsMap = columns.stream().collect(Collectors.toMap(col -> col.getField(), col -> col));
        // select top 5 cardinality filters
        filters.stream()
                .filter(f -> !(f instanceof DateRangeFilter))
                .filter(f -> f.getCardinality() > 1)
                .sorted(Comparator.comparing(Filter::getCardinality))
                .limit(CARDINALITY_TOP_COUNT)
                .forEach(f -> f.setSelected(true));
        // also select all date filters
        filters.stream()
                .filter(f -> f instanceof DateRangeFilter)
                .forEach(f -> f.setSelected(true));
        // and sort by name
        filters.sort((f1, f2) -> {
            Col column1 = columnsMap.get(f1.getField());
            Col column2 = columnsMap.get(f2.getField());
            return column1.getName().compareTo(column2.getName());
        });
        return filters;
    }

    private void populateLists(BookmarkState state) {
        List<Col> columnsWithFilters = state.getColumnList().stream()
                .filter(c -> !c.isDisableFacets())
                .collect(Collectors.toList());

        state.getQueryParams().setFilters(createFilters(columnsWithFilters, state.getTabId().toString()));
        state.getShowList().stream()
                .filter(s -> s.getOp() == null)
                .limit(100)
                .forEach(s -> state.getQueryParams().getShows().add(s));
        state.setRawShowList(state.getColumnList()
                .stream().map(s -> new RawShow(s.getField())).collect(Collectors.toList()));
        state.getRawShowList().stream()
                .limit(100)
                .forEach(s -> s.setSelected(true));
        state.setPendingQueryParams(state.getQueryParams().copy());
    }

    private class NumberComparator implements Comparator<Number> {
        public int compare(Number a, Number b) {
            if(a == null){
                return b == null ? 0 : -1;
            } else if (b == null){
                return 1;
            }
            return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString()));
        }
    }

    public void updateFilters(BookmarkState state) {
        CountDownLatch latch = new CountDownLatch(state.getQueryParams().getFilters().size());
        for (Filter filter : state.getQueryParams().getFilters()) {
            boolean updateScheduled = updateFilter(state.getQueryParams(), state.getColumnList(), filter, state.getTabId().toString(), updatedFilter -> {
                latch.countDown();
            }, true);
            if(!updateScheduled){
                latch.countDown();
            }
        }
        try {
            latch.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public void updateFilters(BookmarkState state, List<String> except, List<Col> cols, Consumer<Filter> onFilterRefreshed, Boolean runImmediately) {
        for (Filter filter : state.getQueryParams().getFilters()) {
            if (except != null && except.contains(filter.getField())) {
                continue;
            }
            filterQueryExecutor.removeFromWaitQueue(state.getTabId(), filter.getField());
            updateFilter(state.getQueryParams(), cols, filter, state.getTabId().toString(), onFilterRefreshed, runImmediately);
        }
    }

    public boolean updateFilter(QueryParams params, List<Col> cols, Filter filter, String tabId, Consumer<Filter> onFilterRefreshed, Boolean runImmediately){
        if(filter.isHidden() || !filter.isSelected()){
            return false;
        }
        if(!runImmediately) {
            filterQueryExecutor.removeFromWaitQueue(Long.parseLong(tabId), filter.getField());
            filterQueryExecutor.removeOneFromExecution(tabId, filter.getField());
        }

        StripedRunnable task = new StripedRunnable() {
            @Override
            public Object getStripe() {
                return tabId + filter.getField();
            }

            @Override
            public void run() {
                try {
                    Col col = cols
                            .stream()
                            .filter(c -> c.getField().equals(filter.getField()))
                            .findFirst()
                            .get();
                    if (!col.isDisableFacets()) {
                        QueryParams paramsCopy = params.copy();
                        paramsCopy.getFilters().removeIf(f -> f.getField().equals(filter.getField()));

                        switch (col.getType()) {
                            case DECIMAL:
                            case DATE:
                            case TIME:
                                updateRangeFilter(paramsCopy, cols, col, filter);
                            case STRING:
                            default:
                                updateListFilter(paramsCopy, cols, col, filter);
                                break;
                        }
                        if (onFilterRefreshed != null) {
                            onFilterRefreshed.accept(filter);
                        }

                    }
                } catch (Exception e) {
                    log.error("Can't refresh filter", e);
                    if (onFilterRefreshed != null) {
                        onFilterRefreshed.accept(null);
                    }
                }
            }
        };
        if(runImmediately) {
            filterQueryExecutor.run(task);
        } else {
            filterQueryExecutor.addToQueue(Long.parseLong(tabId), filter.getField(), task);
        }
        return true;
    }

    public List<Filter> resetFilters(BookmarkState state){
        List<Col> columnsWithFilters = state.getColumnList().stream()
                .filter(c -> !c.isDisableFacets())
                .collect(Collectors.toList());
        List<Filter> filters = createFilters(columnsWithFilters, state.getTabId().toString());
        state.getQueryParams().getFilters().forEach(f -> {
            Filter newFilter = filters.stream().filter(filter -> filter.getField().equals(f.getField())).findFirst().get();
            newFilter.setSelected(f.isSelected());
        });
        state.getQueryParams().setFilters(filters);
        return filters;
    }

    public void createFilter(Col col, String tabId, Consumer<Filter> onFilterCreated) {
        filterQueryExecutor.run(
                new StripedRunnable() {
                    @Override
                    public Object getStripe() {
                        return tabId + col.getField();
                    }

                    @Override
                    public void run() {
                        try {
                            Filter filter = null;
                            switch (col.getType()) {
                                case DATE:
                                    filter = new DateRangeFilter(col.getField());
                                    updateRangeFilter(new QueryParams(), Collections.singletonList(col), col, filter);
                                    break;
                                case TIME:
                                    filter = new IntegerRangeFilter(col.getField());
                                    updateRangeFilter(new QueryParams(), Collections.singletonList(col), col, filter);
                                    break;
                                case DECIMAL:
                                    filter = new DoubleRangeFilter(col.getField());
                                    updateRangeFilter(new QueryParams(), Collections.singletonList(col), col, filter);
                                    break;
                            }
                            // all other types
                            if (filter == null) {
                                filter = new Filter(col.getField(), new ArrayList<>());
                            }
                            updateListFilter(new QueryParams(), Collections.singletonList(col), col, filter);
                            if (filter.getList().size() <= 3) {
                                filter.setListMode(true);
                            }
                            updateCardinality(new QueryParams(), Collections.singletonList(col), col, filter);
                            onFilterCreated.accept(filter);
                        } catch (Exception e){
                            log.error("Can't create filter", e);
                            onFilterCreated.accept(null);
                        }
                    }
                });
    }


    @SuppressWarnings("unchecked")
    private void updateRangeFilter(QueryParams params, List<Col> cols, Col col, Filter filter) {
        FieldStatsRequest statsRequest = new FieldStatsRequest();
        statsRequest.setDatadocId(bookmark.getDatadoc().getId());
        statsRequest.setBookmarkId(bookmark.getId());
        statsRequest.setTableId(bookmark.getTableSchema().getId());
        statsRequest.setAccountId(bookmark.getTableSchema().getAccountId());
        statsRequest.setExternalId(bookmark.getTableSchema().getExternalId());
        statsRequest.setFieldName(col.getField());
        statsRequest.setParams(params);
        statsRequest.setColumns(cols);
        Map<String, Double> fieldStats = queryExecutor.getFilterStats(statsRequest);
        Number min, max;
        if (col.getType().equals(DataType.DECIMAL)) {
            min = fieldStats.get("min");
            max = fieldStats.get("max");
        } else if (col.getType().equals(DataType.DATE)) {
            min = fieldStats.get("min");
            max = fieldStats.get("max");

            min = new DateTime(min.longValue(), DateTimeZone.UTC).withTime(0, 0, 0, 0).getMillis();
            max = new DateTime(max.longValue(), DateTimeZone.UTC).withTime(23, 59, 59, 999).getMillis();
        } else {
            min = fieldStats.get("min");
            max = fieldStats.get("max");
            if(min != null){
                min = min.longValue();
            }
            if(max != null){
                max = max.longValue();
            }
        }
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter) filter;
        if(numberRangeFilter.getValue2() == null){
            numberRangeFilter.setValue2(max);
            numberRangeFilter.setMax(max);
        }
        if(numberRangeFilter.getValue1() == null){
            numberRangeFilter.setValue1(min);
            numberRangeFilter.setMin(min);
        }
        if (numberRangeFilter.getMin() != null && numberRangeFilter.getValue1().equals(numberRangeFilter.getMin())) {
            numberRangeFilter.setValue1(min);
        }
        if (numberRangeFilter.getMax() != null && numberRangeFilter.getValue2().equals(numberRangeFilter.getMax())) {
            numberRangeFilter.setValue2(max);
        }
        numberRangeFilter.setMin(min);
        numberRangeFilter.setMax(max);
        if (new NumberComparator().compare(numberRangeFilter.getValue1(), min) < 0) {
            numberRangeFilter.setValue1(min);
        }
        if (new NumberComparator().compare(numberRangeFilter.getValue2(), max) > 0) {
            numberRangeFilter.setValue2(max);
        }
    }

    private void updateCardinality(QueryParams params, List<Col> cols, Col col, Filter filter) {
        CardinalityRequest statsRequest = new CardinalityRequest();
        statsRequest.setDatadocId(bookmark.getDatadoc().getId());
        statsRequest.setBookmarkId(bookmark.getId());
        statsRequest.setTableId(bookmark.getTableSchema().getId());
        statsRequest.setAccountId(bookmark.getTableSchema().getAccountId());
        statsRequest.setExternalId(bookmark.getTableSchema().getExternalId());
        statsRequest.setFieldName(col.getField());
        statsRequest.setParams(params);
        statsRequest.setColumns(cols);
        Long cardinality = queryExecutor.getFilterCardinality(statsRequest);
        filter.setCardinality(cardinality);
    }

    private void updateListFilter(QueryParams params, List<Col> cols, Col col, Filter filter) {
        FieldAggsRequest aggsRequest = new FieldAggsRequest();
        aggsRequest.setDatadocId(bookmark.getDatadoc().getId());
        aggsRequest.setBookmarkId(bookmark.getId());
        aggsRequest.setTableId(bookmark.getTableSchema().getId());
        aggsRequest.setAccountId(bookmark.getTableSchema().getAccountId());
        aggsRequest.setExternalId(bookmark.getTableSchema().getExternalId());
        aggsRequest.setFieldName(col.getField());
        aggsRequest.setParams(params);
        aggsRequest.setColumns(cols);
        aggsRequest.setCount(10L);
        aggsRequest.setSearch(filter.getSearch());
        Map<Object, Long> fieldAggs = queryExecutor.getFilterTerms(aggsRequest);
        List<FilterValue> listCopy = new ArrayList<>(filter.getList());
        listCopy.forEach(f -> {
            Long count = fieldAggs.get(f.getKey());
            if (count != null) {
                f.setDocCount(count);
                fieldAggs.remove(f.getKey());
            } else {
                f.setDocCount(0L);
            }
        });
        fieldAggs.forEach((key, count) -> {
            listCopy.add(new FilterValue(false, true, key, count));
        });
        listCopy.stream().filter(f -> !f.isSelected() && f.isShow() && f.getDocCount() == 0)
                .collect(Collectors.toList()).forEach(listCopy::remove);
        filter.setList(listCopy);
    }

}
