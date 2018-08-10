package com.dataparse.server.coverage.utils.mockTesting;

import com.dataparse.server.controllers.api.table.SearchIndexRequest;
import com.dataparse.server.controllers.api.table.SearchIndexResponse;
import com.dataparse.server.coverage.utils.socket.SocketManager;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.visualization.Tree;
import com.dataparse.server.service.visualization.bookmark_state.event.BookmarkStateChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.BookmarkVizCompositeStateChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.StateChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.aggs.AggAddEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.filter.FilterChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.filter.FilterToggleAllEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.filter.FilterToggleEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.filter.SearchChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.request.ApplyRequestEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.shows.ShowAddEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.shows.ShowChangeSortEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.shows.ShowRemoveEvent;
import com.dataparse.server.service.visualization.bookmark_state.filter.DateRangeFilter;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static java.net.URLDecoder.decode;


@Slf4j
@Service
public class DatadocFiltersTester {

    @Getter
    private String testsAnswersDirectoryTemplate = "coverage/results/queryScenarioTest/%s.%s.json";

    private ObjectMapper mapper = new ObjectMapper();
    private SocketManager socketManager;

    private UUID stateId;
    private Long bookmarkId;
    private Long tableId;

    private List<Filter> datadocFilters;
    private List<ColumnInfo> columns;
    @Setter
    Map<String, Object> rowPreview;

    public void setup(Long tableId, Long bookmarkId, UUID stateId, List<Filter> datadocFilters, List<ColumnInfo> columns,
                      List<String> sessionCookies, Long userId) {
        // socket with session
        this.socketManager = new SocketManager(sessionCookies, userId);

        // data required for search operations
        this.tableId = tableId;
        this.bookmarkId = bookmarkId;
        this.stateId = stateId;
        this.datadocFilters = datadocFilters;
        this.columns = columns;
    }

    // =================================================================================================================

    // get current state data
    public SearchIndexRequest buildSearchRequest(QueryParams params) {
        return SearchIndexRequest.builder()
                .from(0L).stateId(stateId).tableBookmarkId(bookmarkId).tableId(tableId).params(params)
                .externalId(null).scrollId(null)
                .build();
    }

    private FilterValue getFilterFromExample(Filter filter) {

        Object value;

        if(columns.size() > 0) {
            ColumnInfo columnInfo = columns.stream().filter(col -> col.getAlias().equals(filter.getField()))
                    .collect(Collectors.toList()).get(0);
            value = columnInfo.getExampleValue();
        //} else if(rowPreview != null) {
        //    value = rowPreview.get(filter.getField());
        } else {
            return null;
        }

        FilterValue filterValue = new FilterValue();
        filterValue.setSelected(true);
        filterValue.setShow(true);
        filterValue.setKey(value);
        filterValue.setDocCount(null);

        log.info("Got custom filter value ({}) for col {}.", value, filter.getField());
        return filterValue;
    }

    private void selectFilter(List<FilterValue> list) {
        // select first not null value from filter
        FilterValue value = getFilterFromExample(getAvailableFilter(Arrays.asList("c1", "c0"), "c1"));

        if(value != null) {
            list.add(value);
        }
    }

    private void setC1Filter() {
        // filter by c1='xx' (or another column)
        Filter availableFilter = getAvailableFilter(Arrays.asList("c1", "c0"), "c1");
        Filter singleFilter = datadocFilters.stream()
                .filter(filter -> filter.getField().equals(availableFilter.getField()))
                .findFirst().get();
        singleFilter.setSelected(true);
        selectFilter(singleFilter.getList());
    }

    public SearchIndexRequest getQuery1Request() {
        return getQueryRequest("");
    }

    // get second query request
    public SearchIndexRequest getQuery2Request() {
        setC1Filter();

        // search='yy' (from c0 column)
        String currentSearch = getSearchMatch();

        return getQueryRequest(currentSearch);
    }

    // get third query request
    public SearchIndexRequest getQuery3Request() {
        setC1Filter();

        // search='yy' (from c0 column)
        String currentSearch = getSearchMatch();

        // aggs
        Filter availableFilter = getAvailableFilter(Arrays.asList("c1", "c0"), "c1");
        List<Agg> aggs = Collections.singletonList(new Agg(availableFilter.getField(), null, new AggSettings(new AggSort(SortDirection.ASC, false))));

        QueryParams queryParams = new QueryParams();
        queryParams.setFilters(datadocFilters);
        queryParams.setSearch(currentSearch);
        queryParams.setShows(getShows(Op.VALUE, false));
        queryParams.setAggs(aggs);

        SearchIndexRequest request = new SearchIndexRequest();
        request.setTableBookmarkId(bookmarkId);
        request.setParams(queryParams);

        return request; // the same for now
    }

    // get query answers for results matching
    public SearchIndexResponse getQueryAnswer(String fileName, String step) {
        try {
            String resourcePath = String.format(testsAnswersDirectoryTemplate, fileName, step);
            String filePath = getClass().getClassLoader().getResource(resourcePath).getPath();
            String rawFileData = readFile(filePath, Charset.defaultCharset());

            return mapper.readValue(rawFileData, SearchIndexResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // remove ES metadata field and others non constant fields(if will be needed)
    public List<Tree.Node<Map<String, Object>>> getCleanRowsData(SearchIndexResponse source) {

        List<Tree.Node<Map<String, Object>>> listCopy;

        // check for aggregation:
        // agg sends us data as rows with nested rows ; simple search sends us rows in child node
        if (source.getData().getData() != null && source.getData().getData().size() > 0) {
            listCopy = new ArrayList<>(Arrays.asList(new Tree.Node<>(source.getData().getData())));
        } else {
            listCopy = new ArrayList<>(source.getData().getChildren());
        }

        // declare odd fields and metadata
        String esMetadataKey = "es_metadata_id";
        String clusterSizeKey = "$$cluster_size";

        for (Tree.Node<Map<String, Object>> node : listCopy) {
            Map<String, Object> nodeMap = node.getData();
            nodeMap.remove(esMetadataKey);
            nodeMap.remove(clusterSizeKey);

            // temporary do not check aggregations
            List<String> oddKeys = nodeMap.keySet().stream().filter(key -> key.startsWith("VALUE_")).collect(Collectors.toList());
            for(String key : oddKeys) {
                nodeMap.remove(key);
            }
        }

        // filter empty values
        listCopy = listCopy.stream().filter(l -> l.getData().values().stream().anyMatch(Objects::nonNull)).collect(Collectors.toList());

        return listCopy;
    }

    private boolean checkFilterIfExists(String field) {
        return datadocFilters.stream().anyMatch(datadocFilter -> datadocFilter.getField().equals(field));
    }

    // set filters from left panel showed
    public void sendToggleC1C2C3() throws IOException, InterruptedException {
        FilterToggleAllEvent filterToggleAllEvent = new FilterToggleAllEvent();
        filterToggleAllEvent.setSelected(false);
        sendEvent(filterToggleAllEvent);

        for (String filter : getFiltersToShow()) {
            // send socket event for backend
            FilterToggleEvent filterToggleEvent = new FilterToggleEvent();
            filterToggleEvent.setField(filter);
            filterToggleEvent.setSelected(true);
            sendEvent(filterToggleEvent);

            // set local test filter shown
            Filter localFilter = getFilterByField(filter);
            if (localFilter != null) {
                localFilter.setSelected(true);
            }
        }
    }

    // selecting only 1-2-3 fields for showing
    public void sendShowC1C2C3(boolean isValue) throws IOException, InterruptedException {

        List<StateChangeEvent> stateChangeEvents = new ArrayList<>();

        // we begin from removing all filters
        for (Filter filter : datadocFilters) {
            String field = filter.getField();
            stateChangeEvents.add(new ShowRemoveEvent(new Show(field)));
        }

        // then we show required fields
        getFiltersToShow().forEach(filterToShow -> {
            if (checkFilterIfExists(filterToShow)) {
                stateChangeEvents.add(new ShowAddEvent(
                        isValue ? new Show(filterToShow, Op.VALUE) : new Show(filterToShow),
                        null
                ));
            }
        });

        BookmarkVizCompositeStateChangeEvent stateChangeEvent = new BookmarkVizCompositeStateChangeEvent();
        stateChangeEvent.setEvents(stateChangeEvents);
        sendEvent(stateChangeEvent);
    }

    // custom order for sorting
    public void sendOrderByC2C3C1() throws IOException, InterruptedException {
        List<StateChangeEvent> events = getOrderByEvents();
        BookmarkVizCompositeStateChangeEvent stateChangeEvent = new BookmarkVizCompositeStateChangeEvent();
        stateChangeEvent.setEvents(events);
        sendEvent(stateChangeEvent);
    }

    public void resetFilters() {
        datadocFilters.forEach(datadocFilter -> {
            datadocFilter.setSelected(false);
            datadocFilter.getList().forEach(singleFilterElement -> {
                singleFilterElement.setSelected(false);
            });
        });
    }

    // selecting by c1 value
    public void sendC1FilterSelected() throws IOException, InterruptedException {

        FilterChangeEvent filterChangeEvent = new FilterChangeEvent();
        Filter c1Filter = getAvailableFilter(Arrays.asList("c1", "c0"), "c0");

        datadocFilters.forEach(datadocFilter -> {
            if (datadocFilter.getField().equals(c1Filter.getField())) {
                (datadocFilter.getList()).forEach(datadocFilterList -> c1Filter.getList().add(new FilterValue(
                        datadocFilterList.isSelected(),
                        datadocFilterList.isShow(),
                        datadocFilterList.getKey(),
                        datadocFilterList.getDocCount())
                ));

                selectFilter(c1Filter.getList());

                c1Filter.setListMode(datadocFilter.isListMode());
                c1Filter.setHidden(datadocFilter.isHidden());
                c1Filter.setSelected(true);
                c1Filter.setCardinality(datadocFilter.getCardinality());
                c1Filter.setShowSearch(datadocFilter.isShowSearch());
                c1Filter.setAnd_or(datadocFilter.isAnd_or());
                c1Filter.setLinlog(datadocFilter.isLinlog());

                filterChangeEvent.setFilter(c1Filter);
            }
        });

        sendEvent(filterChangeEvent);
    }

    // searching by c3 field
    public void sendC3SearchField() throws IOException, InterruptedException {
        SearchChangeEvent searchChangeEvent = new SearchChangeEvent();
        searchChangeEvent.setSearch(getSearchMatch());
        sendEvent(searchChangeEvent);
    }

    // grouping by c1
    public void sendGroupByC1() throws IOException, InterruptedException {
        Filter filter = getAvailableFilter(Arrays.asList("c1", "c0"), "c1");

        AggAddEvent aggAddEvent = new AggAddEvent(new Agg(filter.getField(), null, new AggSettings(new AggSort())), null);
        sendEvent(aggAddEvent);
    }

    // =================================================================================================================

    private Filter getAvailableFilter(List<String> filters, String defaultOne) {
        return new Filter(filters.stream().filter(this::checkFilterIfExists).findFirst().orElse(defaultOne));
    }

    // currently disabled: date range
    private Filter getAvailableFilterOfAllowedFormat(List<String> filters, String defaultOne) {
        return new Filter(
                filters.stream()
                        .filter(filter -> datadocFilters.stream().anyMatch(datadocFilter ->
                                datadocFilter.getField().equals(filter) && datadocFilter.getClass() != DateRangeFilter.class))
                        .findFirst().orElse(defaultOne)
        );
    }

    private String getSearchMatch() {
        Filter availableFilter = getAvailableFilterOfAllowedFormat(Arrays.asList("c3", "c2", "c1", "c0"), "c3");

        FilterValue filter = getFilterFromExample(availableFilter);
        return filter != null && filter.getKey() != null ? filter.getKey().toString() : null;
    }

    private String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(decode(path, "UTF-8")));
        return new String(encoded, encoding);
    }

    private List<String> getFiltersToSort() {
        if (datadocFilters.size() < 4) {
            return Arrays.asList("c1", "c2", "c0");
        } else {
            return Arrays.asList("c2", "c3", "c1");
        }
    }

    private List<String> getFiltersToShow() {
        // here we try to handle case with 3 fields [method is not general - just showC1C2C3]
        // c1 c2 c3 if available or c0 c1 c2
        if (datadocFilters.size() < 4) {
            return Arrays.asList("c0", "c1", "c2");
        } else {
            return Arrays.asList("c1", "c2", "c3");
        }
    }

    private List<Show> getShows(Op op, boolean isSort) {
        // get shows columns with sorting
        List<String> filtersToShow = getFiltersToShow();
        List<String> filtersToSort = getFiltersToSort();

        return filtersToShow.stream().map(i -> {
            if (checkFilterIfExists(i)) {
                ShowSettings showSettings = new ShowSettings();
                if (isSort) {
                    Sort sort = new Sort(SortDirection.DESC);
                    sort.setPriority(filtersToSort.indexOf(i));
                    showSettings.setSort(sort);
                }
                return new Show(i, op, showSettings);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private SearchIndexRequest getQueryRequest(String currentSearch) {
        if (currentSearch == null) {
            currentSearch = "";
        }

        QueryParams queryParams = new QueryParams();
        queryParams.setShows(getShows(null, true));
        queryParams.setFilters(datadocFilters);
        queryParams.setSearch(currentSearch);

        SearchIndexRequest request = new SearchIndexRequest();
        request.setTableBookmarkId(bookmarkId);
        request.setParams(queryParams);

        return request;
    }

    private List<StateChangeEvent> getOrderByEvents() {
        List<String> filtersToShow = getFiltersToShow();
        List<String> filtersToSort = getFiltersToSort();

        List<StateChangeEvent> events = new ArrayList<>();

        filtersToShow.forEach(filter -> {
            if (getAvailableFilter(Collections.singletonList(filter), null).getField() != null) {

                Sort sort = new Sort(SortDirection.DESC);
                sort.setPriority(filtersToSort.indexOf(filter));

                ShowSettings showSettings = new ShowSettings();
                showSettings.setSort(sort);

                events.add(new ShowChangeSortEvent(new Show(filter, null, showSettings)));
            }
        });

        return events;
    }

    private Filter getFilterByField(String field) {
        return datadocFilters.stream().filter(datadocFilter -> datadocFilter.getField().equals(field)).findFirst().orElse(null);
    }

    private ArrayList<StateChangeEvent> getHideNullsEvents(List<String> filters) {

        ArrayList<StateChangeEvent> events = new ArrayList<>();

        filters.forEach(filter -> {
            // use already existing filter
            Filter modFilter = getFilterByField(filter);

            if (modFilter != null) {
                List<FilterValue> filterValues = modFilter.getList();

                // hide nulls value or create new exclusion manually
                FilterValue nullsValue = filterValues.stream().filter(value -> value.getKey() == null).findFirst().orElse(null);

                if (nullsValue != null) {
                    nullsValue.setShow(false);
                }

                modFilter.setListMode(true);
                modFilter.setList(filterValues);

                FilterChangeEvent filterChangeEvent = new FilterChangeEvent();
                filterChangeEvent.setFilter(modFilter);

                events.add(filterChangeEvent);
            }
        });

        return events;
    }

    public void sendHideNulls() throws InterruptedException, IOException {
        List<StateChangeEvent> events = getHideNullsEvents(getFiltersToShow());
        BookmarkVizCompositeStateChangeEvent stateChangeEvent = new BookmarkVizCompositeStateChangeEvent();
        stateChangeEvent.setEvents(events);
        sendEvent(stateChangeEvent);
    }

    // =================================================================================================================

    private <T extends BookmarkStateChangeEvent> void sendEvent(T event) throws IOException, InterruptedException {
        sendEvent(event, UUID.randomUUID().toString());
    }

    private <T extends BookmarkStateChangeEvent> void sendEvent(T event, String instanceId) throws IOException, InterruptedException {

        event.setStateId(stateId);
        event.setTabId(bookmarkId);
        event.setId(UUID.randomUUID().toString());
        event.setInstanceId(instanceId);

        socketManager.sendEventAndAwait(event, bookmarkId, stateId);

        if (!(event instanceof ApplyRequestEvent)) {
            sendEvent(new ApplyRequestEvent(), instanceId);
        }
    }

}
