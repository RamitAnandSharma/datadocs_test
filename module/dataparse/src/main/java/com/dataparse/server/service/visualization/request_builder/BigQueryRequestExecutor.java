package com.dataparse.server.service.visualization.request_builder;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.controllers.api.table.SearchIndexResponse;
import com.dataparse.server.service.bigquery.BigQueryClient;
import com.dataparse.server.service.bigquery.BigQueryService;
import com.dataparse.server.service.bigquery.cache.IntelliCache;
import com.dataparse.server.service.bigquery.cache.QueryKey;
import com.dataparse.server.service.bigquery.cache.serialization.BigQueryAggResult;
import com.dataparse.server.service.bigquery.cache.serialization.BigQueryRawResult;
import com.dataparse.server.service.bigquery.cache.serialization.BigQueryRequest;
import com.dataparse.server.service.bigquery.cache.serialization.BigQueryResult;
import com.dataparse.server.service.flow.settings.SearchType;
import com.dataparse.server.service.parser.iterator.ProgressAwareIterator;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.visualization.Tree;
import com.dataparse.server.service.visualization.TreeUtils;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.service.visualization.request.*;
import com.dataparse.server.util.*;
import com.dataparse.server.util.thread.FutureExecutionResult;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BigQueryRequestExecutor implements IQueryExecutor, IFastFilters {

    private final static String BUCKET_SIZE = "$$cluster_size";

    private static final String FIELD_PIVOT = "__pivot";
    private static final String FIELD_VECTOR = "__vector";
    private static final String FIELD_COUNT = "__count";

    private static final String STRING_QUOTE = "'''";
    public static final String LIST_TEMPLATE = "APPROX_TOP_COUNT(%1$s, 10) as %1$s";
    public static final String LIST_TEMPLATE_REPEATED = "APPROX_TOP_COUNT((SELECT MAX(%1$s_nest) FROM UNNEST(%1$s) as %1$s_nest), 10) %1$s";
    public static final String RANGE_TEMPLATE = "MIN(%1$s) as %1$s_min, MAX(%1$s) as %1$s_max";
    public static final String RANGE_TEMPLATE_REPEATED = "MAX((SELECT MAX(%1$s_nest) FROM UNNEST(%1$s) as %1$s_nest)) %1$s_max," +
            "  MIN((SELECT MIN(%1$s_nest) FROM UNNEST(%1$s) as %1$s_nest)) %1$s_min";

    public static final String COUNT_COLUMN_NAME = "cnt";

    @Autowired
    private BigQueryClient bigQuery;

    @Autowired
    private IntelliCache queryCache;

    private List<Filter> except(List<Filter> filters, String except) {
        return filters.stream().filter(f -> !f.getField().equals(except)).collect(Collectors.toList());
    }

    private Object castToType(Object value, BigQueryResult.Field col) {
        if (value == null) {
            return null;
        }
        if (value instanceof List) {
            List list = (List) value;
            List result = new ArrayList(list.size());
            for (Object listValue : list) {
                FieldValue fieldValue = (FieldValue) listValue;
                result.add(castToType(fieldValue.getValue(), col));
            }
            return result;
        }
        if (col.getType().equals(Field.Type.string().getValue())) {
            return value.toString();
        } else if (col.getType().equals(Field.Type.integer().getValue())) {
            return Long.parseLong(value.toString());
        } else if (col.getType().equals(Field.Type.floatingPoint().getValue())) {
            return Double.parseDouble(value.toString());
        } else if (col.getType().equals(Field.Type.timestamp().getValue())) {
            return (new Double(Double.valueOf(value.toString()) * 1000.0D)).longValue();
        } else if (col.getType().equals(Field.Type.bool().getValue())) {
            return Boolean.parseBoolean(value.toString());
        } else {
            return value;
        }
    }

    @Override
    public Map<Object, Long> getFilterTerms(final FieldAggsRequest request) {
        Col col = request.getColumns().stream()
                .filter(c -> c.getField().equals(request.getFieldName()))
                .findFirst()
                .get();
        String query;
        String tableName = escape(request.getExternalId() + "." + BigQueryService.ORIGIN_TABLE);
if(col.isRepeated()) {
            String subquery = "SELECT x FROM " + tableName + ", UNNEST(" + request.getFieldName() + ") as x";
            query = "SELECT unnested.x, COUNT(unnested.x) FROM (" + subquery + ") unnested " +
                    "GROUP BY unnested.x " +
                    "ORDER BY 2 DESC " +
                    "LIMIT " + request.getCount();
        } else {
        String filters = getFiltersClause(request.getColumns(),
                                          except(request.getParams().getFilters(), request.getFieldName()),
                                          request.getParams().getSearch());
        String searchClause = null;
        if (StringUtils.isNotBlank(request.getSearch())) {
            if (request.getSearch().equalsIgnoreCase("[empty value]")) {
                searchClause = request.getFieldName() + " IS NULL";
            } else {
                searchClause = "LOWER(" + request.getFieldName() + ") LIKE \"%" + request.getSearch() + "%\"";
            }
        }
        String whereClause = and(filters, searchClause);

            query = "SELECT " + request.getFieldName() + ", COUNT(*) FROM " + tableName;
            if (StringUtils.isNotBlank(whereClause)) {
                query += " WHERE " + whereClause;
            }
            query += " GROUP BY " + request.getFieldName() + " ORDER BY 2 DESC LIMIT " + request.getCount();
        }
        BigQueryRawResult queryResult = (BigQueryRawResult) bigQuery.querySync(
                BigQueryRequest.builder(request, query).build());
        Map<Object, Long> result = new LinkedHashMap<>();
        for (List<Object> values : queryResult.getValues()) {
            Object value = castToType(values.get(0), queryResult.getFields().get(0));
            Long count = Long.parseLong(values.get(1).toString());
            result.put(value, count);
        }
        return result;
    }

    @Override
    public Map<String, Double> getFilterStats(final FieldStatsRequest request) {
        String tableName = escape(request.getExternalId() + "." + BigQueryService.ORIGIN_TABLE);
        String whereClause = getFiltersClause(request.getColumns(),
                except(request.getParams().getFilters(), request.getFieldName()),
                request.getParams().getSearch());

        String query = getFilterStatsQuery(tableName, request);
        if (StringUtils.isNotBlank(whereClause)) {
            query += " WHERE " + whereClause;
        }

        BigQueryRawResult queryResult = (BigQueryRawResult) bigQuery.querySync(BigQueryRequest.builder(request, query).build());
        Map<String, Double> result = new HashMap<>();
        for (List<Object> values : queryResult.getValues()) {
            Object min = castToType(values.get(0), queryResult.getFields().get(0));
            Object max = castToType(values.get(1), queryResult.getFields().get(1));
            result.put("min", min == null ? null : (min instanceof Long ? ((Long) min).doubleValue() : (Double) min));
            result.put("max", max == null ? null : (max instanceof Long ? ((Long) max).doubleValue() : (Double) max));
        }
        return result;
    }
    private String getFilterStatsQuery(String tableName, FieldStatsRequest request) {
        Col col = tryToFindColumnDefinition(request.getFieldName(), request.getColumns());
        if(col.isRepeated()) {
            return "SELECT MIN(x), MAX(x) FROM " + tableName + ", UNNEST(" + request.getFieldName() + ") as x";
        } else {
            return "SELECT MIN(" + request.getFieldName() + "), MAX(" + request.getFieldName() + ") FROM " + tableName;
        }
    }


    @Override
    public Long getFilterCardinality(final CardinalityRequest request) {
        String tableName = escape(request.getExternalId() + "." + BigQueryService.ORIGIN_TABLE);
        String whereClause = getFiltersClause(request.getColumns(),
                except(request.getParams().getFilters(), request.getFieldName()),
                request.getParams().getSearch());
        String query = getFilterCardinalityQuery(tableName, request);
        if (StringUtils.isNotBlank(whereClause)) {
            query += " WHERE " + whereClause;
        }

        Long result = 0L;
        BigQueryRawResult queryResult = (BigQueryRawResult) bigQuery.querySync(
                BigQueryRequest.builder(request, query).build());
        for (List<Object> values : queryResult.getValues()) {
            result = Long.parseLong(values.get(0).toString());
        }
        return result;
    }

    private String getFilterCardinalityQuery(String tableName, CardinalityRequest request) {
        Col col = tryToFindColumnDefinition(request.getFieldName(), request.getColumns());
        if(col.isRepeated()) {
            return "SELECT COUNT(DISTINCT x) FROM " + tableName + ", UNNEST(" + request.getFieldName() + ") as x";
        } else {
            return "SELECT COUNT(DISTINCT " + request.getFieldName() + ") FROM " + tableName;
        }
    }

    public Future<? extends BigQueryResult> refreshFilters(SearchRequest request) {
        String tableName = getTableName(request.getExternalId());
        String refreshFiltersQuery = getRefreshFiltersQuery(tableName, request);
        return executeSingleBigQueryRequest(request, refreshFiltersQuery);

    }

    private String getRealFieldName(LegacySQLTypeName type, String name) {
        switch (type) {
            case TIMESTAMP:
            case TIME:
            case DATE:
            case FLOAT:
            case INTEGER:
                return name.split("_")[0];
            default:
                return name;
        }
    }

    private Col tryToFindColumnDefinition(String fieldName, List<Col> cols) {
        Optional<Col> columnDefFound = cols.stream()
                .filter(col -> col.getField().equals(fieldName))
                .findFirst();
        if(!columnDefFound.isPresent()) {
            throw new RuntimeException("There is no column definition, for column: " + FunctionUtils.coalesceWithDefaultNull(fieldName));
        }
        return columnDefFound.get();
    }

    private List<Filter> buildFilterData(BigQueryResult bigQueryResult, Map<String, Filter> currentFilters) {
        List<BigQueryResult.Field> fields = bigQueryResult.getFields();
        List<Object> values = (List) ((List) bigQueryResult.getValues()).get(0);
        List<Filter> result = new ArrayList<>(values.size());


        for (int i = 0; i < fields.size(); i++) {
            BigQueryResult.Field field = fields.get(i);
            Object filterData = castToType(values.get(i), field);
            String name = getRealFieldName(field.getType(), field.getName());

            if(COUNT_COLUMN_NAME.equals(name)) {
                continue;
            }
            Filter currentFilter = currentFilters.get(name);
            switch (field.getType()) {
                case RECORD:
                    List<FilterValue> selectedFilterValues = currentFilter.getList().stream().filter(FilterValue::isSelected).collect(Collectors.toList());

                    List<FilterValue> newFilterValues = ((List<List>) filterData).stream().map(filter -> {
                        Object key = filter.get(0);
                        Object count = filter.get(1);
                        return new FilterValue(false, true, key, FunctionUtils.applyIfNotNull(FunctionUtils.safeToString(count), Long::parseLong));
                    }).collect(Collectors.toList());
                    selectedFilterValues.forEach(selectedValue -> {
                        Optional<FilterValue> newFilterValue = newFilterValues.stream()
                                .filter(newValue -> FunctionUtils.safeEquals(selectedValue.getKey(), newValue.getKey()))
                                .findFirst();
                        if (newFilterValue.isPresent()) {
                            newFilterValue.get().setSelected(true);
                        } else {
                            newFilterValues.add(selectedValue);
                        }
                    });
                    Filter newFilter = new Filter();
                    newFilter.setField(name);
                    BeanUtils.copyProperties(currentFilter, newFilter);
                    newFilter.setList(newFilterValues);
                    newFilter.setListMode(true);
                    result.add(newFilter);
                    break;
                case TIMESTAMP:
                case TIME:
                case DATE:
                case FLOAT:
                case INTEGER:
                    if(currentFilter != null && currentFilter.isActive()) {
                        continue;
                    }
                    Object maxValue = castToType(values.get(++i), field);
                    NumberRangeFilter rangeFilter = getRangeFilter(field.getType(), name, filterData, maxValue);
                    rangeFilter.setHidden(currentFilter.isHidden());
                    rangeFilter.setSearch(currentFilter.getSearch());
                    rangeFilter.setSelected(currentFilter.isSelected());
                    result.add(rangeFilter);
                    break;
                default:
                    log.warn("Unknown currentFilter result type. {}:{}", name, field.getType());
            }
        }
        return result;
    }

    private NumberRangeFilter getRangeFilter(LegacySQLTypeName type, String fieldName, Object minValue, Object maxValue) {
        switch (type) {
            case TIMESTAMP:
            case TIME:
            case DATE:
                return new DateRangeFilter(fieldName, (Long) minValue, (Long) maxValue, null, null);
            case FLOAT:
                return new DoubleRangeFilter(fieldName, (Double) minValue, (Double) maxValue, null);
            case INTEGER:
                return new IntegerRangeFilter(fieldName, (Long) minValue, (Long) maxValue, null);
            default:
                    log.warn("Unknown range filter type {}:{}", fieldName, type);
        }
        return null;
    }

    private Future<? extends BigQueryResult> executeSingleBigQueryRequest(SearchRequest searchRequest, String query) {
        Map<String, Object> options = searchRequest.getOptions();
        BigQueryRequest request = BigQueryRequest.builder(searchRequest, query)
                .setRequestUID((String) options.get(ExecutorOptions.REQUEST_UID))
                .setPageNumber(0)
                .setPageSize(1L)
                .setUseIntelliCache(false)
                .build();
        return bigQuery.query(request);
    }

    private String getTableName(String tableId) {
        return escape(tableId + "." + BigQueryService.ORIGIN_TABLE);
    }

    private Future<BigQueryResult> executeMainQuery(SearchRequest request, String query, Long pageSize, Integer pageNumber) {
        Map<String, Object> options = request.getOptions();
        BigQueryRequest queryRequest = BigQueryRequest.builder(request, query)
                .setRequestUID((String) options.get(ExecutorOptions.REQUEST_UID))
                .setPageNumber(pageNumber)
                .setPageSize(pageSize)
                .setUseIntelliCache(false)
                .build();
        return (Future<BigQueryResult>) bigQuery.query(queryRequest);
    }

    @Override
    public SearchIndexResponse search(SearchRequest request) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Long pageSize = null;
        Integer pageNumber = null;
        List<Col> columns = request.getColumns();
        QueryParams params = request.getParams();
        Map<String, Object> options = request.getOptions();
        String requestId = (String) options.get(ExecutorOptions.REQUEST_UID);
        String query = null;
        String fullTableName = getTableName(request.getExternalId());
        if (request.getParams().isRaw()) {
            query = getSimpleQuery(fullTableName, columns, params);
            pageSize = (long) params.getLimit().getPageSize();
            Long from = (Long) options.get(ExecutorOptions.FROM);
            if (from != null) {
                pageNumber = (int) (from / pageSize);
            }
        } else {
            pageNumber = 0;
            pageSize = 100L;
            query = getRollupQuery(fullTableName, columns, params);
        }
        Future<BigQueryResult> queryResultFuture = executeMainQuery(request, query, pageSize, pageNumber);

        Future<? extends BigQueryResult> filtersQuery = refreshFilters(request);

        FutureExecutionResult<BigQueryResult> queryResult = ConcurrentUtils.waitUntilFinished(queryResultFuture);

        log.info("All required data retrieved for {}", stopwatch.stop());

        if (queryResult.isError()) {
            throw new RuntimeException("Query execution exception.", queryResult.getException());
        }


        stopwatch.start();
        SearchIndexResponse result = new SearchIndexResponse();
        result.setScrollId(requestId);
        result.setExternalId(queryResult.getResult().getTableId());
        result.setQuery(query);
        if (params.isRaw()) {
            BigQueryRawResult rawResult = (BigQueryRawResult) queryResult.getResult();

            Tree<Map<String, Object>> results = new Tree<>();
            result.setData(results.getRoot());
            List<BigQueryResult.Field> fields = rawResult.getFields();
            Iterator<List<Object>> it = rawResult.getValues().iterator();

            Map<String, Show> showsByField = ListUtils.groupByKey(params.getShows(), Show::getField);
            Map<Integer, Pair<BigQueryResult.Field, Show>> showsById = new HashMap<>(showsByField.size());
            for (int i = 0; i < fields.size(); i++) {
                BigQueryResult.Field field = fields.get(i);
                Show show = showsByField.get(field.getName());
                if (show != null) {
                    showsById.put(i, Pair.of(field, show));
                }
            }

            while (it.hasNext()) {
                List<Object> values = it.next();
                Map<String, Object> o = new LinkedHashMap<>(showsById.size(), 1);
                showsById.forEach((index, showAndField) ->
                        o.put(showAndField.getRight().key(), castToType(values.get(index), showAndField.getLeft())));
                results.getRoot().addChild(new Tree.Node<>(o));
            }
            log.info("Post processing retrieved data took {}", stopwatch.stop());
        } else {
            Tree<Map<String, Object>> tree;
            Tree<Map<String, Object>> treeRows;
            Tree<Map<String, Object>> treeCols;

            if (queryResult.getResult() instanceof BigQueryRawResult) {
                BigQueryRawResult rawResult = (BigQueryRawResult) queryResult.getResult();
                long start = System.currentTimeMillis();
                treeRows = new Tree<>();
                treeCols = new Tree<>();

                List<BigQueryResult.Field> fields = rawResult.getFields();
                Iterator<List<Object>> it = rawResult.getValues().iterator();


                List<Agg> rowAggs = params.getAggs();
                List<Agg> colAggs = params.getPivot();
                int aggsSize = rowAggs.size() + colAggs.size();
                List<Agg> allAggs = new ArrayList<>();
                allAggs.addAll(params.getAggs());
                allAggs.addAll(params.getPivot());
                Map<String, Object> tuple;

                Map<Long, List<Agg>> bvCache = new HashMap<>();
                List<Object> treeKey;
                while (it.hasNext()) {
                    List<Object> values = it.next();
                    tuple = new HashMap<>(fields.size(), 1);
                    for (int i = 0; i < fields.size(); i++) {
                        BigQueryResult.Field field = fields.get(i);
                        Object value = values.get(i);
                        tuple.put(field.getName(), castToType(value, field));
                    }

                    Long vector = (Long) tuple.get(FIELD_VECTOR);
                    Boolean pivot = (Boolean) tuple.get(FIELD_PIVOT);

                    List<Agg> selectedAggs = bvCache.get(vector);
                    if (selectedAggs == null) {
                        selectedAggs = fromBitVector(allAggs, vector);
                        bvCache.put(vector, selectedAggs);
                    }
                    List<Agg> aggs;
                    if (pivot) {
                        tree = treeCols;
                        aggs = colAggs;
                    } else {
                        tree = treeRows;
                        aggs = rowAggs;
                    }
                    Map<String, Object> o = new LinkedHashMap<>(aggs.size());
                    treeKey = new ArrayList<>();
                    List<Object> prefix = new ArrayList<>();

                    for (Agg agg : selectedAggs) {
                        Object value = tuple.get(agg.key());
                        if (aggs.contains(agg)) {
                            o.put(agg.key(), value);
                            treeKey.add(value);
                        } else {
                            prefix.add(value);
                        }

                    }
                    for (Show show : params.getShows()) {
                        String key = show.key();
                        o.put(key, tuple.get(key));
                    }

                    long count = (Long) tuple.get(FIELD_COUNT);
                    o.put(BUCKET_SIZE, count);

                    if (aggsSize == 1) { // todo use hashset-based trees for fast updates
                        tree.create(treeKey, o);
                    } else {
                        tree.update(treeKey, data -> {
                            if (data == null) {
                                data = new HashMap<>(o.size(), 1);
                            }
                            data.putAll(TreeUtils.prefixMap(o, prefix));
                            return data;
                        });
                    }
                }

                log.info("Created tree in {}", System.currentTimeMillis() - start);
                start = System.currentTimeMillis();
                sortAndLimit(treeRows, params.getAggs(), params.getLimit().getAggData());
                sortAndLimit(treeCols, params.getPivot(), params.getLimit().getPivotData());
                log.info("Sorted tree in {}", System.currentTimeMillis() - start);

                Tree<Map<String, Object>> data, headers;
                if (!params.getAggs().isEmpty()) {
                    data = treeRows;
                } else {
                    data = new Tree<>();
                    data.getRoot().setData(treeCols.getRoot().getData());
                    Tree.Node<Map<String, Object>> tmp = new Tree.Node<>();
                    tmp.setData(TreeUtils.flattenTree(treeCols.getRoot(), params.getPivot()));
                    data.getRoot().addChild(tmp);
                }

                if (!params.getPivot().isEmpty()) {
                    headers = getPivotHeaders(treeCols);
                    if (!params.getAggs().isEmpty()) {
                        result.setTotals(TreeUtils.flattenTree(treeCols.getRoot(), params.getPivot()));
                    }
                } else {
                    headers = getHeaders(params.getShows());
                }

                result.setData(data.getRoot());
                result.setHeaders(headers.getRoot());

                if (AppConfig.getBqEnableQueryCache()) {
                    BigQueryResult resultToCache;
                    if (rawResult.getValues().size() <= 1E6) {
                        resultToCache = rawResult;
                    } else {
                        resultToCache = new BigQueryAggResult(rawResult.getFields(),
                                rawResult.getTotalRows(),
                                BigQueryAggResult.AggResult.of(data, headers));
                    }
                    QueryKey queryKey = QueryKey.of(queryResult.getResult().getRequest());
                    queryCache.put(queryKey, resultToCache);
                }
            } else {
                BigQueryAggResult aggResult = (BigQueryAggResult) queryResult.getResult();
                treeRows = aggResult.getValues().getRows();
                treeCols = aggResult.getValues().getCols();
                result.setHeaders(treeCols.getRoot());
                result.setData(treeRows.getRoot());
            }
            if (params.getPivot().isEmpty()) {
                tree = treeRows;
            } else {
                tree = treeCols;
            }
            result.setCount(((Long) tree.getRoot().getData().get(BUCKET_SIZE)).intValue());
        }

        Stopwatch additionalQueries = Stopwatch.createStarted();
        FutureExecutionResult<? extends BigQueryResult> filtersResults = ConcurrentUtils.waitUntilFinished(filtersQuery);
        log.info("Waiting until additional queries executed took {}", additionalQueries.stop());
        additionalQueries = Stopwatch.createStarted();

        Integer rawCount = retrieveRowsCount(filtersResults);

        Map<String, Filter> currentFilters = MapUtils.mapFromList(request.getParams().getFilters(), Filter::getField, FunctionUtils.identity());
        List<Filter> newFilters = buildFilterData(filtersResults.getResult(), currentFilters);

        result.setFilters(newFilters);
        result.setCount(rawCount);
        log.info("Additional queries processing took {}", additionalQueries.stop());
        return result;
    }

    public Integer retrieveRowsCount(FutureExecutionResult<? extends BigQueryResult> result) {
        if(result.isSuccess()) {
            BigQueryResult bigQueryResult = result.getResult();
            List<BigQueryResult.Field> fields = bigQueryResult.getFields();
            List<Object> values = (List) ((List) bigQueryResult.getValues()).get(0);
            for(int i = fields.size() - 1; i >= 0; i--) {
                BigQueryResult.Field field = fields.get(i);
                if(COUNT_COLUMN_NAME.equals(field.getName())) {
                    return Integer.parseInt(values.get(i).toString());
                }
            }
        }
        return 0;
    }

    private Future<? extends BigQueryResult> executeCountQuery(SearchRequest request, String countQuery, Long pageSize, Integer pageNumber) {
        Map<String, Object> options = request.getOptions();
        CountRequest countRequest = CountRequest.forRequest(request);
        BigQueryRequest countQueryRequest = BigQueryRequest.builder(countRequest, countQuery)
                .setRequestUID((String) options.get(ExecutorOptions.REQUEST_UID))
                .setPageNumber(pageNumber)
                .setPageSize(pageSize)
                .setUseIntelliCache(false)
                .build();

        return bigQuery.query(countQueryRequest);
    }

    private Tree<Map<String, Object>> getPivotHeaders(Tree<Map<String, Object>> columnTotals) {
        return TreeUtils.mapData(columnTotals, data -> data == null ? null : data.entrySet().stream()
                .filter(e -> e.getKey().endsWith(BUCKET_SIZE))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private Tree<Map<String, Object>> getHeaders(List<Show> shows) {
        Tree<Map<String, Object>> headers = new Tree<>();
        for (Show show : shows) {
            Tree.Node<Map<String, Object>> header = new Tree.Node<>();
            header.setKey(show.getField());
            headers.getRoot().getChildren().add(header);
        }
        return headers;
    }

    private class SortComparator implements Comparator<Object> {

        private Sort sort;

        public SortComparator(Sort sort) {
            this.sort = sort;
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compare(Object v1, Object v2) {
            if (v1 == null ^ v2 == null) {
                int c = (v1 == null) ? 1 : -1;
                if (sort.getDirection().equals(SortDirection.ASC)) {
                    c = -c;
                }
                return c;
            }
            if (v1 == null /** && v2 == null */) {
                return 0;
            }
            if (v1 instanceof String) {
                v1 = ((String) v1).toLowerCase();
            }
            if (v2 instanceof String) {
                v2 = ((String) v2).toLowerCase();
            }
            if (sort.getDirection().equals(SortDirection.ASC)) {
                return ((Comparable) v1).compareTo(v2);
            }
            return ((Comparable) v2).compareTo(v1);
        }
    }

    private void sortAndLimit(Tree<Map<String, Object>> tree, List<Agg> aggs, Integer defaultLimit) {
        List<Comparator<Tree.Node<Map<String, Object>>>> comparators = new ArrayList<>();
        List<Integer> limits = new ArrayList<>();
        for (Agg agg : aggs) {
            AggSort sort = agg.getSettings().getSort();
            Integer limit = agg.getSettings().getLimit();
            if (limit == null) {
                limit = defaultLimit;
            }
            limits.add(limit);
            Comparator<Tree.Node<Map<String, Object>>> comparator;
            if (sort.getType().equals(SortType.BY_KEY)) {
                SortComparator sortComparator = new SortComparator(sort);
                comparator = (o1, o2) -> {
                    Object v1;
                    Object v2;
                    if (sort.getIsCount()) {
                        v1 = o1.getData().get(BUCKET_SIZE);
                        v2 = o2.getData().get(BUCKET_SIZE);
                    } else {
                        v1 = o1.getKey();
                        v2 = o2.getKey();
                    }
                    return sortComparator.compare(v1, v2);
                };
            } else {
                SortComparator sortComparator = new SortComparator(sort);
                comparator = (o1, o2) -> {
                    if (o1 == null ^ o2 == null) {
                        int c = (o1 == null) ? 1 : -1;
                        if (sort.getDirection().equals(SortDirection.ASC)) {
                            c = -c;
                        }
                        return c;
                    }
                    if (o1 == null /** && o2 == null */) {
                        return 0;
                    }
                    String field = sort.getIsCount() ? BUCKET_SIZE : sort.getField();
                    List<Object> path = Lists.newArrayList(sort.getAggKeyPath());
                    path.add(field);
                    String key = TreeUtils.getField(path);
                    Object v1 = o1.getData().get(key);
                    Object v2 = o2.getData().get(key);
                    return sortComparator.compare(v1, v2);
                };
            }
            comparators.add(comparator);
        }

        Iterator<Tree.Node<Map<String, Object>>> it = tree.getRoot().iterator();
        while (it.hasNext()) {
            Tree.Node<Map<String, Object>> node = it.next();
            int depth = node.depth();
            if (depth > 0 && depth <= aggs.size()) {
                List<Tree.Node<Map<String, Object>>> children = node.getChildren();
                Comparator<Tree.Node<Map<String, Object>>> cmp = comparators.get(depth - 1);
                children.sort(cmp);
                Integer limit = limits.get(depth - 1);
                node.setChildren(new ArrayList<>(children.subList(0, Math.min(limit, children.size()))));
            }
        }
    }

    private String getCountQuery(String table, final List<Col> columns, QueryParams queryParams) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT COUNT(*) FROM ")
                .append(table);
        String filters = getFiltersClause(columns, queryParams.getFilters(), queryParams.getSearch());
        if (StringUtils.isNotBlank(filters)) {
            builder.append(" WHERE ")
                    .append(filters);
        }
        return builder.toString();
    }

    private String getRefreshFiltersQuery(String tableName, SearchRequest request) {
        QueryParams queryParams = request.getParams();
        List<Filter> selectedFilters = queryParams.getFilters()
                .stream()
                .filter(Filter::isSelected)
                .collect(Collectors.toList());

        Set<String> selectedFiltersFields = selectedFilters.stream()
                .map(Filter::getField)
                .collect(Collectors.toSet());

        List<Col> columns = request.getColumns().stream().filter(col -> selectedFiltersFields.contains(col.getField())).collect(Collectors.toList());

        String filtersClause = getFiltersClause(columns, selectedFilters, queryParams.getSearch(), false);

        Map<String, Col> colsByName = Maps.uniqueIndex(columns, Col::getField);

        String listStatement = buildSelectStatementForFilters(colsByName, queryParams, LIST_TEMPLATE, LIST_TEMPLATE_REPEATED, Filter::isListMode);
        String rangeStatement = buildSelectStatementForFilters(colsByName, queryParams, RANGE_TEMPLATE, RANGE_TEMPLATE_REPEATED, FunctionUtils.complementFunc(Filter::isListMode));
        String count = "COUNT(*) as " + COUNT_COLUMN_NAME;

        // empty range causes syntax exception ( ,, )
        String selectClause = Arrays.stream(new String[]{listStatement, rangeStatement, count})
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.joining(", "));

        return getRefreshFiltersQuery(filtersClause, selectClause, tableName);
    }

    private String getRefreshFiltersQuery(String filtersClause, String selectPart, String tableName) {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isBlank(selectPart)) {
            return null;
        }
        builder.append("SELECT ").
                append(selectPart);
        builder.append(" FROM ").append(tableName);
        if (StringUtils.isNotBlank(filtersClause)) {
            builder.append(" WHERE ").append(filtersClause);
        }
        builder.append(" LIMIT 1");
        return builder.toString();
    }


    private String buildSelectStatementForFilters(Map<String, Col> colsByName, QueryParams queryParams, String template, String repeatedTemplate, Function<Filter, Boolean> applyCause) {
        return queryParams.getFilters().stream().filter(Filter::isSelected).map(filter -> {
            if (applyCause.apply(filter)) {
                Col col = colsByName.get(filter.getField());
                return String.format(col.isRepeated() ? repeatedTemplate : template, filter.getField());
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.joining(","));
    }

    private String getSimpleQuery(String table, final List<Col> columns, QueryParams queryParams) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ")
                .append(queryParams.getShows().stream().map(s -> s.getField()).collect(Collectors.joining(", ")))
                .append(" FROM ")
                .append(table);
        List<Show> ordered = queryParams.getShows().stream()
                .filter(s -> s.getSettings().getSort() != null)
                .sorted(Comparator.comparing(s1 -> s1.getSettings().getSort().getPriority()))
                .collect(Collectors.toList());
        String filters = getFiltersClause(columns, queryParams.getFilters(), queryParams.getSearch());
        if (StringUtils.isNotBlank(filters)) {
            builder.append(" WHERE ")
                    .append(filters);
        }
        if (!ordered.isEmpty()) {
            builder.append(" ORDER BY ")
                    .append(ordered.stream().map(s -> {
                        Col col = columns.stream()
                                .filter(c -> c.getField().equals(s.getField()))
                                .findFirst()
                                .get();
                        return getOrderBy(s, col);
                    }).collect(Collectors.joining(", ")));
        }
        builder.append(" LIMIT ")
                .append(queryParams.getLimit().getRawData());
        return builder.toString();
    }

    private static long toBitVector(List<Agg> allAggs, List<Agg> selectedAggs) {
        BitSet bitSet = new BitSet(allAggs.size());
        for (int i = 0; i < allAggs.size(); i++) {
            if (selectedAggs.contains(allAggs.get(i))) {
                bitSet.set(i);
            }
        }

        return BitUtils.convert(bitSet);
    }

    private static List<Agg> fromBitVector(List<Agg> allAggs, long vector) {
        BitSet bs = BitUtils.convert(vector);
        List<Agg> result = new ArrayList<>();
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            result.add(allAggs.get(i));
            if (i == Integer.MAX_VALUE) {
                break; // or (i+1) would overflow
            }
        }
        return result;
    }

    private String getRollupQuery(String table,
                                  List<Col> columns,
                                  QueryParams params) {

        List<String> subQueries = new ArrayList<>();
        List<Agg> rowAggs = new ArrayList<>();
        rowAggs.addAll(params.getAggs());
        rowAggs.addAll(params.getPivot());
        subQueries.add(getRollupSubQuery(table, params.getShows(), rowAggs, new ArrayList<>(), params.getFilters(), params.getSearch(), columns, 0, !params.getPivot().isEmpty()));

        for (int i = 0; i < params.getAggs().size(); i++) {
            for (int j = 0; j < params.getPivot().size() + 1; j++) {
                List<Agg> aggs = new ArrayList<>();
                aggs.addAll(params.getAggs().subList(0, i + 1));
                aggs.addAll(params.getPivot().subList(0, j));
                long vector = toBitVector(rowAggs, aggs);
                subQueries.add(getRollupSubQuery(table, params.getShows(), rowAggs, aggs, params.getFilters(), params.getSearch(), columns, vector, false));
            }
        }
        List<Agg> colAggs = new ArrayList<>();
        colAggs.addAll(params.getPivot());
        colAggs.addAll(params.getAggs());
        for (int i = 0; i < colAggs.size(); i++) {
            List<Agg> aggs = colAggs.subList(0, i + 1);
            long vector = toBitVector(rowAggs, aggs);
            subQueries.add(getRollupSubQuery(table, params.getShows(), rowAggs, aggs, params.getFilters(), params.getSearch(), columns, vector, true));
        }
        return subQueries.stream().map(this::paren).collect(Collectors.joining("\n UNION ALL \n"));
    }

    private String toShow(String field, Op op, Show show) {
        if (op == null) {
            return field + " as " + show.key();
        }
        return getAggFunction(field, op) + " as " + show.key();
    }

    private String toShow(String field, AggOp op, Agg agg) {
        if (op == null) {
            return field.equals(agg.key()) ? field : field + " as " + agg.key();
        }
        switch (op) {
            case HOUR:
            case DAY:
            case MONTH:
            case QUARTER:
            case YEAR:
                return "TIMESTAMP_TRUNC(" + field + ", " + op.name() + ") as " + agg.key();
            default:
                throw new RuntimeException("Unknown function: " + op);
        }
    }

    private String getAggFunction(String field, Op op) {
        switch (op) {
            case VALUE:
                return "APPROX_TOP_COUNT(" + field + ", 1)[OFFSET(0)].value ";
            case UNIQUE_COUNT:
                return "COUNT(DISTINCT " + field + ")";
            case APPROX_UNIQUE_COUNT:
                return "APPROX_COUNT_DISTINCT(" + field + ")";
            case COUNT:
            case AVG:
            case MIN:
            case MAX:
            case SUM:
                return op.name() + "(" + field + ")";
            default:
                throw new RuntimeException("Unknown agg function: " + op);
        }
    }

    private String getOrderBy(Show s, Col col) {
        SortDirection direction = s.getSettings().getSort().getDirection();
        DataType fieldType = col.getType();
        String field = fieldType.equals(DataType.STRING) ? " LOWER(" + s.getField() + ") " : s.getField();

        if(col.isRepeated()) {
            return "(SELECT MAX(x) FROM UNNEST(" + field + ") x) " + direction;
        }

        return field + " " + direction;
    }

    private String getFieldNameForClause(Col col) {
        return col.isRepeated() ? "x" : col.getField();
    }

    private String getInFilter(List<FilterValue> values, Col col, boolean notIn) {
        FilterValue nullValue = values.stream()
                .filter(v -> v.getKey() == null)
                .findFirst().orElse(null);
        List<String> nonNullValues = values.stream()
                .filter(v -> v.getKey() != null)
                .map(v -> serializeValue(v.getKey(), col))
                .collect(Collectors.toList());
        String result = "";
        String fieldName = getFieldNameForClause(col);
        if (nullValue != null) {
            result += fieldName + " IS " + (notIn ? "NOT " : "") + "NULL";
        }
        if (!nonNullValues.isEmpty()) {
            if (nullValue != null) {
                result = result + (notIn ? "AND" : " OR ");
            }
            result += fieldName
                    + (notIn ? " NOT" : "") + " IN (" + nonNullValues.stream().collect(Collectors.joining(", ")) + ")";
        }
        if(col.isRepeated()) {
            result = "EXISTS (SELECT 1 FROM UNNEST(" + col.getField() + ") x WHERE " + result + ")";
        }
        return result;
    }

    private int getCurrentQuarter() {
        return (DateTime.now(DateTimeZone.UTC).getMonthOfYear() - 1) / 3;
    }

    private Interval getFixedInterval(FixedDateType fixedDateType) {
        DateTime today = DateTime.now(DateTimeZone.UTC).dayOfMonth().roundFloorCopy();
        DateTime thisWeek = today.withDayOfWeek(1).minusDays(1);
        DateTime thisMonth = today.withDayOfMonth(1);
        DateTime thisQuarter = today.withMonthOfYear(getCurrentQuarter() * 3 + 1);
        DateTime thisYear = today.withDayOfYear(1);
        Period quarter = Period.months(3);
        DateTime from, to;
        switch (fixedDateType) {
            case last_7_days:
                from = today.minusDays(7);
                to = today;
                break;
            case last_14_days:
                from = today.minusDays(14);
                to = today;
                break;
            case last_28_days:
                from = today.minusDays(28);
                to = today;
                break;
            case last_30_days:
                from = today.minusDays(30);
                to = today;
                break;
            case today:
                from = today;
                to = today.plusDays(1);
                break;
            case yesterday:
                from = today.minusDays(1);
                to = today;
                break;
            case this_week:
                from = thisWeek;
                to = thisWeek.plusWeeks(1);
                break;
            case last_week:
                from = thisWeek.minusWeeks(1);
                to = thisWeek;
                break;
            case this_month:
                from = thisMonth;
                to = thisMonth.plusMonths(1);
                break;
            case last_month:
                from = thisMonth.minusMonths(1);
                to = thisMonth;
                break;
            case this_quarter:
                from = thisQuarter;
                to = thisQuarter.plus(quarter);
                break;
            case last_quarter:
                from = thisQuarter.minus(quarter);
                to = thisQuarter;
                break;
            case this_year:
                from = thisYear;
                to = thisYear.plusYears(1);
                break;
            case last_year:
                from = thisYear.minusYears(1);
                to = thisYear;
                break;
            case year_to_date:
                from = thisYear;
                to = today;
                break;
            default:
                throw new RuntimeException("Unknown interval: " + fixedDateType);
        }
        return new Interval(from, to);
    }

    private String wrapWithQuotes(String value) {
        return STRING_QUOTE + value + STRING_QUOTE;
    }

    private String buildTokensString(String token, Boolean startWith, Boolean endWith) {
        String result = startWith ? "\\\\b" + token : token;
        result = endWith ? result + "$" : result;
        return "(?i)" + result;
    }

    private String buildRegexpClause(List<Col> fields, String token, Boolean startWith, Boolean endWith) {
        List<String> fieldValues = fields.stream().map(col -> col.isRepeated()
                ? String.format("IFNULL(ARRAY_TO_STRING(%s, ' '), '')", col.getField())
                : String.format("IFNULL(%s, ''), ' '", col.getField())).collect(Collectors.toList());
        String fieldsString = String.join(",", fieldValues);
        String tokensString = wrapWithQuotes(buildTokensString(token, startWith, endWith));
        return String.format("REGEXP_CONTAINS(CONCAT(%s), %s)", fieldsString, tokensString);
    }

    private String getFieldSearchClause(List<Col> fields, String token) {
        List<Col> stringFields = fields.stream().filter(field -> field.getType().isStringType()).collect(Collectors.toList());

        String stringSearchClause = buildFilterSearchClauseForStrings(stringFields, token);
        String numericSearchClause = buildFilterSearchClauseForNumbers(fields, token);
        return or(stringSearchClause, numericSearchClause);
    }

    private String buildFilterSearchClauseForNumbers(List<Col> allFields, String token) {
        List<Col> numericFields = allFields.stream()
                .filter(col -> col.getSearchType().equals(SearchType.EXACT_MATCH) && col.getType().isNumericType())
                .collect(Collectors.toList());
        List<String> exactQueries = numericFields.stream().map(col -> {
            if (NumberUtils.isNumber(token)) {
                return col.getField() + " = " + token;
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        return or(exactQueries);
    }

    private String buildFilterSearchClauseForStrings(List<Col> allFields, String token) {
        Map<SearchType, List<Col>> columnsBySearchType = allFields.stream().collect(Collectors.groupingBy(Col::getSearchType));

        return and(columnsBySearchType.entrySet().stream().map((columnInfo) -> {
            List<Col> fields = columnInfo.getValue();
            SearchType searchType = columnInfo.getKey();
            switch (searchType) {
                case EDGE:
                    return buildRegexpClause(fields, token, true, false);
                case FULL:
                    return buildRegexpClause(fields, token, false, false);
                case EXACT_MATCH:
                    return buildRegexpClause(fields, token, true, true);
                case NONE:
                    return "";
                default:
                    throw new RuntimeException("Unknown search type: " + searchType);
            }
        }).collect(Collectors.toList()));
    }

    private String getFiltersClause(List<Col> cols, List<Filter> filters, String search) {
        return getFiltersClause(cols, filters, search, true);
    }

    private String getFiltersClause(List<Col> cols, List<Filter> filters, String search, Boolean withArrayTransform) {
        List<String> clauses = new ArrayList<>();

        if (StringUtils.isNotBlank(search)) {
            String lowerCasedSearch = search.toLowerCase();
            List<String> tokens = Lists.newArrayList(lowerCasedSearch.split("\\s+"));

            String crossFieldsSearchClause = and(tokens.stream().map(token -> getFieldSearchClause(cols, token)).collect(Collectors.toList()));
            clauses.add(crossFieldsSearchClause);
        }

        for (Filter filter : filters) {
            Col col = cols.stream().filter(c -> c.getField().equals(filter.getField())).findFirst().get();

            if (filter.isListMode()) {

                List<FilterValue> selected = filter.getList().stream().filter(v -> v.isSelected()).collect(
                        Collectors.toList());
                List<FilterValue> crossed = filter.getList().stream().filter(v -> !v.isShow()).collect(
                        Collectors.toList());

                List<String> subClauses = new ArrayList<>();
                if (!selected.isEmpty()) {
                    subClauses.add(getInFilter(selected, col, false));
                }
                if (!crossed.isEmpty()) {
                    subClauses.add(getInFilter(crossed, col, true));
                }
                if (!subClauses.isEmpty()) {
                    clauses.add(and(subClauses.stream().collect(Collectors.toList())));
                }
            } else {
                NumberRangeFilter rangeFilter = (NumberRangeFilter) filter;
                Number value1, value2;
                boolean fixedInterval = false,
                        dateRange = filter instanceof DateRangeFilter;
                String fieldName = getFieldNameForClause(col);
                if (dateRange && ((DateRangeFilter) filter).getFixedDate() != null) {
                    Interval interval = getFixedInterval(((DateRangeFilter) filter).getFixedDate());
                    value1 = interval.getStartMillis();
                    value2 = interval.getEndMillis();
                    fixedInterval = true;
                } else {
                    value1 = rangeFilter.getValue1();
                    value2 = rangeFilter.getValue2();
                }
                List<String> subClauses = new ArrayList<>();
                if (fixedInterval || (rangeFilter.getMin() != null && !rangeFilter.getValue1().equals(rangeFilter.getMin()))) {
                    subClauses.add(fieldName + " >= " + serializeValue(value1, col));
                }
                if (fixedInterval || (rangeFilter.getMax() != null && !rangeFilter.getValue2().equals(rangeFilter.getMax()))) {
                    subClauses.add(fieldName + (dateRange ? " < " : " <= ") + serializeValue(value2, col));
                }
                if (!subClauses.isEmpty()) {
                    String rangeClause = and(subClauses.stream().collect(Collectors.toList()));
                    clauses.add(rangeClause);
                }
            }
        }
        return and(clauses.stream().collect(Collectors.toList()));
    }

    private String escape(String label) {
        return "`" + label + "`";
    }

    private String paren(String s) {
        return "(" + s + ")";
    }

    private String serializeValue(Object value, Col col) {
        if (col.getType().equals(DataType.STRING)) {
            return STRING_QUOTE + value.toString() + STRING_QUOTE;
        } else if (col.getType().equals(DataType.DATE)
                || col.getType().equals(DataType.TIME)) {
            return "TIMESTAMP_MILLIS(" + value.toString() + ")";
        } else {
            return value.toString();
        }
    }

    private static String or(List<String> clauses) {
        return clauses.stream()
                .filter(StringUtils::isNotBlank)
                .map(f -> "(" + f + ")")
                .collect(Collectors.joining(" OR "));
    }

    private static String or(String... clauses) {
        return or(Lists.newArrayList(clauses));
    }

    private static String and(List<String> clauses) {
        return clauses.stream()
                .filter(StringUtils::isNotBlank)
                .map(f -> "(" + f + ")")
                .collect(Collectors.joining(" AND "));

    }

    private static String and(String... clauses) {
        return and(Lists.newArrayList(clauses));
    }

    private String getBQType(DataType dataType) {
        switch (dataType) {
            case DATE:
            case TIME:
                return "TIMESTAMP";
            case DECIMAL:
                return "FLOAT64";
            default:
                return "STRING";
        }
    }

    private String getRollupSubQuery(String table, List<Show> select, List<Agg> allAggs, List<Agg> showAggs,
                                     List<Filter> filters, String search, List<Col> columns, long vector, boolean pivot) {
        StringBuilder subQuery = new StringBuilder();

        Map<String, Col> columnMap = columns.stream().collect(Collectors.toMap(c -> c.getField(), c -> c));
        Map<String, String> unnestAliases = new LinkedHashMap<>();
        Set<String> usedRepeatedCols = new HashSet<>();
        AtomicInteger unnestedCounter = new AtomicInteger();
        columnMap.values().forEach(col -> {
            String alias = "x" + unnestedCounter.getAndIncrement();
            unnestAliases.put(col.getField(), col.isRepeated() ? alias : col.getField());
        });

        List<String> selectAggs = allAggs.stream().map(agg -> {
            Col col = columnMap.get(agg.getField());
            String type = getBQType(col.getType());
            if (!showAggs.contains(agg)) {
                return toShow("cast(NULL as " + type + ")", null, agg);
            }
            if(col.isRepeated()){
                usedRepeatedCols.add(col.getField());
                String alias = unnestAliases.get(col.getField());
                return toShow(alias, agg.getOp(), agg);
            }
            return toShow(agg.getField(), agg.getOp(), agg);
        }).collect(Collectors.toList());
        List<String> aggs = allAggs.stream()
                .filter(showAggs::contains)
                .map(a -> {
                    Col col = columnMap.get(a.getField());
                    if(col.isRepeated()) {
                        usedRepeatedCols.add(a.getField());
                    }
                    return a.key();
                }).collect(Collectors.toList());

        List<String> tfSelect = select.stream()
                .map(s -> {
                    Col col = columnMap.get(s.getField());
                    if(col.isRepeated()) {
                        usedRepeatedCols.add(s.getField());
                    }
                    String alias = unnestAliases.getOrDefault(s.getField(), s.getField());
                    return toShow(alias, s.getOp(), s);
                })
                .collect(Collectors.toList());

        String filterString = getFiltersClause(columns, filters, search);

        List<String> shows = new ArrayList<>();
        shows.add(pivot + " as " + FIELD_PIVOT);
        shows.add(vector + " as " + FIELD_VECTOR);
        shows.addAll(selectAggs);
        shows.addAll(tfSelect);

        boolean useSubQueryForCount = !usedRepeatedCols.isEmpty();
        if (!useSubQueryForCount) {
            shows.add("COUNT(*) as " + FIELD_COUNT);
        } else {
            shows.add("ANY_VALUE(" + FIELD_COUNT + ") as " + FIELD_COUNT);

            List<String> aliases = aggs.stream()
                    .map(a -> unnestAliases.getOrDefault(a, a))
                    .collect(Collectors.toList());
            List<String> countSubQueryShows = new ArrayList<>(aliases);
            countSubQueryShows.add("COUNT(*) as " + FIELD_COUNT);
            List<String> countSubQueryFrom = new ArrayList<>();
            countSubQueryFrom.add(table);
            aggs.forEach(a -> {
                if (usedRepeatedCols.contains(a)) {
                    countSubQueryFrom.add("UNNEST(" + a + ") as " + unnestAliases.get(a));
                }
            });
            subQuery.append("WITH count_query AS (SELECT ")
                    .append(countSubQueryShows.stream().collect(Collectors.joining(", ")))
                    .append(" FROM ")
                    .append(countSubQueryFrom.stream().collect(Collectors.joining(", ")));
            if (StringUtils.isNotBlank(filterString)) {
                subQuery.append(" WHERE ")
                        .append(filterString);
            }
            if (!aggs.isEmpty()) {
                List<String> countSubQueryGroupBy = new ArrayList<>(aliases);
                subQuery.append(" GROUP BY ")
                        .append(countSubQueryGroupBy.stream().collect(Collectors.joining(", ")));
            }
            subQuery.append(")\n");
        }

        List<String> from = new ArrayList<>();
        from.add(table);
        unnestAliases.entrySet().forEach(e -> {
            if (usedRepeatedCols.contains(e.getKey())) {
                from.add("UNNEST(" + e.getKey() + ") as " + e.getValue());
            }
        });
        subQuery.append("SELECT ")
                .append(shows.stream().collect(Collectors.joining(", ")))
                .append(" FROM ")
                .append(from.stream().collect(Collectors.joining(", ")));
        if (useSubQueryForCount) {
            String countJoinQuery;
            if (aggs.isEmpty()) {
                countJoinQuery = " CROSS JOIN count_query";
            } else {
                countJoinQuery = " LEFT JOIN count_query on ";
                countJoinQuery += aggs.stream()
                        .map(a -> {
                            String alias = unnestAliases.getOrDefault(a, a);
                            return "count_query." + alias + " = " + alias;
                        })
                        .collect(Collectors.joining(" AND "));
            }
            subQuery.append(countJoinQuery);
        }
        if (StringUtils.isNotBlank(filterString)) {
            subQuery.append(" WHERE ")
                    .append(filterString);
        }
        if (!aggs.isEmpty()) {
            subQuery.append(" GROUP BY ")
                    .append(aggs.stream()
                            .map(a -> unnestAliases.getOrDefault(a, a))
                            .collect(Collectors.joining(", ")));
        }

        return subQuery.toString();
    }

    @Override
    public ProgressAwareIterator<Map<String, Object>> scroll(SearchRequest request) {
        request.getOptions().put(ExecutorOptions.FROM, 0L);
        SearchIndexResponse response = search(request);
        request.getOptions().put(ExecutorOptions.REQUEST_UID, response.getScrollId());

        final AtomicReference<List<Map<String, Object>>> buffer = new AtomicReference<>(
                response.getData().getChildren().stream()
                        .map(Tree.Node::getData)
                        .collect(Collectors.toList()));

        return new ProgressAwareIterator<Map<String, Object>>() {

            int cursor = 0;
            int loaded = 0;

            @Override
            public Tree.Node<Map<String, Object>> getHeaders() {
                return new Tree.Node<>();
            }

            @Override
            public Map<String, Object> getTotals() {
                return response.getTotals();
            }

            @Override
            public Double getComplete() {
                return response.getCount() > 0. ? this.loaded / (double) response.getCount() : 0.;
            }

            @Override
            public boolean hasNext() {
                return this.loaded < MAX_EXPORTED_ROWS && this.loaded < response.getCount();
            }

            @Override
            @SuppressWarnings("unchecked")
            public Map<String, Object> next() {
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("Query results scrolling interrupted externally");
                }
                if (cursor > buffer.get().size() - 1) {
                    this.cursor = 0;
                    SearchIndexResponse response = search(request);
                    buffer.set(response.getData().getChildren().stream()
                            .map(Tree.Node::getData)
                            .collect(Collectors.toList()));
                }
                this.loaded++;
                return buffer.get().get(this.cursor++);
            }
        };
    }

    @Override
    public String getAutoCompleteList(final String query, final Integer cursor, final List<Col> columns,
                                      final List<Filter> filters) {
        return null;
    }

    @Override
    public String fromFilters(final List<Col> columns, final List<Filter> filters) {
        return null;
    }

    @Override
    public String toFilters(final String query, final List<Col> columns, final List<Filter> filters) {
        return null;
    }
}
