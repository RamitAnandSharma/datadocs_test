package com.dataparse.server.service.visualization.request_builder;

import com.dataparse.request_builder.*;
import com.dataparse.server.auth.*;
import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.service.bigquery.cache.serialization.BigQueryResult;
import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.parser.iterator.*;
import com.dataparse.server.service.schema.log.*;
import com.dataparse.server.service.es.*;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.visualization.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.service.visualization.request.*;
import com.dataparse.server.util.*;
import io.searchbox.client.*;
import io.searchbox.core.*;
import io.searchbox.core.search.aggregation.*;
import io.searchbox.params.*;
import lombok.extern.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.*;

@Slf4j
@Service
public class EsRequestExecutor implements IQueryExecutor {

    private final static int SCROLL_SIZE = 10_000;

    private final static String DATA = "data";
    private final static String ROW_DATA = "agg_row";
    private final static String COL_DATA = "agg_col";

    private final static String DATA_AGGS_ROOT = "data_aggs";
    private final static String PIVOT_AGGS_ROOT = "pivot_aggs";
    private final static String BUCKET_SIZE = "$$cluster_size";

    private final static String MISSING_COUNT_AGGREGATION = "MISSING_COUNT";

    private final static long MISSING_VALUE = -9223372036854774784L;

    private SearchRequestBuilderImpl requestBuilder = new SearchRequestBuilderImpl();

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    private BookmarkActionLogService actionLogService;

    private Object tryParseAggregationKey(DataType dataType, String aggregationKeyString){
        Object aggregationKey = aggregationKeyString;
        try {
            switch (dataType) {
                case DATE:
                case TIME:
                case DECIMAL:
                    aggregationKey = Double.parseDouble(aggregationKeyString);
                    if(((Double) aggregationKey).longValue() == MISSING_VALUE){
                        aggregationKey = null;
                    }
                    break;
                default:
                    aggregationKey = aggregationKeyString;
                    if(aggregationKey.equals(String.valueOf(MISSING_VALUE))){
                        aggregationKey = null;
                    }
                    break;
            }
        } catch (Exception e) {
            log.error("Can't parse aggregation key", e);
        }
        return aggregationKey;
    }

    @Override
    public Map<Object, Long> getFilterTerms(final FieldAggsRequest request) {
        String colString = JsonUtils.writeValue(request.getColumns());
        String paramsString = JsonUtils.writeValue(request.getParams());
        Long count = request.getCount();
        if(count == null){
            count = 10L;
        }
        String query = requestBuilder.getFilterTermsQuery(colString, paramsString, request.getFieldName(), count.intValue(), request.getSearch());
        Search action = new Search.Builder(query)
                .addIndex(request.getExternalId())
                .addType("row")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setParameter(Parameters.SIZE, 0)
                .build();

        RequestLogEntry logEntry = RequestLogEntry.of(request);
        logEntry.setUserId(Auth.get().getUserId());
        logEntry.setBillableUserId(Auth.get().getUserId());
        logEntry.setBytesProcessed(0); // todo ?
        logEntry.setStorageType(EngineType.ES);
        logEntry.setQuery(query);
        logEntry.setFromCache(false);
        log.info(query);
        Map<Object, Long> result = new HashMap<>();
        try {
            SearchResult searchResult = elasticClient.getClient().execute(action);
            MetricAggregation aggregations = searchResult.getAggregations();
            Col col = request.getColumns().stream().filter(c -> c.getField().equals(request.getFieldName())).findFirst().get();
            aggregations.getTermsAggregation("fields_agg").getBuckets().forEach(entry -> {
                Object aggregationKey = tryParseAggregationKey(col.getType(), entry.getKey());
                result.put(aggregationKey, entry.getCount());
            });
            long executionTime = searchResult.getJsonObject().get("took").getAsLong();
            logEntry.setSuccess(true);
            logEntry.setDuration(executionTime);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            actionLogService.save(logEntry);
        }
        return result;
    }

    @Override
    public Long getFilterCardinality(final CardinalityRequest request) {
        String colString = JsonUtils.writeValue(request.getColumns());
        String paramsString = JsonUtils.writeValue(request.getParams());

        String esIndex = request.getExternalId();
        String query = requestBuilder.getFilterCardinalityQuery(colString, paramsString, request.getFieldName());
        Search search = new Search.Builder(query)
                .addIndex(esIndex)
                .addType("row")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setParameter(Parameters.SIZE, 0)
                .build();
        RequestLogEntry logEntry = RequestLogEntry.of(request);
        logEntry.setUserId(Auth.get().getUserId());
        logEntry.setBillableUserId(Auth.get().getUserId());
        logEntry.setBytesProcessed(0); // todo ?
        logEntry.setStorageType(EngineType.ES);
        logEntry.setQuery(query);
        logEntry.setFromCache(false);
        Long cardinality;
        try {
            SearchResult searchResult = elasticClient.getClient().execute(search);
            MetricAggregation aggregations = searchResult.getAggregations();
            CardinalityAggregation agg = aggregations.getCardinalityAggregation("fields_agg");
            cardinality = agg.getCardinality();
            long executionTime = searchResult.getJsonObject().get("took").getAsLong();
            logEntry.setSuccess(true);
            logEntry.setDuration(executionTime);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            actionLogService.save(logEntry);
        }
        return cardinality;
    }

    @Override
    public Map<String, Double> getFilterStats(final FieldStatsRequest request) {
        String colString = JsonUtils.writeValue(request.getColumns());
        String paramsString = JsonUtils.writeValue(request.getParams());

        String esIndex = request.getExternalId();
        String query = requestBuilder.getFilterStatsQuery(colString, paramsString, request.getFieldName());
        Search search = new Search.Builder(query)
                .addIndex(esIndex)
                .addType("row")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setParameter(Parameters.SIZE, 0)
                .build();
        RequestLogEntry logEntry = RequestLogEntry.of(request);
        logEntry.setUserId(Auth.get().getUserId());
        logEntry.setBillableUserId(Auth.get().getUserId());
        logEntry.setBytesProcessed(0); // todo ?
        logEntry.setStorageType(EngineType.ES);
        logEntry.setQuery(query);
        logEntry.setFromCache(false);
        Map<String, Double> result = new HashMap<>();
        try {
            SearchResult searchResult = elasticClient.getClient().execute(search);
            MetricAggregation aggregations = searchResult.getAggregations();
            StatsAggregation agg = aggregations.getStatsAggregation("fields_agg");
            result.put("min", agg.getMin());
            result.put("max", agg.getMax());
            long executionTime = searchResult.getJsonObject().get("took").getAsLong();
            logEntry.setSuccess(true);
            logEntry.setDuration(executionTime);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            actionLogService.save(logEntry);
        }
        return result;
    }

    @Override
    public SearchIndexResponse search(SearchRequest request) {
        long start = System.nanoTime();
        List<Col> columns = request.getColumns();
        QueryParams params = request.getParams();
        Map<String, Object> options = request.getOptions();
        String query = requestBuilder.getQuery(JsonUtils.writeValue(columns), JsonUtils.writeValue(params));
        log.info("Query string generated in " + ((System.nanoTime() - start) / 1E6));

        boolean isAgg = !request.getParams().isRaw();
        Search.Builder searchBuilder = new Search.Builder(query)
                .addIndex(request.getExternalId())
                .addType("row")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setParameter("request_cache", true)
                .setParameter(Parameters.SIZE, isAgg ? 0 : request.getParams().getLimit().getPageSize());

        if(!isAgg){
            searchBuilder.setParameter("from", options.get(ExecutorOptions.FROM));
        }

        String scroll = (String) options.get(ExecutorOptions.REQUEST_UID);
        if(scroll != null) {
            searchBuilder.setParameter(Parameters.SCROLL, "1m");
        }

        Search search = searchBuilder.build();

        RequestLogEntry logEntry = RequestLogEntry.of(request);
        logEntry.setUserId(Auth.get().getUserId());
        logEntry.setBillableUserId(Auth.get().getUserId());
        logEntry.setBytesProcessed(0); // todo ?
        logEntry.setStorageType(EngineType.ES);
        logEntry.setQuery(query);
        logEntry.setFromCache(false);
        SearchIndexResponse response = new SearchIndexResponse();
        response.setQuery(search.toString());
        try {
            start = System.currentTimeMillis();
            SearchResult searchResult = elasticClient.getClient().execute(search);
            long end = System.currentTimeMillis();
            long responseRetrievalTime = end - start;
            log.info("Query executed in " + responseRetrievalTime);
            if (searchResult.getErrorMessage() != null) {
                throw new RuntimeException(searchResult.getErrorMessage());
            }
            long executionTime = searchResult.getJsonObject().get("took").getAsLong();
            logEntry.setSuccess(true);
            logEntry.setDuration(executionTime);
            if(scroll != null) {
                response.setScrollId(searchResult.getJsonObject().get("_scroll_id").getAsString());
            }
            response.setCount(searchResult.getTotal());

            Map<String, DataType> columnTypes = columns.stream().collect(Collectors.toMap(Col::getField, Col::getType));
            Map<String, DataType> aggTypes = new HashMap<>();
            aggTypes.putAll(params.getShows().stream().collect(Collectors.toMap(Show::key, s -> columnTypes.get(s.getField()))));
            if(params.getAggs() != null) {
                aggTypes.putAll(params.getAggs().stream().collect(Collectors.toMap(Agg::key, a -> columnTypes.get(a.getField()))));
            }
            if(params.getPivot() != null) {
                aggTypes.putAll(params.getAggs().stream().collect(Collectors.toMap(Agg::key, a -> columnTypes.get(a.getField()))));
                aggTypes.putAll(params.getPivot().stream().collect(Collectors.toMap(Agg::key, a -> columnTypes.get(a.getField()))));
            }

            if(!params.getPivot().isEmpty() || !params.getAggs().isEmpty()){
                Tree<Map<String, Object>> pivotData = getAggregations(PIVOT_AGGS_ROOT, aggTypes, searchResult.getAggregations(), query, params);
                pivotData.getRoot().getData().put(BUCKET_SIZE, searchResult.getTotal()); // add term size
                int level = 0;
                if(params.getRow() != null){
                    level = params.getRow().getTreeLevel() + 1;
                }
                Agg agg = params.getAggs().isEmpty() ? null : params.getAggs().get(level);

                Tree.Node<Map<String, Object>> data;
                if (agg != null) {
                    Tree<Map<String, Object>> aggsData = getAggregations(DATA_AGGS_ROOT, aggTypes, searchResult.getAggregations(), query, params);
                    pivotData.getRoot().getData().put(BUCKET_SIZE, searchResult.getTotal()); // add term size
                    data = aggsData.getRoot();
                } else {
                    data = new Tree<Map<String, Object>>().getRoot();
                    data.setData(pivotData.getRoot().getData());
                    Tree.Node<Map<String, Object>> tmp = new Tree.Node<>();
                    tmp.setData(TreeUtils.flattenTree(pivotData.getRoot(), params.getPivot()));
                    data.addChild(tmp);
                }
                if(!params.getPivot().isEmpty()){
                    response.setHeaders(getPivotHeaders(pivotData).getRoot());
                    if(!params.getAggs().isEmpty()) {
                        response.setTotals(TreeUtils.flattenTree(pivotData.getRoot(), params.getPivot()));
                    }
                } else {
                    response.setHeaders(getHeaders(params.getShows()).getRoot());
                }
                response.setData(data);
            } else {
                response.setHeaders(getHeaders(params.getShows()).getRoot());
                response.setTotals(parseAggregationValues(searchResult.getAggregations(), aggTypes));
                List<Map<String, Object>> hits = searchResult.getHits(Map.class)
                        .stream().map(hit -> {
                            if((Boolean) options.get(ExecutorOptions.USE_TT)){
                                return JSONTuplewiseTransform.restore((Map<String, Object>) hit.source);
                            }
                            return (Map<String, Object>) hit.source;
                        })
                        .collect(Collectors.toList());
                Tree<Map<String, Object>> tree = new Tree<>();
                hits.forEach(hit -> tree.getRoot().addChild(new Tree.Node<>(hit)));
                response.setData(tree.getRoot());
            }
        } catch (Exception e) {
            throw new RuntimeException("Can't execute query for bookmark " + request.getBookmarkId(), e);
        } finally {
            actionLogService.save(logEntry);
        }
        return response;
    }

    @Override
    public Future<? extends BigQueryResult> refreshFilters(SearchRequest request) {
        return null;
    }

    @Override
    public ProgressAwareIterator<Map<String, Object>> scroll(SearchRequest request) {
        request.getOptions().put(ExecutorOptions.FROM, 0L);
        request.getOptions().put(ExecutorOptions.REQUEST_UID, "1m");
        SearchIndexResponse response = search(request);

        return new ProgressAwareIterator<Map<String,Object>>() {

            String scrollId = null;

            int cursor = 0;
            int loaded = 0;
            List<Map<String, Object>> buffer = new ArrayList<>();

            @Override
            public Tree.Node<Map<String, Object>> getHeaders() {
                return new Tree.Node<>();
            }

            @Override
            public Map<String, Object> getTotals() {
                return response.getTotals();
            }

            @Override
            public Double getComplete(){
                return response.getCount() > 0. ? this.loaded / (double) response.getCount() : 0.;
            }

            @Override
            public boolean hasNext() {
                return this.loaded < MAX_EXPORTED_ROWS && this.loaded < response.getCount();
            }

            @Override
            @SuppressWarnings("unchecked")
            public Map<String, Object> next() {
                if(Thread.currentThread().isInterrupted()){
                    throw new RuntimeException("Query results scrolling interrupted externally");
                }
                if(cursor > buffer.size() - 1) {
                    if (scrollId == null) {
                        this.scrollId = response.getScrollId();
                        this.buffer = response.getData().getChildren().stream()
                                .map(n -> n.getData())
                                .collect(Collectors.toList());
                    } else {
                        // todo refactor: use scrollID in search method
                        SearchScroll scroll = new SearchScroll.Builder(this.scrollId, "1m").build();
                        JestResult searchResult;
                        try {
                            searchResult = elasticClient.getClient().execute(scroll);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        List<Map<String, Object>> hits = (List<Map<String, Object>>)((Map)(searchResult.getJsonMap()).get("hits")).get("hits");
                        // todo fix TT
                        this.buffer = hits.stream().map(hit -> (Map<String, Object>) hit.get("_source")).collect(Collectors.toList());
                        this.cursor = 0;
                    }
                }
                this.loaded++;
                return this.buffer.get(this.cursor++);
            }
        };
    }

    private Tree<Map<String, Object>> getPivotHeaders(Tree<Map<String, Object>> columnTotals){
        return TreeUtils.mapData(columnTotals, data -> data == null ? null : data.entrySet().stream()
                .filter(e -> e.getKey().endsWith(BUCKET_SIZE))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private Tree<Map<String, Object>> getHeaders(List<Show> shows){
        Tree<Map<String, Object>> headers = new Tree<>();
        for(Show show : shows){
            Tree.Node<Map<String, Object>> header = new Tree.Node<>();
            header.setKey(show.getField());
            headers.getRoot().getChildren().add(header);
        }
        return headers;
    }

    private Tree<Map<String, Object>> getAggregations(String aggsRoot, Map<String, DataType> columnTypes, MetricAggregation aggregations, String query, QueryParams params) {
        Tree<Map<String, Object>> tree = new Tree<>();
        //        if (data != null) {
        Map<String, Object> resPart = parseAggregationValues(aggregations, columnTypes);
        String aggName;
        if(PIVOT_AGGS_ROOT.equals(aggsRoot)){
            aggName = COL_DATA;
        } else {
            aggName = ROW_DATA;
        }
        tree.getRoot().setData(resPart);
        getBucketAggregation(columnTypes, aggregations, aggName, tree.getRoot(), params, false);
        //        }
        return tree;
    }

    private boolean getBucketAggregation(Map<String, DataType> columnTypes, MetricAggregation data,
                                         String aggName, Tree.Node<Map<String, Object>> node, QueryParams params, boolean suppressSubAggs) {
        TermsAggregation baseBuckets = data.getTermsAggregation(aggName);
        if(baseBuckets == null) {
            return false;
        }

        List<TermsAggregation.Entry> buckets = baseBuckets.getBuckets().stream().filter(b -> b.getCount() > 0).collect(Collectors.toList());
        for (TermsAggregation.Entry b : buckets) {

            String aggregationColumn = baseBuckets.getMeta("key");
            DataType dataType = columnTypes.get(aggregationColumn);
            String aggregationKeyString = b.getKey();
            Object aggregationKey = tryParseAggregationKey(dataType, aggregationKeyString);

            Tree.Node<Map<String, Object>> newNode = new Tree.Node<>();
            newNode.setKey(aggregationKey);
            node.addChild(newNode);

            getBucketAggregation(columnTypes, b, aggName, newNode, params, false);
            // add aggregated values
            Map<String, Object> resPart = parseAggregationValues(b, columnTypes);
            resPart.put(BUCKET_SIZE, b.getCount()); // add term size
            newNode.setData(resPart);

            if(!suppressSubAggs) {
                Tree<Map<String, Object>> subAggTree = new Tree<>();
                getBucketAggregation(columnTypes, b, ROW_DATA.equals(aggName) ? COL_DATA : ROW_DATA,
                                     subAggTree.getRoot(), params, true);
                Map<String, Object> flattenedSubTree = TreeUtils.flattenTree(subAggTree.getRoot(), ROW_DATA.equals(aggName) ? params.getPivot() : params.getAggs());
                newNode.getData().putAll(flattenedSubTree);
            }
        }
        return true;
    }

    private Map<String, Object> parseAggregationValues(MetricAggregation parentData, Map<String, DataType> columnTypes){
        Map<String, Object> resPart = new HashMap<>();
        List<String> aggregations = parentData.getFields().stream()
                .filter(a -> !a.startsWith(MISSING_COUNT_AGGREGATION))
                .collect(Collectors.toList());
        aggregations.forEach(field -> {
            if (field.startsWith(Op.SUM.name())) {
                SumAggregation sumAggregation = parentData.getSumAggregation(field);
                resPart.put(field, sumAggregation.getSum());
            } else if (field.startsWith(Op.COUNT.name())) {
                ValueCountAggregation valueCountAggregation = parentData.getValueCountAggregation(field);
                resPart.put(field, valueCountAggregation.getValueCount());
            } else if (field.startsWith(Op.APPROX_UNIQUE_COUNT.name())) {
                CardinalityAggregation cardinalityAggregation = parentData.getCardinalityAggregation(field);
                resPart.put(field, cardinalityAggregation.getCardinality());
            } else if (field.startsWith(Op.UNIQUE_COUNT.name())) {
                // ES doesn't count NULLs in cardinality aggregation, so we must take them into consideration by ourselves
                ExactCardinalityAggregation exactCardinalityAggregation = parentData.getExactCardinalityAggregation(field);
                MissingAggregation missingAggregation = parentData.getMissingAggregation(
                        MISSING_COUNT_AGGREGATION + "_" + field.substring(Op.UNIQUE_COUNT.name().length() + 1));
                Long exactCardinality = exactCardinalityAggregation.getExactCardinality();
                if(missingAggregation != null && missingAggregation.getMissing() > 0){
                    exactCardinality++;
                }
                resPart.put(field, exactCardinality);
            } else if (field.startsWith(Op.AVG.name())) {
                AvgAggregation avgAggregation = parentData.getAvgAggregation(field);
                resPart.put(field, avgAggregation.getAvg());
            } else if (field.startsWith(Op.MIN.name())) {
                MinAggregation minAggregation = parentData.getMinAggregation(field);
                resPart.put(field, minAggregation.getMin());
            } else if (field.startsWith(Op.MAX.name())) {
                MaxAggregation maxAggregation = parentData.getMaxAggregation(field);
                resPart.put(field, maxAggregation.getMax());
            } else if (field.startsWith(Op.VALUE.name())) {
                TermsAggregation termsAggregation = parentData.getTermsAggregation(field);
                List<TermsAggregation.Entry> entries = termsAggregation.getBuckets();
                if (!entries.isEmpty()) {
                        DataType valueDataType = columnTypes.get(field);
                        String aggregatedValueString = entries.get(0).getKey();
                        Object aggregatedValue = tryParseAggregationKey(valueDataType, aggregatedValueString);
                        resPart.put(field, aggregatedValue);
                }
            } else if(!DATA.equals(field) && !ROW_DATA.equals(field) && !COL_DATA.equals(field)) { // for raw data we get total sum for numbers
                SumAggregation sumAggregation = parentData.getSumAggregation(field);
                resPart.put(field, sumAggregation.getSum());
            }
        });
        return resPart;
    }

    @Override
    public String getAutoCompleteList(final String query, final Integer cursor, final List<Col> columns,
                                      final List<Filter> filters) {
        return requestBuilder.getAutoCompleteList(query, cursor,
                                                  JsonUtils.writeValue(columns),
                                                  JsonUtils.writeValue(filters));
    }

    @Override
    public String fromFilters(final List<Col> columns, final List<Filter> filters) {
        return requestBuilder.fromFilters(JsonUtils.writeValue(columns),
                                          JsonUtils.writeValue(filters));
    }

    @Override
    public String toFilters(final String query, final List<Col> columns, final List<Filter> filters) {
        FilterListWrapper wrapperCrutch = new FilterListWrapper();
        wrapperCrutch.addAll(filters);
        return requestBuilder.toFilters(query,
                                        JsonUtils.writeValue(columns),
                                        JsonUtils.writeValue(wrapperCrutch));
    }
}
