package com.dataparse.server.service.visualization.request_builder;

import com.dataparse.server.controllers.api.table.SearchIndexResponse;
import com.dataparse.server.service.bigquery.cache.serialization.BigQueryResult;
import com.dataparse.server.service.parser.iterator.ProgressAwareIterator;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import com.dataparse.server.service.visualization.bookmark_state.state.Col;
import com.dataparse.server.service.visualization.request.CardinalityRequest;
import com.dataparse.server.service.visualization.request.FieldAggsRequest;
import com.dataparse.server.service.visualization.request.FieldStatsRequest;
import com.dataparse.server.service.visualization.request.SearchRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface IQueryExecutor {

    Integer MAX_EXPORTED_ROWS = 2000000;

    Map<Object, Long> getFilterTerms(FieldAggsRequest request);

    Map<String, Double> getFilterStats(FieldStatsRequest request);

    Long getFilterCardinality(CardinalityRequest request);

    SearchIndexResponse search(SearchRequest request);

    Future<? extends BigQueryResult> refreshFilters(SearchRequest request);

    ProgressAwareIterator<Map<String, Object>> scroll(SearchRequest request);

    String getAutoCompleteList(String query, Integer cursor, List<Col> columns, List<Filter> filters);

    String fromFilters(List<Col> columns, List<Filter> filters);

    String toFilters(String query, List<Col> columns, List<Filter> filters);
}
