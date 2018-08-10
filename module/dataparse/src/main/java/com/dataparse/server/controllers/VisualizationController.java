package com.dataparse.server.controllers;

import com.dataparse.request_builder.ParseException;
import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.service.parser.processor.FormatProcessor;
import com.dataparse.server.service.visualization.VisualizationService;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import com.dataparse.server.service.visualization.bookmark_state.filter.FilterQueryExecutor;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.dataparse.server.service.visualization.bookmark_state.state.Col;
import com.dataparse.server.util.ConcurrentUtils;
import com.google.common.base.Stopwatch;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@Api(value = "Visualization", description = "Visualization api operations")
@RequestMapping("/api/visualization")
public class VisualizationController extends ApiController {

    @Autowired
    private VisualizationService visualizationService;

    @Autowired
    private FilterQueryExecutor filterQueryExecutor;

    @Autowired
    private BookmarkStateStorage bookmarkStateStorage;

    @ExceptionHandler(ParseException.class)
    @ResponseStatus(value = HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleException(ParseException e) {
        Map<String, Object> result = new HashMap<>();
        result.put("type", "ParseException");
        result.put("message", e.getMessage());
        result.put("column", e.getColumn());
        result.put("line", e.getLine());
        result.put("index", e.getIndex());
        result.put("expected", e.getExpected());
        return result;
    }

    @RequestMapping(value = "/format-value", method = RequestMethod.POST, produces = "text/plain")
    public String formatValue(@RequestBody FormatSingleValueRequest request) {
        BookmarkStateId stateId = new BookmarkStateId(request.getTabId(), request.getStateId());
        BookmarkState state = bookmarkStateStorage.get(stateId, false).getState();
        Optional<Col> first = state.getColumnList().stream().filter(col -> col.getField().equals(request.getFieldName())).findFirst();
        if(!first.isPresent()) {
            throw new RuntimeException("There is no field. " + request.getFieldName());
        }
        Col col = first.get();
        Object convertedValue = FormatProcessor.tryToConvertValue(col.getExampleValue());
        Object result = FormatProcessor.simpleFormatValue(convertedValue, col, request.getFormat());
        assert result != null;

        return result.toString();
    }

    @RequestMapping(value = "/autocomplete", method = RequestMethod.POST)
    public String autocomplete(@RequestBody AdvancedFiltersAutoCompleteRequest request) {
        return visualizationService.autocomplete(request);
    }

    @RequestMapping(value = "/from-filters", method = RequestMethod.POST)
    public FromFiltersResponse fromFilters(@RequestBody FromFiltersRequest request) {
        return new FromFiltersResponse(visualizationService.getQueryFromFilters(request));
    }

    @RequestMapping(value = "/to-filters", method = RequestMethod.POST)
    public ToFiltersResponse toFilters(@RequestBody ToFiltersRequest request) {
        return new ToFiltersResponse(visualizationService.getFiltersFromQuery(request));
    }

    @RequestMapping(value = "/refresh-filter", method = RequestMethod.POST)
    public Filter refreshFilter(@RequestBody RefreshFilterRequest request){
        return visualizationService.refreshFilter(request);
    }

    @ApiOperation(value = "Search by Query API")
    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public SearchIndexResponse search(@RequestBody SearchIndexRequest request) {
        log.info("Start searching tab: {}, state: {}", request.getTableBookmarkId(), request.getStateId());
        Stopwatch stopwatch = Stopwatch.createStarted();
        SearchIndexResponse search = visualizationService.search(request);
        ConcurrentUtils.setTimeout(() -> {
            Integer filtersQueries = filterQueryExecutor.runQueue(request.getTableBookmarkId());
            log.info("Running {} filter queries for bookmark {}", filtersQueries, request.getTableBookmarkId());
        }, 1500);
        log.info("Search took {}", stopwatch.stop());
        return search;
    }

}
