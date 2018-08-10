package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.visualization.Tree;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class SearchIndexResponse {

    /** ID of BQ anonymous table */
    private String externalId;

    private String scrollId;

    private Integer count;

//    todo this is for debug purpose, remove
    private String query;

    private Tree.Node<Map<String, Object>> headers = new Tree.Node<>();
    private Tree.Node<Map<String, Object>> data = new Tree.Node<>();
    private List<Filter> filters = new ArrayList<>();
    private Map<String, Object> totals = new HashMap<>();
}
