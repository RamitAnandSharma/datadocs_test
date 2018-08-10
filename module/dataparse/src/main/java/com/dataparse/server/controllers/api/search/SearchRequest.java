package com.dataparse.server.controllers.api.search;

import com.dataparse.server.util.db.OrderBy;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SearchRequest {

    private String s;
    private Integer limit = 0;
    private Boolean fetchSections = false;
    private Boolean fetchRelatedDatadocs = false;
    private OrderBy orderBy;
}
