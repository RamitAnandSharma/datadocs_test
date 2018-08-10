package com.dataparse.server.controllers.api.table;

import lombok.Data;

@Data
public class AdvancedFiltersAutoCompleteRequest extends AbstractBookmarkRequest {

    private String query;
    private Integer cursor;

}