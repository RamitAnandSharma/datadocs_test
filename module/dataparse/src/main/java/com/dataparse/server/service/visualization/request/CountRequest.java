package com.dataparse.server.service.visualization.request;

import lombok.*;

@Data
public class CountRequest extends QueryRequest {
    @Override
    public boolean isFacetQuery() {
        return false;
    }

    public static CountRequest forRequest(SearchRequest searchRequest){
        CountRequest request = new CountRequest();
        request.setBookmarkId(searchRequest.getBookmarkId());
        request.setDatadocId(searchRequest.getDatadocId());
        request.setTableId(searchRequest.getTableId());
        request.setAccountId(searchRequest.getAccountId());
        request.setExternalId(searchRequest.getExternalId());
        request.setColumns(searchRequest.getColumns());
        request.setParams(searchRequest.getParams());
        return request;
    }
}
