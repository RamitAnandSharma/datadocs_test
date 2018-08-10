package com.dataparse.server.service.visualization.request;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldAggsRequest  extends QueryRequest {

    String fieldName;
    Long count;
    String search;

    @Override
    public boolean isFacetQuery() {
        return true;
    }
}
