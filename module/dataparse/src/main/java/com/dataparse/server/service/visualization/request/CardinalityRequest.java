package com.dataparse.server.service.visualization.request;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CardinalityRequest extends QueryRequest {

    String fieldName;

    @Override
    public boolean isFacetQuery() {
        return true;
    }
}
