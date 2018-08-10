package com.dataparse.server.service.visualization.request;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldStatsRequest extends QueryRequest {

    String fieldName;
    List<Col> columns;
    QueryParams params;

    @Override
    public boolean isFacetQuery() {
        return true;
    }
}
