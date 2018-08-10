package com.dataparse.server.service.visualization.request;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class QueryRequest {

    Long datadocId;
    Long bookmarkId;
    Long tableId;
    String accountId;
    String externalId;
    List<Col> columns;
    QueryParams params;

    @JsonIgnore
    public abstract boolean isFacetQuery();

}
