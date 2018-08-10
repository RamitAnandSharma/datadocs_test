package com.dataparse.server.service.visualization.request;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.util.*;
import java.util.stream.*;

@Data
@NoArgsConstructor
public class SearchRequest extends QueryRequest {

    public SearchRequest(final Long datadocId, final Long bookmarkId, final Long tableId, final String accountId,
                         final String externalId,
                         final List<Col> columns,
                         final QueryParams params,
                         final Map<String, Object> options) {
        super(datadocId, bookmarkId, tableId, accountId, externalId, columns, params);
        this.options = options;
    }

    Map<String, Object> options;

    @Override
    public boolean isFacetQuery(){
        return false;
    }
}
