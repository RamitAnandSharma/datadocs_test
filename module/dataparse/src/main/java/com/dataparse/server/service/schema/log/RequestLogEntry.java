package com.dataparse.server.service.schema.log;

import com.dataparse.server.service.visualization.bookmark_state.utils.*;
import com.dataparse.server.service.visualization.request.*;
import lombok.*;

import javax.persistence.*;
import java.util.*;

@Data
@Entity
@DiscriminatorValue("q")
public class RequestLogEntry extends BookmarkActionLogEntry {

    @Lob private String query;
    @Lob private String showMeQuery;

    private long bytesProcessed;
    private boolean fromCache;

    private boolean countQuery;
    private boolean facetQuery;

    public static RequestLogEntry of(QueryRequest request){
        RequestLogEntry logEntry = new RequestLogEntry();
        logEntry.setStartTime(new Date());
        logEntry.setDatadocId(request.getDatadocId());
        logEntry.setBookmarkId(request.getBookmarkId());
        logEntry.setFacetQuery(request.isFacetQuery());
        logEntry.setTableId(request.getTableId());
        logEntry.setCountQuery(request instanceof CountRequest);
        if(!request.isFacetQuery() && !logEntry.isCountQuery()){
            logEntry.setShowMeQuery(new QueryToString(request.getColumns(), request.getParams()).toString());
        }
        return logEntry;
    }

}
