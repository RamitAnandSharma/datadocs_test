package com.dataparse.server.service.flow.cache;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FlowPreviewResultCacheKey {

    private Long bookmarkId;
    private String nodeId;

    @Override
    public String toString() {
        return bookmarkId + ":" + nodeId;
    }
}
