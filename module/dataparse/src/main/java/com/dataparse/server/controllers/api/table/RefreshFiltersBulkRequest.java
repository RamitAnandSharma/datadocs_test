package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.visualization.bookmark_state.state.QueryParams;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class RefreshFiltersBulkRequest extends AbstractRefreshFilterRequest {
    private List<String> filters;

    public RefreshFiltersBulkRequest(Long datadocId, Long tableBookmarkId, QueryParams params, List<String> filters) {
        super(datadocId, tableBookmarkId, params);
        this.filters = filters;

    }
}
