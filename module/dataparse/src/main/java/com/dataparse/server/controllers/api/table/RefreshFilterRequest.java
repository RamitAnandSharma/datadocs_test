package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.visualization.bookmark_state.state.QueryParams;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class RefreshFilterRequest extends AbstractRefreshFilterRequest {
    private String filter;

    public RefreshFilterRequest(Long datadocId, Long tableBookmarkId, QueryParams params, String filter) {
        super(datadocId, tableBookmarkId, params);
        this.filter = filter;
    }
}
