package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.visualization.bookmark_state.state.QueryParams;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AbstractRefreshFilterRequest {

    private Long datadocId;
    private Long tableBookmarkId;
    private QueryParams params;
}
