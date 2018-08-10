package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.visualization.bookmark_state.state.QueryParams;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Builder;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchIndexRequest {

    /** ID of BQ's anonymous table */
    private String externalId;

    private Long tableId;
    private Long tableBookmarkId;
    private UUID stateId;
    private String scrollId;

    private QueryParams params;
    private Long from = 0L;

}
