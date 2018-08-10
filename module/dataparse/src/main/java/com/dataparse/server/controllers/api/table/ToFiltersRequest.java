package com.dataparse.server.controllers.api.table;

import lombok.Data;

@Data
public class ToFiltersRequest {

    Long tableBookmarkId;
    String query;


}
