package com.dataparse.server.controllers.api.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MoveTableBookmarkRequest {

    Long tableBookmarkId;
    int toPosition;

}
