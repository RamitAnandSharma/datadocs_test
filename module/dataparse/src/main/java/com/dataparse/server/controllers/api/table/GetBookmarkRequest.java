package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import lombok.*;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GetBookmarkRequest {
    private Long tabId;
    private UUID stateId;

    public BookmarkStateId getBookmarkStateId() {
        return new BookmarkStateId(tabId, stateId);
    }
}
