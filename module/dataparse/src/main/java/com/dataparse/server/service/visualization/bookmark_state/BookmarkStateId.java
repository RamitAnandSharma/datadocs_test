package com.dataparse.server.service.visualization.bookmark_state;


import com.dataparse.server.auth.Auth;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@NoArgsConstructor
public class BookmarkStateId implements Serializable {
    private Long tabId;
    private UUID stateId;
    private Long userId;

    public BookmarkStateId(Long tabId, UUID stateId) {
        this(tabId, stateId, Auth.get().getUserId());
    }
    public BookmarkStateId(Long tabId, UUID stateId, Long userId) {
        this.tabId = tabId;
        this.stateId = stateId;
        this.userId = userId;
    }
}
