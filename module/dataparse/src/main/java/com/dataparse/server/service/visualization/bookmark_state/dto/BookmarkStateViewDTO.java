package com.dataparse.server.service.visualization.bookmark_state.dto;

import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BookmarkStateViewDTO {
    private UUID uuid;
    private String name;
    private Long tabId;

    public BookmarkStateViewDTO(BookmarkState bookmarkState) {
        this(bookmarkState.getId(), bookmarkState.getStateName(), bookmarkState.getTabId());
    }
}
