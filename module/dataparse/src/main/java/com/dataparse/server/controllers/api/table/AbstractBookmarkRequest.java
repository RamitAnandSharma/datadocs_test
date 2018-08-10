package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;
import java.util.UUID;


@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class AbstractBookmarkRequest {

    @NotNull
    private BookmarkStateId bookmarkStateId;

    @JsonIgnore
    public Long getTabId() {
        if(bookmarkStateId == null) {
            return null;
        }
        return bookmarkStateId.getTabId();
    }

    @JsonIgnore
    public UUID getStateId() {
        if(bookmarkStateId == null) {
            return null;
        }
        return bookmarkStateId.getStateId();
    }

}
