package com.dataparse.server.controllers.api.flow;

import com.dataparse.server.controllers.api.table.AbstractBookmarkRequest;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;

@Data
@NoArgsConstructor
public class PreviewRequest extends AbstractBookmarkRequest {

    @NotNull
    private String rootNode;
    private Boolean force = true;

    @NotNull
    @Max(1000)
    private Integer limit;

    public PreviewRequest(BookmarkStateId stateId, String rootNode, Boolean force, Integer limit) {
        super(stateId);
        this.rootNode = rootNode;
        this.force = force;
        this.limit = limit;
    }

}
