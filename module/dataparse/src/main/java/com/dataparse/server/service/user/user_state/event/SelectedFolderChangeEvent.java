package com.dataparse.server.service.user.user_state.event;

import com.dataparse.server.service.user.user_state.state.*;
import lombok.*;

@Data
public class SelectedFolderChangeEvent extends UserStateChangeEvent {

    private Integer folderId;

    @Override
    public void apply(final UserState state) {
        state.setSelectedFolderId(folderId);
    }
}
