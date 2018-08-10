package com.dataparse.server.service.user.user_state.event;

import com.dataparse.server.service.user.user_state.state.*;
import lombok.*;

@Data
public class ShowTypesOptionsChangeEvent extends UserStateChangeEvent {

    private boolean datadocsOnly;
    private boolean foldersOnly;
    private boolean sourcesOnly;

    @Override
    public void apply(final UserState state) {
        state.getShowTypesOptions().setDatadocsOnly(datadocsOnly);
        state.getShowTypesOptions().setFoldersOnly(foldersOnly);
        state.getShowTypesOptions().setSourcesOnly(sourcesOnly);
    }
}
