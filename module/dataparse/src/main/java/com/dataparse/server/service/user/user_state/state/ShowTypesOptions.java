package com.dataparse.server.service.user.user_state.state;

import lombok.Data;

@Data
public class ShowTypesOptions {
    private boolean datadocsOnly = true;
    private boolean foldersOnly = true;
    private boolean sourcesOnly;
}
