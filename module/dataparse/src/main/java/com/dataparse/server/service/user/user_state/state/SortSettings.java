package com.dataparse.server.service.user.user_state.state;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SortSettings {

    public SortSettings (boolean disabled) {
        this.disabled = disabled;
    }

    public SortSettings (SortDirection direction) {
        this.direction = direction;
    }

    private boolean disabled = false;
    private SortDirection direction;
}
