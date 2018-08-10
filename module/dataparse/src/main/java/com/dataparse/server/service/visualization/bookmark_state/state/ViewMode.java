package com.dataparse.server.service.visualization.bookmark_state.state;

import com.fasterxml.jackson.annotation.*;

public enum ViewMode {
    COLUMN,
    BAR,
    PIE,
    LINE,
    AREA,
    MAP,
    SCATTER,
    TABLE,
    LIST;

    @JsonValue
    public int toValue() {
        return ordinal();
    }
}
