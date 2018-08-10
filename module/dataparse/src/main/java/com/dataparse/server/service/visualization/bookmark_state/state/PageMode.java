package com.dataparse.server.service.visualization.bookmark_state.state;

import com.fasterxml.jackson.annotation.JsonValue;

public enum PageMode {
    VIZ,
    INGEST;

    @JsonValue
    public int toValue() {
        return ordinal();
    }
}
