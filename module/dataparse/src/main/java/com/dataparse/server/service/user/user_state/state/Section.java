package com.dataparse.server.service.user.user_state.state;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Section {
    MY_DATA,
    USER_SETTINGS;

    @JsonValue
    public int toValue() {
        return ordinal();
    }
}
