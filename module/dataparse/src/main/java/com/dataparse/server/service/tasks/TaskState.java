package com.dataparse.server.service.tasks;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface TaskState {

    default int getTimeout(){
        return 1000;
    }

    default int getMaxRetries() {
        return 0;
    }

    String name();

    int ordinal();
}
