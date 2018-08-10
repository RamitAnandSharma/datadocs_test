package com.dataparse.server.config;

import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

import java.io.File;

public class TimeBasedRollingPolicyWithRemove<E> extends TimeBasedRollingPolicy<E> {

    @Override
    public boolean isTriggeringEvent(File activeFile, E event) {
        boolean triggeringEvent = super.isTriggeringEvent(activeFile, event);
        if(triggeringEvent) {
            activeFile.delete();
        }
        return triggeringEvent;
    }

}
