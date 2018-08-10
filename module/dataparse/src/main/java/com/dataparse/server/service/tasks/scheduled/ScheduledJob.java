package com.dataparse.server.service.tasks.scheduled;

import lombok.Data;

import java.util.*;

@Data
public abstract class ScheduledJob implements Runnable {

    private String cronExpression;
    private TimeZone timeZone;

    @Override
    public boolean equals(Object o) {
        throw new IllegalStateException("You have to override equals and hashcode for correct queue managing.");
    }

    @Override
    public int hashCode() {
        throw new IllegalStateException("You have to override equals and hashcode for correct queue managing.");
    }
}
