package com.dataparse.server.service.notification;

import lombok.Data;

@Data
public abstract class Event {

    private String instanceId;
    private Long user;
    private String sessionId;

    abstract public EventType getType();

}
