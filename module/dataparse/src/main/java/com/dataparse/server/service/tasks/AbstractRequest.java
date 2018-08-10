package com.dataparse.server.service.tasks;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.user.User;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class AbstractRequest
{
    private String parentTask;
    private String routingKey;

    private Auth auth;

    @JsonIgnore
    public abstract Class<? extends AbstractTask> getTaskClass();
}
