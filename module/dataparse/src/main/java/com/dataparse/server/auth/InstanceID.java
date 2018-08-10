package com.dataparse.server.auth;

import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

public class InstanceID {

    public static final String INSTANCE_ID = "INSTANCE_ID";

    public static String get()
    {
        if(RequestContextHolder.getRequestAttributes() != null)
        {
            return (String) RequestContextHolder.getRequestAttributes().getAttribute(INSTANCE_ID, RequestAttributes.SCOPE_REQUEST);
        }
        return null;
    }

    public static void set(String instanceId)
    {
        RequestContextHolder.getRequestAttributes().setAttribute(INSTANCE_ID, instanceId, RequestAttributes.SCOPE_REQUEST);
    }

}
