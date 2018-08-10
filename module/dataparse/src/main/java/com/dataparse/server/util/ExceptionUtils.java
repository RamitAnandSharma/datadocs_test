package com.dataparse.server.util;

public class ExceptionUtils {

    public static String getRootCauseMessage(Exception e){
        Throwable t = org.apache.commons.lang3.exception.ExceptionUtils.getRootCause(e);
        if(t == null){
            t = e;
        }
        return t.getMessage();
    }

}
