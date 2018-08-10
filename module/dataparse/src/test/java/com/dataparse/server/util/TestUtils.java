package com.dataparse.server.util;

public class TestUtils {

    public static boolean isExceptionThrown(Class<? extends Exception> clazz, Runnable r){
        try {
            r.run();
            return false;
        } catch (Exception e){
            return clazz.isAssignableFrom(e.getClass());
        }
    }

}
