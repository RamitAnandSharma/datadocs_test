package com.dataparse.server.util;

import java.util.Optional;

public class SystemUtils {

    public static String getProperty(String name, String defaultValue){
        return Optional.ofNullable(System.getenv(name))
                .orElse(Optional.ofNullable(System.getProperty(name))
                        .orElse(defaultValue));
    }

    public static Integer getProperty(String name, Integer defaultValue){
        return Integer.parseInt(getProperty(name, defaultValue.toString()));
    }

    public static Boolean getProperty(String name, Boolean defaultValue){
        return Boolean.parseBoolean(getProperty(name, defaultValue.toString()));
    }
}
