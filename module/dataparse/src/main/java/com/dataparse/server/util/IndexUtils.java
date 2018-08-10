package com.dataparse.server.util;

import java.util.Date;

public class IndexUtils {

    public static String getHistoryColumnName(String columnName, Date date){
        return columnName + "_" + date.getTime();
    }

    public static Date getHistoryColumnDate(String columnName, String originalColumnName){
        String prefix = originalColumnName + "_";
        if(columnName.startsWith(prefix)){
            try {
                return new Date(Long.parseLong(columnName.substring(prefix.length())));
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

}
