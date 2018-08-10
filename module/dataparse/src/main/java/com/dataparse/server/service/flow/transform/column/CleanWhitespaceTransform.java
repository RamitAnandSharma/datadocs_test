package com.dataparse.server.service.flow.transform.column;

import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.flow.transform.ColumnTransform;

public class CleanWhitespaceTransform extends ColumnTransform {

    @Override
    protected Object apply(Object o) {
        if(o == null){
            return null;
        }
        if(o instanceof String){
            String s = (String) o;
            return s.trim().replaceAll("\\s+", " ");
        }
        return new ErrorValue("String value required");
    }
}
