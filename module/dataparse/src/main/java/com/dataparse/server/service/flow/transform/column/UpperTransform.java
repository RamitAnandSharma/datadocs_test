package com.dataparse.server.service.flow.transform.column;

import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.flow.transform.ColumnTransform;

public class UpperTransform extends ColumnTransform {

    @Override
    protected Object apply(Object o) {
        if(o == null){
            return null;
        }
        if(o instanceof String){
            return ((String) o).toUpperCase();
        }
        return new ErrorValue("String value required");
    }
}
