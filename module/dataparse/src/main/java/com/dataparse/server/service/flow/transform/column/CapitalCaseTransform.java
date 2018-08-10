package com.dataparse.server.service.flow.transform.column;

import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.flow.transform.ColumnTransform;
import org.apache.commons.lang3.text.WordUtils;

public class CapitalCaseTransform extends ColumnTransform {

    @Override
    protected Object apply(Object o) {
        if(o == null){
            return null;
        }
        if(o instanceof String){
            return WordUtils.capitalizeFully((String) o);
        }
        return new ErrorValue("String value required");
    }
}
