package com.dataparse.server.service.flow.transform;

import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.upload.Descriptor;

import java.util.Collections;

public abstract class TableTransform implements Transform {

    public Descriptor apply(Descriptor d){
        Descriptor result = applyInternal(d);
        result.setColumns(ColumnInfo.aggregateColumns(Collections.singletonList(d)));
        return result;
    }

    public abstract Descriptor applyInternal(Descriptor d);

}
