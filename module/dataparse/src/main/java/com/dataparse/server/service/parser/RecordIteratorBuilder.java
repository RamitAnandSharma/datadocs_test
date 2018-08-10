package com.dataparse.server.service.parser;

import com.dataparse.server.service.flow.transform.ColumnTransform;
import com.dataparse.server.service.parser.iterator.*;
import com.dataparse.server.service.parser.type.ColumnInfo;

import java.util.List;

public class RecordIteratorBuilder {

    RecordIterator it;

    private RecordIteratorBuilder(RecordIterator it){
        this.it = it;
    }

    public static RecordIteratorBuilder with(RecordIterator it){
        return new RecordIteratorBuilder(it);
    }

    public RecordIterator build(){
        return it;
    }

    public RecordIteratorBuilder withTransforms(List<ColumnTransform> transforms){
        it = RecordIterator.withTransforms(it, transforms);
        return this;
    }

    public RecordIteratorBuilder withColumns(List<ColumnInfo> columnInfo){
        it = RecordIterator.withColumns(it, columnInfo);
        return this;
    }

    public RecordIteratorBuilder withSkippedRows(Integer skipAfterHeader, Integer skipBeforeBottom){
        it = RecordIterator.withSkippedRows(it, skipAfterHeader, skipBeforeBottom);
        return this;
    }

    public RecordIteratorBuilder interruptible(){
        it = RecordIterator.interruptible(it);
        return this;
    }

    public RecordIteratorBuilder limited(Integer limited){
        if(limited != null) {
            it = RecordIterator.limited(it, limited);
        }
        return this;
    }

}
