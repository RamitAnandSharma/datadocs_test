package com.dataparse.server.service.upload;

import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;

import java.util.Map;

@Data
@NoArgsConstructor
public class CollectionDelegateDescriptor extends Descriptor {

    private Descriptor d;
    private Iterable<Map<AbstractParsedColumn, Object>> iterable;

    public CollectionDelegateDescriptor(Iterable<Map<AbstractParsedColumn, Object>> iterable){
        this.iterable = iterable;
    }

    public CollectionDelegateDescriptor(Descriptor d, Iterable<Map<AbstractParsedColumn, Object>> iterable){
        this(iterable);
        this.d = d;
        BeanUtils.copyProperties(d, this);
    }

    @Override
    public DataFormat getFormat() {
        return DataFormat.COLLECTION_DELEGATE;
    }

    @Override
    public int hashCode() {
        if(d == null) {
            return 0;
        }
        return d.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(d == null) {
            return false;
        }
        return d.equals(obj);
    }
}
