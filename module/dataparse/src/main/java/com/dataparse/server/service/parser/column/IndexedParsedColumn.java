package com.dataparse.server.service.parser.column;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class IndexedParsedColumn extends AbstractParsedColumn {
    private Integer index;

    public Integer getIndex() {
        return index;
    }

    public IndexedParsedColumn(Integer index, String columnName) {
        super(columnName);
        this.index = index;
    }

    @Override
    public int hashCode() {
        return this.index.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof IndexedParsedColumn)) {
            return false;
        }
        IndexedParsedColumn that = (IndexedParsedColumn) obj;
        return this.index.equals(that.getIndex());
    }

}
