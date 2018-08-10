package com.dataparse.server.service.db;

import com.dataparse.server.service.parser.type.ColumnInfo;
import com.hazelcast.util.HashUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {

    private String name;
    private Long rowsCount;
    private List<ColumnInfo> columns = new ArrayList<>();

    public TableInfo(String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        return HashUtil.hashCode(this.name, this.columns);
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || !(o instanceof TableInfo)) {
            return false;
        }

        TableInfo that = (TableInfo) o;
        return this.name.equals(that.getName())
                && this.columns.equals(that.getColumns());
    }

}
