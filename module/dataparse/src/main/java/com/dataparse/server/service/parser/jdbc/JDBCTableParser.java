package com.dataparse.server.service.parser.jdbc;

import com.dataparse.server.service.upload.DbTableDescriptor;
import com.dataparse.server.util.DbUtils;

import java.util.List;
import java.util.stream.Collectors;

public class JDBCTableParser extends JDBCQueryParser {

    private String tableName;
    protected List<String> sortingColumns;

    public JDBCTableParser(DbTableDescriptor descriptor, Boolean preview) {
        super(descriptor, null, preview);
        this.tableName = descriptor.getTableName();
        this.knownCount = true;
    }

    protected String getCountQueryString(){
        return DbUtils.getRowsEstimateCountQuery(connectionParams.getProtocol(), connectionParams.getDbName(), this.tableName);
    }

    @Override
    protected String getQueryString() {
        return "SELECT * FROM " + quote(this.tableName)
                + (sortingColumns == null ? "" : " ORDER BY " + String.join(",", sortingColumns.stream().map(this::quote).collect(Collectors.toList())));
    }

    public void setSortingColumns(List<String> sortingColumns){
        this.sortingColumns = sortingColumns;
    }
}
