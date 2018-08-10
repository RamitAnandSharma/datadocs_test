package com.dataparse.server.service.db;

import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.util.DbUtils;
import com.dataparse.server.util.db.*;
import com.google.common.collect.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


@Slf4j
@Service
public class DBService {

    @Autowired
    private DbConnectionProvider dbConnectionProvider;


    public List<TableInfo> getTables(ConnectionParams params) {
        return getTables(params, false);
    }

    public List<TableInfo> getTables(ConnectionParams params, Boolean withoutRowsCount) {
        try(Connection connection = dbConnectionProvider.getConnection(params)) {
            return getTables(params, connection, withoutRowsCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean test(ConnectionParams params) {
        try(Connection connection = dbConnectionProvider.getConnection(params)) {
            return connection.isValid(5000);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<TableInfo> getTables(ConnectionParams params, Connection connection, Boolean withoutRowsCount) throws SQLException {

        String query = DbUtils.getColumnsQuery(params.getProtocol());
        Map<String, TableInfo> result = new HashMap<>();

        try(PreparedStatement stmt = connection.prepareStatement(query)){
            if(query.contains("?")) {
                stmt.setString(1, params.getDbName());
            }
            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next()) {
                ColumnInfo column = new ColumnInfo();
                column.setName(resultSet.getString("COLUMN_NAME"));
                column.setType(DbUtils.getDataType(params.getProtocol(), resultSet.getString("DATA_TYPE"), null));
                String tableName = resultSet.getString("TABLE_NAME");
                column.setInitialIndex(resultSet.getInt("POSITION") - 1);

                TableInfo table = result.getOrDefault(tableName, new TableInfo(tableName));
                table.getColumns().add(column);
                result.put(tableName, table);
            }
            if(!withoutRowsCount) {
                Map<String, Long> countData = retrieveRowsCount(params, connection);
                result.forEach((tableName, tableInfo) -> tableInfo.setRowsCount(countData.getOrDefault(tableName, -1L)));
            }
            return Lists.newArrayList(result.values());
        }
    }

    private Map<String, Long> retrieveRowsCount(ConnectionParams params, Connection connection) throws SQLException {
        String countQuery = DbUtils.getRowsEstimateCountQuery(params.getProtocol());
        Map<String, Long> result = new HashMap<>();

        try (PreparedStatement stmt = connection.prepareStatement(countQuery)) {
            if (countQuery.contains("?")) {
                stmt.setString(1, params.getDbName());
            }
            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                long rowsCount = resultSet.getLong("TABLE_ROWS");
                result.put(tableName, rowsCount);
            }
        }
        return result;
    }
}