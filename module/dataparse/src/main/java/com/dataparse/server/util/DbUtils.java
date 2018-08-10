package com.dataparse.server.util;

import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.parser.jdbc.Protocol;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.tasks.*;
import com.mysql.jdbc.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.regex.*;

@Slf4j
public class DbUtils {

    public static DataFormat getRemoteDataSourceFormat(Protocol protocol) {
        DataFormat dataFormat;
        switch (protocol) {
            case mysql:
                dataFormat = DataFormat.MYSQL_QUERY;
                break;
            case postgresql:
                dataFormat = DataFormat.POSTGRESQL_QUERY;
                break;
            case sqlserver:
                dataFormat = DataFormat.MSSQL_QUERY;
                break;
            case oracle:
                dataFormat = DataFormat.ORACLE_QUERY;
                break;
            default:
                throw new RuntimeException("Unknown protocol: " + protocol);
        }
        return dataFormat;
    }

    public static TypeDescriptor getDataType(Protocol protocol, String dataTypeString, Integer dataTypeSize){
        dataTypeString = dataTypeString.toLowerCase();
        if(protocol == Protocol.mysql && dataTypeSize == null) {
            // try to parse data type size from data type string (column_type)
            try {
                Pattern p = Pattern.compile("(.*?)\\((.*?)\\)");
                Matcher m = p.matcher(dataTypeString);
                while(m.find()) {
                    dataTypeString = m.group(1);
                    dataTypeSize = Integer.parseInt(m.group(2));
                }
            } catch (Exception e){
                // do nothing
            }
        }
        switch (protocol){
            case mysql:
                switch (dataTypeString){
                    case "tinyint":
                    case "tinyint unsigned":
                    case "bit":
                        if(dataTypeSize != null && dataTypeSize == 1){
                            return new TypeDescriptor(DataType.STRING);
                        }
                    case "smallint":
                    case "smallint unsigned":
                    case "mediumint":
                    case "mediumint unsigned":
                    case "int":
                    case "int unsigned":
                    case "integer":
                    case "serial":
                    case "bigint":
                    case "bigint unsigned":
                        return new NumberTypeDescriptor(true, false);
                    case "decimal":
                    case "dec":
                    case "float":
                    case "float unsigned":
                    case "double":
                    case "double unsigned":
                    case "double precision":
                    case "real":
                        return new NumberTypeDescriptor();
                    case "date":
                    case "datetime":
                    case "timestamp":
                        return new DateTypeDescriptor();
                    case "time":
                        return new TimeTypeDescriptor();
                    case "boolean":
                    case "bool":
                        return new TypeDescriptor(DataType.STRING);
                    default:
                        return new TypeDescriptor(DataType.STRING);
                }
            case postgresql:
                switch (dataTypeString){
                    case "bigint":
                    case "int8":
                    case "bigsearial":
                    case "serial8":
                    case "bit":
                    case "bit varying":
                    case "varbit":
                    case "integer":
                    case "int":
                    case "int4":
                    case "smallint":
                    case "int2":
                    case "searial":
                    case "serial4":
                        return new NumberTypeDescriptor(true, false);
                    case "double precision":
                    case "float8":
                    case "numeric":
                    case "decimal":
                    case "real":
                    case "float4":
                        return new NumberTypeDescriptor();
                    case "date":
                    case "timestamp":
                    case "timestamp with time zone":
                    case "timestamp without time zone":
                    case "timestamptz":
                        return new DateTypeDescriptor();
                    case "time":
                    case "time with time zone":
                    case "time without time zone":
                    case "timetz":
                        return new TimeTypeDescriptor();
                    case "boolean":
                    case "bool":
                        return new TypeDescriptor(DataType.STRING);
                    default:
                        return new TypeDescriptor(DataType.STRING);
                }
            case oracle:
                switch (dataTypeString){
                    case "number":
                    case "long":
                        return new NumberTypeDescriptor(true, false);
                    case "binary_float":
                    case "binary_double":
                        return new NumberTypeDescriptor();
                    case "date":
                        return new DateTypeDescriptor();
                    default:
                        return new TypeDescriptor(DataType.STRING);
                }
            case sqlserver:
                switch (dataTypeString){
                    case "int":
                    case "bigint":
                    case "numeric":
                    case "smallint":
                    case "bit":
                    case "smallmoney":
                    case "tinyint":
                    case "money":
                        return new NumberTypeDescriptor(true, false);
                    case "float":
                    case "real":
                        return new NumberTypeDescriptor();
                    case "date":
                    case "datetimeoffset":
                    case "datetime2":
                    case "smalldatetime":
                    case "datetime":
                        return new DateTypeDescriptor();
                    case "time":
                        return new TimeTypeDescriptor();
                    default:
                        return new TypeDescriptor(DataType.STRING);
                }
            default:
                throw new RuntimeException("Unknown protocol: " + protocol);
        }
    }

    public static Connection createConnection(ConnectionParams params) {
        StringBuilder dbUrl = new StringBuilder();
        dbUrl.append("jdbc:").append(params.getProtocol());
        if(params.getProtocol().getDriverType() != null){
            dbUrl.append(":").append(params.getProtocol().getDriverType());
        }
        if(params.getProtocol().equals(Protocol.oracle)){
            dbUrl.append(":@");
        } else {
            dbUrl.append("://");
        }
        dbUrl.append(params.getHost()).append(":").append(params.getPort());

        if(params.getProtocol().equals(Protocol.sqlserver)){
            dbUrl.append(";databaseName=").append(params.getDbName());
        } else if(params.getProtocol().equals(Protocol.oracle)){
            dbUrl.append(":").append(params.getDbName());
        } else {
            dbUrl.append("/").append(params.getDbName());
        }

        if (params.getProtocol().equals(Protocol.mysql)) {
            dbUrl.append("?connectTimeout=1000");
        }

        long start = System.currentTimeMillis();
        try {
            Connection conn = DriverManager.getConnection(dbUrl.toString(), params.getUser(), params.getPassword());
            log.info("Retrieved connection to {} in {}", dbUrl.toString(), (System.currentTimeMillis() - start));
            if(params.getProtocol().equals(Protocol.mysql)){
                ((JDBC4Connection) conn).setClobberStreamingResults(true);
            }
            return conn;
        } catch (Exception e) {
            log.warn("Rejected connection to {} in {}", dbUrl.toString(), (System.currentTimeMillis() - start));
            throw ExceptionWrapper.wrap(e);
        }
    }

    public static Statement createStatement(Connection conn, ConnectionParams connectionParams, Integer limit) throws SQLException {
        Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if(connectionParams.getProtocol().equals(Protocol.mysql)) {
            st.setFetchSize(Integer.MIN_VALUE);
            if(limit != null) {
                st.setMaxRows(limit);
                st.setFetchSize(limit);
            }
        } else {
            st.setFetchSize(1000);
        }
        return st;
    }

    public static void close(Connection connection, Statement stmt, ResultSet rs) {
        try {
            if (rs != null){
                rs.close();
            }
        } catch (SQLException e){
            log.error("Can't close result set", e);
        }
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            log.error("Can't close statement", e);
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            log.error("Can't close connection", e);
        }
    }

    public static String quote(Protocol protocol, String s){
        char quoteChar = DbUtils.getQuoteChar(protocol);
        return quoteChar + s + quoteChar;
    }

    public static String getRowsEstimateCountQuery(Protocol protocol, String database, String table){
        switch (protocol){
            case mysql:
                return "SELECT table_name, table_rows " +
                       "FROM information_schema.tables " +
                       "WHERE table_schema = " + quote(protocol, database) + " and table_name = " + quote(protocol, table);
            default:
                return "SELECT count(*) AS total FROM " + quote(protocol, table);
        }
    }

    public static String getRowsEstimateCountQuery(Protocol protocol){
        switch (protocol){
            case mysql:
                return "SELECT table_name, table_rows " +
                        "FROM information_schema.tables " +
                        "WHERE table_schema = ?";
            case postgresql:
                return "SELECT " +
                        "  pgClass.relname   AS table_name," +
                        "  pgClass.reltuples AS table_rows " +
                        "FROM" +
                        "  pg_class pgClass " +
                        "LEFT JOIN" +
                        "  pg_namespace pgNamespace ON (pgNamespace.oid = pgClass.relnamespace) " +
                        "WHERE" +
                        "  pgNamespace.nspname NOT IN ('pg_catalog', 'information_schema') AND " +
                        "  pgClass.relkind='r'";
            case sqlserver:
                return "SELECT o.name as table_name, " +
                        "  ddps.row_count as table_rows " +
                        "FROM sys.indexes AS i" +
                        "  INNER JOIN sys.objects AS o ON i.OBJECT_ID = o.OBJECT_ID" +
                        "  INNER JOIN sys.dm_db_partition_stats AS ddps ON i.OBJECT_ID = ddps.OBJECT_ID" +
                        "  AND i.index_id = ddps.index_id " +
                        "WHERE i.index_id < 2  AND o.is_ms_shipped = 0 ORDER BY o.NAME ";
            case oracle:
                return "select table_name, num_rows as table_rows from user_tables";
            default:
                throw new RuntimeException("Protocol is not supported: " + protocol);
        }
    }

    public static String getColumnsQuery(Protocol protocol){
        switch (protocol){
            case mysql:
                // for MySQL it's important to get column_type as it has column type size in brackets, like "tinyint(1)"
                return "SELECT table_name, column_name, column_type as 'data_type', ordinal_position as 'position' " +
                        " FROM information_schema.columns" +
                        " WHERE table_schema = ?";
            case postgresql:
                return "SELECT table_name, column_name, data_type, ordinal_position as position " +
                        " FROM information_schema.columns" +
                        " WHERE table_catalog = ? AND table_schema NOT IN ('pg_catalog', 'information_schema')";
            case sqlserver:
                return "SELECT column_name, table_name, column_name, data_type, ordinal_position as position " +
                        "FROM INFORMATION_SCHEMA.COLUMNS";
            case oracle:
                return "select table_name, column_name, data_type from user_tab_columns";
            default:
                throw new RuntimeException("Protocol is not supported: " + protocol);
        }
    }

    public static String getPkeyQuery(Protocol protocol){
        switch(protocol){
            case mysql:
                return "SELECT k.COLUMN_NAME " +
                        "FROM information_schema.table_constraints t " +
                        "LEFT JOIN information_schema.key_column_usage k " +
                        "USING(constraint_name,table_schema,table_name) " +
                        "WHERE t.constraint_type='PRIMARY KEY' " +
                        "    AND t.table_schema=DATABASE() " +
                        "    AND t.table_name = ?";
            case postgresql:
                return "SELECT c.column_name " +
                        "FROM information_schema.table_constraints tc " +
                        "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) " +
                        "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name " +
                        "where constraint_type = 'PRIMARY KEY' and tc.table_name = ?";
            case sqlserver:
                return "SELECT COLUMN_NAME " +
                        "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                        "WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 " +
                        "AND TABLE_NAME = ?";
            case oracle:
                return "SELECT cols.column_name " +
                        "FROM all_constraints cons, all_cons_columns cols " +
                        "WHERE cols.table_name = ?" +
                        "AND cons.constraint_type = 'P' " +
                        "AND cons.constraint_name = cols.constraint_name " +
                        "AND cons.owner = cols.owner " +
                        "ORDER BY cols.table_name, cols.position";
            default:
                throw new RuntimeException("Protocol is not supported: " + protocol);

        }
    }

    public static char getQuoteChar(Protocol protocol){
        switch (protocol){
            case mysql:
                return '`';
            case postgresql:
            case sqlserver:
            case oracle:
                return '"';
            default:
                throw new RuntimeException("Protocol is not supported: " + protocol);
        }
    }

    public static String getTablePrefixedColumnName(Protocol protocol, ResultSetMetaData rsmd, int colIndex)
            throws SQLException{
        String tableNameOrAlias;
        switch (protocol){
            // unfortunately only MySQL provides table alias info
            case mysql:
                tableNameOrAlias = MySqlResultSetWrapper.getField((com.mysql.jdbc.ResultSetMetaData) rsmd, colIndex).getTableName();
                break;
            default:
                tableNameOrAlias = rsmd.getTableName(colIndex);
        }
        return tableNameOrAlias + "." + rsmd.getColumnLabel(colIndex);
    }
}
