package com.mysql.jdbc;

import java.sql.SQLException;

public class MySqlResultSetWrapper {

    public static Field getField(ResultSetMetaData rsmd, int i) throws SQLException {
        return rsmd.getField(i);
    }
}
