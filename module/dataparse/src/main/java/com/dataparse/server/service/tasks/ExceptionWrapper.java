package com.dataparse.server.service.tasks;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.function.Supplier;

public class ExceptionWrapper {

    public static RuntimeException wrap(Exception e){
        Throwable t;
        int indexOfSqlException = ExceptionUtils.indexOfType(e, SQLException.class);
        if(indexOfSqlException >= 0){
            t = ExceptionUtils.getThrowables(e)[indexOfSqlException];
        } else {
            t = ExceptionUtils.getRootCause(e);
        }
        if(t instanceof SQLException){
            String code = ((SQLException) t).getSQLState();
            if(code == null && t.getMessage().contains("Connection is not available")){
                return ExecutionException.of("db_conn_refused", "Connection refused");
            }
            if(code.startsWith("28")){
                return ExecutionException.of("db_auth_error", "Authentication error");
            } else if (code.startsWith("3D")) {
                return ExecutionException.of("db_not_exists", "Database does not exist");
            } else if (code.startsWith("08")) {
                return ExecutionException.of("db_conn_refused", "Connection refused");
            } else if(code.startsWith("42S02")) {
                return ExecutionException.of("no_such_table", t.getMessage());
            } else if (code.startsWith("42")) {
                return ExecutionException.of("access_denied_error", "Access denied");
            }
        }
        if(t instanceof ConnectException){
            return ExecutionException.of("db_conn_refused", "Connection refused");
        }
        if(t instanceof UnknownHostException){
            return ExecutionException.of("db_unknown_host", "Unknown host");
        }

        Throwable rootCause = ExceptionUtils.getRootCause(e);
        if(rootCause != null && rootCause instanceof Exception) {
            return wrap((Exception) rootCause);
        }
        return new RuntimeException(e);
    }

    public static <T> T wrap(Supplier<T> s){
        try {
            return s.get();
        } catch (RuntimeException e) {
            throw wrap(e);
        }
    }

}
