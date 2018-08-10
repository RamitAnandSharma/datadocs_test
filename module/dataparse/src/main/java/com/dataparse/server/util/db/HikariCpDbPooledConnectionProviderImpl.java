package com.dataparse.server.util.db;

import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.parser.jdbc.Protocol;
import com.dataparse.server.service.tasks.ExceptionWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.mysql.jdbc.JDBC4Connection;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class HikariCpDbPooledConnectionProviderImpl implements DbConnectionProvider {

    private final Cache<ConnectionParams, HikariDataSource> connectionPoolCache
            = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(24, TimeUnit.HOURS)
            .removalListener((RemovalListener<ConnectionParams, HikariDataSource>) notification -> {
                notification.getValue().close();
                log.info("Removed connection pool {} due to: " + notification.getCause(), notification.getKey().toString());
            })
            .build();


    @Override
    public Connection getConnection(ConnectionParams params) {
        return getConnection(params, true);
    }

    @Override
    public Connection getConnection(ConnectionParams params, Boolean streaming) {
        HikariDataSource ds;
        boolean isMySQL = params.getProtocol().equals(Protocol.mysql);
        try {
            ds = connectionPoolCache.get(params, () -> {
                HikariConfig config = new HikariConfig();
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

                if (isMySQL) {
                    dbUrl.append("?useSSL=false&clobberStreamingResults=true&zeroDateTimeBehavior=convertToNull");
                }

                config.setJdbcUrl(dbUrl.toString());
                config.setUsername(params.getUser());
                config.setPassword(params.getPassword());
                config.setReadOnly(true);
                config.setAutoCommit(true);
                return new HikariDataSource(config);
            });
        } catch (Exception e) {
            throw ExceptionWrapper.wrap(e);
        }

        long start = System.currentTimeMillis();
        Connection conn;
        try {
            conn = ds.getConnection();
            log.info("Connected to {} in {}", params, (System.currentTimeMillis() - start));
            if(isMySQL && streaming){
                conn.unwrap(JDBC4Connection.class).setClobberStreamingResults(true);
            }
        } catch (Exception e) {
            log.warn("Rejected connection to {} in {}", params, (System.currentTimeMillis() - start));
            throw ExceptionWrapper.wrap(e);
        }
        return conn;
    }

}
