package com.dataparse.server.sql;

import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.parser.jdbc.Protocol;
import com.dataparse.server.service.parser.writer.CSVWriter;
import com.dataparse.server.util.DbUtils;
import com.dataparse.server.util.MapUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.Map;

@Slf4j
public class DbQueriesTest {

    private Integer ROWS_COUNT = 50_000_000;
    private Integer BATCH_SIZE = 2000;

    private final String DATABASE = "testdb";
    private String MYSQL_CREATE_TABLE = "CREATE TABLE if not exists test_table (one VARCHAR(100), two DOUBLE, " +
            " three FLOAT, four INTEGER, five BIGINT, six DATE);" ;

    private String POSTGRESQL_CREATE_TABLE = "CREATE TABLE if not exists test_table (one VARCHAR(100), two FLOAT, " +
            " three FLOAT, four INTEGER, five BIGINT, six DATE);";
    private String POSTGRESQL_COPY_TO = "copy test_table FROM '/tmp/test_file.csv' DELIMITER ',' CSV HEADER;";
    private String MYSQL_COPY_TO = "load data LOCAL infile '/tmp/test_file.csv' into table test_table;";

    @Test
    public void generateCsv() throws IOException {
        generateCsvFile();
    }

    @Test
    public void mysqlTests() throws SQLException {
        ConnectionParams connectionParams = new ConnectionParams(null, Protocol.mysql, "localhost", 3306, DATABASE, DATABASE, DATABASE);
        Connection connection = DbUtils.createConnection(connectionParams);
        createTable(connection, MYSQL_CREATE_TABLE);
        insertData(connection);
    }

    @Test
    public void postgresTests() throws SQLException {
        ConnectionParams connectionParams = new ConnectionParams(null, Protocol.postgresql, "localhost", 5432, DATABASE, DATABASE, DATABASE);
        Connection connection = DbUtils.createConnection(connectionParams);
        createTable(connection, POSTGRESQL_CREATE_TABLE);
        insertData(connection);

    }


    @Test
    @Ignore
    public void mySqlCountTest() throws SQLException {
        String db = "avails";
        String pass = "password";
        ConnectionParams connectionParams = new ConnectionParams(null, Protocol.mysql, "avails-provisioned.cyrnzp9lr1gr.us-west-1.rds.amazonaws.com", 3306, db, pass, db);
        Connection connection = DbUtils.createConnection(connectionParams);
        Statement statement1 = DbUtils.createStatement(connection, connectionParams, null);
        Statement statement2 = DbUtils.createStatement(connection, connectionParams, null);
        Statement statement3 = DbUtils.createStatement(connection, connectionParams, null);

        statement1.execute("select @rank := 0");
        ResultSet rs2 = statement2.executeQuery("select *, @rank := @rank + 1 from __wb limit 50000");
        ResultSet rs3 = statement3.executeQuery("select @rank");
        rs3.next();

        System.out.println(rs3.getLong(1));
        rs3.close();
        statement3.close();
        int counter = 0;
        while (rs2.next()) {
            Object object = rs2.getObject(1);
            counter++;
        }
        System.out.println(counter);
    }


    private void createTable(Connection connection, String query) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.execute(query);
        }
    }

    private void insertData(Connection connection) throws SQLException {
        try(PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_table (one, two, three, four, five, six) values (?,?,?,?,?,?)")) {
            Date date = new Date(new java.util.Date().getTime());

            for(int i = 0; i < ROWS_COUNT; i++) {
                preparedStatement.setString(1, String.valueOf(i));
                preparedStatement.setDouble(2, i * 1.0d);
                preparedStatement.setFloat(3, i * 1.0f);
                preparedStatement.setInt(4, i);
                preparedStatement.setLong(5, i);
                preparedStatement.setDate(6, date);
                preparedStatement.addBatch();

                if(i % BATCH_SIZE == 0) {
                    log.info("Inserted " + i + " rows");
                    preparedStatement.executeBatch();
                }

            }
            preparedStatement.executeBatch();
        }
    }

    private void generateCsvFile() throws IOException {
        File file = new File(new File("/tmp"), "test_file.csv");
        if(file.exists()) {
            file.delete();
        }
        file.createNewFile();
        OutputStream writer = new FileOutputStream(file);
        List<String> headers = Lists.newArrayList("one", "two", "three", "four", "five", "six");
        CSVWriter csvWriter = new CSVWriter(writer, headers);
        for (int i = 0; i < ROWS_COUNT; i++) {
            Map<String, Object> data = MapUtils.buildMap(Lists.newArrayList(
                    Pair.of("one", i),
                    Pair.of("two", i),
                    Pair.of("three", i),
                    Pair.of("four", i),
                    Pair.of("five", i),
                    Pair.of("six", new java.util.Date())
            ));
            csvWriter.writeRecord(data);
            if(i % 100000 == 0) {
                System.out.println("Write " + i + " to csv.");
            }
        }
        writer.flush();
        writer.close();
    }

}
