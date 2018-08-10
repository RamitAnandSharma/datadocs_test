package com.dataparse.server.sql;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.service.bigquery.BigQueryClient;
import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.parser.jdbc.Protocol;
import com.dataparse.server.service.storage.DefaultStorageStrategy;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.storage.GcsFileStorage;
import com.dataparse.server.service.storage.StorageType;
import com.dataparse.server.util.DbUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class PerformanceTests extends IsolatedContextTest {

    @Autowired
    private BigQueryClient bigQuery;

    @Autowired
    private DefaultStorageStrategy storageStrategy;

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    private static String accountId = "tidy-centaur-164320";
    private static String bigQueryDataset = "test";
    private static String bigQueryTable = "test";


    private static final String TABLE_NAME = "TEST";

    private Connection getLocalPostgresConnection() {
        ConnectionParams params = new ConnectionParams();
        params.setDbName("dataparse");
        params.setHost("localhost");
        params.setPort(5432);
        params.setProtocol(Protocol.postgresql);
        params.setUser("user");
        params.setPassword("user");
        return DbUtils.createConnection(params);
    }

    private Connection getRemoteMySQLConnection() {
        ConnectionParams params = new ConnectionParams();
        params.setDbName("avails");
        params.setHost("avails-provisioned.cyrnzp9lr1gr.us-west-1.rds.amazonaws.com");
        params.setPort(3306);
        params.setProtocol(Protocol.mysql);
        params.setUser("avails");
        params.setPassword("8XwZ7jFmf8ZCZ62");
        return DbUtils.createConnection(params);
    }

    public void insertToBigQueryStream(List<Map<String, Object>> rows) {
        List<InsertAllRequest.RowToInsert> r = rows.stream().map(row -> {
            try {
                Map<String, Object> copiedMap = new HashMap<>(row);
                List<String> keys = copiedMap.entrySet().stream().map(entry -> {
                    if(entry.getValue() != null) {
                        return entry.getKey();
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList());
                copiedMap.keySet().retainAll(keys);
                return InsertAllRequest.RowToInsert.of(copiedMap);
            } catch (Exception e) {
                System.out.println();
            }
            return null;
        }).collect(Collectors.toList());
        InsertAllRequest of = InsertAllRequest.of(bigQueryDataset, bigQueryTable, r);
        InsertAllResponse response = bigQuery.getClient(accountId).insertAll(of);

        if (response.hasErrors()) {
            System.out.println("Errors count " + response.getInsertErrors().entrySet().size());
        }
    }

    @Test
    @Ignore
    public void insertFileDirectlyToBigQuery() throws TimeoutException, InterruptedException {
        TableId tableId = TableId.of(bigQueryDataset, bigQueryTable);
        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId)
                        .setFormatOptions(FormatOptions.csv())
                        .build();
        TableDataWriteChannel writer = bigQuery.getClient(accountId).writer(writeChannelConfiguration);
// Write data to writer
        long start = System.currentTimeMillis();
        try (OutputStream stream = Channels.newOutputStream(writer)) {
            InputStream str = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("copy_1M.csv"));
            IOUtils.copy(str, stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
// Get load job
        Job job = writer.getJob();
        job = job.waitFor();
        JobStatistics.CopyStatistics stats = job.getStatistics();
        System.out.println( "Performance: " + (System.currentTimeMillis() - start));
    }

    @Test
    @Ignore
    public void insertJsonToBigQuery() throws TimeoutException, InterruptedException {
        TableId tableId = TableId.of(bigQueryDataset, bigQueryTable);
        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId)
                        .setFormatOptions(FormatOptions.json())
                        .build();
        TableDataWriteChannel writer = bigQuery.getClient(accountId).writer(writeChannelConfiguration);

        long start = System.currentTimeMillis();
        try (OutputStream stream = Channels.newOutputStream(writer)) {
            InputStream str = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("/home/vsamofal/job/dataparse/result.json"));
            IOUtils.copy(str, stream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Job job = writer.getJob();
        job = job.waitFor();
        JobStatistics.CopyStatistics stats = job.getStatistics();
        System.out.println( "Performance: " + (System.currentTimeMillis() - start));
    }

    @Test
    public void buildJsonLinesToBigQuery() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        try (Connection connection = getLocalPostgresConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            long globalStartTime = System.currentTimeMillis();
            statement.setFetchSize(10000);
            ResultSet resultSet = statement.executeQuery(String.format("SELECT * FROM %s", TABLE_NAME));
            ResultSetMetaData md = null;

            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream("data.json"));
            while (resultSet.next()) {
                if(md == null) {
                    md = resultSet.getMetaData();
                }
                Map<String, Object> row = new HashMap<>(md.getColumnCount());
                for(int i=1; i <= md.getColumnCount(); ++i){
                    Object object = resultSet.getObject(i);
                    if(object != null) {
                        row.put(md.getColumnName(i), object);
                    }

                }

                try {
                    outputStream.write(objectMapper.writeValueAsString(row).getBytes(Charset.forName("UTF-8")));
                    outputStream.write("\n\r".getBytes(Charset.forName("UTF-8")));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
            outputStream.write("]".getBytes(Charset.forName("UTF-8")));
            log.info("Retrieve {} took {}s", 1000000, (System.currentTimeMillis() - globalStartTime) / 1000);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void buildTestCsv() throws IOException {
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream("data.csv"));
        try (Connection connection = getLocalPostgresConnection()) {

            connection.setAutoCommit(false);
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(10000);
            ResultSet resultSet = statement.executeQuery(String.format("SELECT * FROM %s", TABLE_NAME));
            ResultSetMetaData md = null;
            boolean first = true;
            while (resultSet.next()) {
                if (md == null) {
                    md = resultSet.getMetaData();
                }
                if(first) {
                    for (int i = 1; i <= md.getColumnCount(); ++i) {
                        outputStream.write(md.getColumnName(i).getBytes(Charset.forName("UTF-8")));
                        if(i != md.getColumnCount()) {
                            outputStream.write(",".getBytes(Charset.forName("UTF-8")));
                        } else {
                            outputStream.write("\n".getBytes(Charset.forName("UTF-8")));
                        }
                    }
                    first = false;
                }

                for (int i = 1; i <= md.getColumnCount(); ++i) {
                    Object obj = resultSet.getObject(i);
                    if(obj != null) {
                        outputStream.write(obj.toString().replaceAll("\n", " ").getBytes(Charset.forName("UTF-8")));
                    }
                    if(i  != md.getColumnCount()) {
                        outputStream.write(",".getBytes(Charset.forName("UTF-8")));
                    } else {
                        outputStream.write("\n".getBytes(Charset.forName("UTF-8")));
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void retrieveDataTestFromRemoveMySQL() throws InterruptedException {
        AtomicInteger rowsCount = new AtomicInteger();

        try (Connection connection = getRemoteMySQLConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            long globalStartTime = System.currentTimeMillis();
            statement.setFetchSize(Integer.MIN_VALUE);
            ResultSet resultSet = statement.executeQuery(String.format("SELECT * FROM %s", "z"));
            AtomicInteger processedRows = new AtomicInteger();
            System.out.println("Start processing data.");

            while (resultSet.next()) {
                resultSet.getObject(1);
                if(processedRows.incrementAndGet() % 10000 == 0) {
                    log.info("Processed {} rows", processedRows.get());
                }
            }
            log.info("Retrieve {} took {}s", rowsCount.get(), (System.currentTimeMillis() - globalStartTime) / 1000);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void uploadCsvToGCS() throws IOException {
        long start = System.currentTimeMillis();
        FileStorage fileStorage = storageStrategy.get(StorageType.GCS);
        String file = fileStorage.saveFile(new FileInputStream("data.csv"));
        log.info("Saving {} took {}ms", file, System.currentTimeMillis() - start);
    }

    @Test
    @Ignore
    public void uploadJsonToGCS() throws IOException {
        long start = System.currentTimeMillis();
        FileStorage fileStorage = storageStrategy.get(StorageType.GCS);
        String file = fileStorage.saveFile(new FileInputStream("data.json"));
        log.info("Saving {} took {}ms", file, System.currentTimeMillis() - start);
    }


    @Test
    @Ignore
    public void saveCsvFromGCS() {
        String fileName = "ffa3dcfb-7482-4b2c-a696-0390c6969058";
        CsvOptions options = FormatOptions.csv().toBuilder().setFieldDelimiter(",").build();
        String gcsPath = "gs://" + GcsFileStorage.getGoogleCloudStorageBucketName() + "/" + fileName;
        TableId bgTableId = TableId.of(bigQueryDataset, bigQueryTable);
        long startTime = System.currentTimeMillis();
        Job job = bigQuery.getClient(accountId).create(JobInfo.of(
                LoadJobConfiguration.newBuilder(bgTableId, gcsPath, options)
                        .setMaxBadRecords(1000)
                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                        .build()));
        waitUntilJobFinish(startTime, job);

    }

    private void waitUntilJobFinish(long startTime, Job job) {
        try {
            Retryer<Job> retryer = RetryerBuilder.<Job>newBuilder()
                    .retryIfResult(o -> !o.getStatus().getState().equals(JobStatus.State.DONE))
                    .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS))
                    .withStopStrategy(StopStrategies.neverStop())
                    .build();
            job = retryer.call(job::reload);
            log.info("Uploaded json in {}ms", System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            job.cancel();
            throw new RuntimeException(e);
        }
    }

    @Test
    @Ignore
    public void saveJSONNDFromGCS() {
        String fileName = "799ff3ac-bd6a-4719-9e32-8d3206aaed53";
        FormatOptions options = FormatOptions.json();
        TableId bgTableId = TableId.of(bigQueryDataset, bigQueryTable);

        String gcsPath = "gs://" + GcsFileStorage.getGoogleCloudStorageBucketName() + "/" + fileName;

        long startTime = System.currentTimeMillis();
        Job job = bigQuery.getClient(accountId).create(JobInfo.of(
                LoadJobConfiguration.newBuilder(bgTableId, gcsPath, options)
                        .setMaxBadRecords(1000)
                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                        .build()));

        waitUntilJobFinish(startTime, job);
    }


    @Test
    @Ignore
    public void retrieveDataTestFromLocalPostgres() throws InterruptedException {
        AtomicInteger rowsCount = new AtomicInteger();

        try (Connection connection = getLocalPostgresConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            long globalStartTime = System.currentTimeMillis();
            statement.setFetchSize(10000);
            ResultSet resultSet = statement.executeQuery(String.format("SELECT * FROM %s", TABLE_NAME));
            ResultSetMetaData md = null;
            AtomicInteger processedRows = new AtomicInteger();
            List<Map<String, Object>> rows = new ArrayList<>(1000);
            while (resultSet.next()) {
                if(md == null) {
                    md = resultSet.getMetaData();
                }
                if(rowsCount.incrementAndGet() % 10000 == 0) {
                    ArrayList<Map<String, Object>> forInsert = new ArrayList<>(rows);
                    rows.clear();
                    executorService.execute(() -> {
                        insertToBigQueryStream(forInsert);
                        log.info("Processed next {}", processedRows.addAndGet(10000));
                    });
                }
                Map<String, Object> row = new HashMap<>(md.getColumnCount());
                for(int i=1; i <= md.getColumnCount(); ++i){
                    row.put(md.getColumnName(i),resultSet.getObject(i));
                }
                rows.add(row);
            }
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            log.info("Retrieve {} took {}s", rowsCount.get(), (System.currentTimeMillis() - globalStartTime) / 1000);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
