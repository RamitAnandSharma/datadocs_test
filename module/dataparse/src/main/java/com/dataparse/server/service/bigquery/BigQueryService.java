package com.dataparse.server.service.bigquery;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.bigquery.load.LoadRequest;
import com.dataparse.server.service.bigquery.load.LoadResult;
import com.dataparse.server.service.bigquery.sync.SyncRequest;
import com.dataparse.server.service.bigquery.sync.SyncResult;
import com.dataparse.server.service.docs.BookmarkStatistics;
import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.storage.GcsFileStorage;
import com.dataparse.server.service.storage.IStorageStrategy;
import com.dataparse.server.service.storage.StorageType;
import com.dataparse.server.service.tasks.TaskManagementService;
import com.dataparse.server.service.upload.CsvFileDescriptor;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.visualization.bookmark_state.state.IngestErrorMode;
import com.dataparse.server.util.SystemUtils;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.cloud.bigquery.*;
import com.google.common.util.concurrent.AtomicDouble;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BigQueryService {

    private final static String BQ_INDEX_THREADS = "BQ_INDEX_THREADS";

    public final static String ORIGIN_TABLE = "ORIGIN";

    @Autowired
    private BigQueryClient bigQuery;

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private TaskManagementService taskManagementService;

    @Autowired
    private IStorageStrategy storageStrategy;


    private ExecutorService taskExecutor;

    private Semaphore taskExecutorSemaphore;

    @PostConstruct
    public void init(){
        int executorSize = SystemUtils.getProperty(BQ_INDEX_THREADS, Runtime.getRuntime().availableProcessors());
        log.info("Initialized with executor size: " + executorSize);
        taskExecutor = Executors.newFixedThreadPool(executorSize);
        taskExecutorSemaphore = new Semaphore(executorSize * 10);

    }

    @PreDestroy
    protected void destroy() {
        taskExecutor.shutdownNow();
    }

    public Future<SyncResult> insertBatchAsync(String accountId, String externalId, List<Map<String, Object>> batch){
        try {
            taskExecutorSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return taskExecutor.submit(() -> {
            try {
                return insertBatch(accountId, externalId, batch);
            } finally {
                taskExecutorSemaphore.release();
            }
        });
    }

    public SyncResult insertBatch(String accountId, String externalId, List<Map<String, Object>> batch) {
        List<InsertAllRequest.RowToInsert> rows = batch.stream().map(InsertAllRequest.RowToInsert::of).collect(Collectors.toList());
        InsertAllRequest request = InsertAllRequest.of(externalId, ORIGIN_TABLE, rows);
        bigQuery.getClient(accountId).insertAll(request);
        SyncResult result = new SyncResult();
        result.setTotal(batch.size());
        return result;
    }

    private TableId createTableId(String datasetId){
        return TableId.of(datasetId, ORIGIN_TABLE);
    }

    public String createTable(String accountId, Descriptor descriptor){

        String externalId = UUID.randomUUID().toString().replaceAll("-", "");
        TableId tableId = TableId.of(externalId, ORIGIN_TABLE);

        descriptor.getColumns().sort(Comparator.comparing(c -> c.getSettings().getIndex()));
        List<Field> fields = descriptor.getColumns()
                .stream()
                .filter(column -> !column.getSettings().isRemoved())
                .map(column -> {
                    Field.Type type;
                    TypeDescriptor typeDescriptor = column.getType();
                    if(column.getSettings().getType() != null){
                        typeDescriptor = column.getSettings().getType();
                    }
                    switch(typeDescriptor.getDataType()){
                        case DECIMAL:
                            type = Field.Type.floatingPoint();
                            break;
                        case TIME:
                        case DATE:
                            type = Field.Type.timestamp();
                            break;
                        case STRING:
                            type = Field.Type.string();
                            break;
                        default:
                            throw new RuntimeException("Can't map to BigQuery type: " + column.getType().getDataType());
                    }
                    Field.Builder builder = Field.newBuilder(column.getAlias(), type);
                    if(column.isRepeated()){
                        builder.setMode(Field.Mode.REPEATED);
                    }

                    log.info("Ingested column: [{} - {}]", column.getName(), column.getType().getDataType());
                    return builder.build();
                }).collect(Collectors.toList());

        Schema schema = Schema.of(fields);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);

        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        Dataset dataset = bigQuery.getClient(accountId).create(DatasetInfo.of(externalId));
        log.info("Created dataset: " + dataset.getDatasetId());

        Table table = bigQuery.getClient(accountId).create(tableInfo);
        log.info("Created table: " + table.getTableId().getDataset());
        return externalId;
    }

    public boolean deleteDataset(String accountId, String datasetId){
        boolean deleted = bigQuery.getClient(accountId).delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
        if (deleted) {
            log.info("Removed dataset: " + datasetId);
        } else {
            log.warn("Dataset not found: " + datasetId);
        }
        return deleted;
    }

    private LoadResult getResult(JobStatistics stats){
        LoadResult result = new LoadResult();
        if(stats instanceof JobStatistics.LoadStatistics) {
            JobStatistics.LoadStatistics loadStats = (JobStatistics.LoadStatistics) stats;
            result.setTotal(loadStats.getOutputRows());
            result.setPercentComplete(loadStats.getOutputBytes() / (double) loadStats.getInputBytes() * 100);
            result.setAllRowsCount(loadStats.getInputBytes());
        }
        return result;
    }

    public LoadResult doLoad(String accountId, String datasetId, Descriptor descriptor, Consumer<LoadResult> progressConsumer){
        FormatOptions options;
        switch (descriptor.getFormat()){
            case CSV:
                CsvOptions.Builder builder = CsvOptions.newBuilder();
                CsvFileDescriptor csvFileDescriptor = (CsvFileDescriptor) descriptor;
                int skip = 0;
                if(csvFileDescriptor.getSettings().getUseHeaders()) {
                    skip++;
                }
                if(csvFileDescriptor.getSettings().getSkipAfterHeader() != null) {
                    skip += csvFileDescriptor.getSettings().getSkipAfterHeader();
                }
                // todo  configuration.load.nullMarker?
                builder.setSkipLeadingRows(skip);
                builder.setAllowQuotedNewLines(true);
                if(csvFileDescriptor.getSettings().getQuote() != null) {
                    builder.setQuote(String.valueOf(csvFileDescriptor.getSettings().getQuote()));
                }
                builder.setFieldDelimiter(String.valueOf(csvFileDescriptor.getSettings().getDelimiter()));
                options = builder.build();
                break;
            case AVRO:
                options = FormatOptions.avro();
                break;
            case JSON_LINES:
                options = FormatOptions.json();
                break;
            default:
                throw new IllegalStateException("Can't import file of format: " + descriptor.getFormat());
        }
        FileDescriptor fileDescriptor = (FileDescriptor) descriptor;
        FileDescriptor tmpFileDescriptor = null;
        String gcsPath;
        if(fileDescriptor.getStorage().equals(StorageType.GCS)) {
            gcsPath = "gs://" + GcsFileStorage.getGoogleCloudStorageBucketName() + "/" + fileDescriptor.getPath();
        } else {
            InputStream is = storageStrategy.get(fileDescriptor).getFile(fileDescriptor.getPath());
            long start = System.currentTimeMillis();
            try {
                log.info("Uploading data source to GCS...");
                tmpFileDescriptor = FileDescriptor.gcs();
                String tmpFileName = storageStrategy.get(tmpFileDescriptor).saveFile(is);
                tmpFileDescriptor.setPath(tmpFileName);
                gcsPath = "gs://" + GcsFileStorage.getGoogleCloudStorageBucketName() + "/" + tmpFileName;
                log.info("Uploaded file to GCS in {}", (System.currentTimeMillis() - start));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        long start = System.currentTimeMillis();
        TableId bqTableId = TableId.of(datasetId, ORIGIN_TABLE);
        double approxLoadDurationMs;
        double x = descriptor.getRowsCount() / 1E6;
        // polynomial based on empirical data: BQ imported 1M per 23s, 10M rows per 2.5min, 1B per 10m
        approxLoadDurationMs = (0.00012447913682481583716151617386185 * x * x * x
                                -0.13951864322234692605062975433346 * x * x
                                + 15.63199900236937273974311011348 * x
                                + 7.5073951617161493704703581246791) * 1000;
        log.info("Approximate load duration: {}ms", approxLoadDurationMs);
        log.info("Importing data into BigQuery table...");
        Job job = bigQuery.getClient(accountId).create(JobInfo.of(
                LoadJobConfiguration.newBuilder(bqTableId, gcsPath, options)
                        .setMaxBadRecords(1000)
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_EMPTY)
                        .build()));
        AtomicDouble lastPercentComplete = new AtomicDouble(-5);
        try {
            Retryer<Job> retryer = RetryerBuilder.<Job>newBuilder()
                    .retryIfResult(o -> {
                        // for some reason no load statistics comes from BQ until file is completely loaded
                        if(o.getStatistics() instanceof JobStatistics.LoadStatistics) {
                            JobStatistics.LoadStatistics loadStats = o.getStatistics();
                            LoadResult result = getResult(loadStats);
                            if(result.getPercentComplete() - lastPercentComplete.get() > 5) {
                                lastPercentComplete.set(result.getPercentComplete());
                                log.info("Loaded {} rows", loadStats.getOutputRows());
                            }
                            progressConsumer.accept(result);
                        } else {
                            // since upload result progress doesn't really work now in BQ, we should calculate approximate progress
                            LoadResult approxResult = new LoadResult();
                            long currentDurationMs = System.currentTimeMillis() - start;
                            double percentComplete = currentDurationMs / approxLoadDurationMs * 100.;
                            if(percentComplete > 100.){
                                percentComplete = 100.;
                            }
                            approxResult.setPercentComplete(percentComplete);
                            progressConsumer.accept(approxResult);
                        }
                        return !o.getStatus().getState().equals(JobStatus.State.DONE);
                    })
                    .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS))
                    .withStopStrategy(StopStrategies.neverStop())
                    .build();
            job = retryer.call(job::reload);
        } catch (Exception e) {
            job.cancel();
            throw new RuntimeException(e);
        } finally {
            if(tmpFileDescriptor != null) {
                storageStrategy.get(tmpFileDescriptor).removeFile(tmpFileDescriptor.getPath());
            }
        }
        LoadResult result = getResult(job.getStatistics());
        result.setAccountId(accountId);
        result.setTableName(datasetId);
        result.setSuccess(job.getStatus().getError() == null);
        if(job.getStatus().getExecutionErrors() != null) {
            List<ErrorValue> errors = job.getStatus().
                    getExecutionErrors().
                    stream().
                    map(err -> new ErrorValue(err.getMessage())).
                    collect(Collectors.toList());
            result.addProcessionErrors(errors);
        }
        if(result.isSuccess()){
            log.info("Imported BigQuery table in {}", (System.currentTimeMillis() - start));
        }
        return result;
    }

    public String load(Auth auth, String accountId, Long bookmarkId, Descriptor descriptor) {
        TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
        if(bookmark == null) {
            throw new RuntimeException("Bookmark not found");
        }
        LoadRequest task = new LoadRequest(bookmarkId, accountId, descriptor);
        task.setParentTask(MDC.get("request"));
        return taskManagementService.execute(auth, task);
    }

    public String sync(Auth auth, String accountId, Long bookmarkId, Descriptor descriptor, IngestErrorMode ingestErrorMode) {
        TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
        if (bookmark == null) {
            throw new RuntimeException("Bookmark not found");
        }
        SyncRequest task = new SyncRequest(bookmarkId, accountId, descriptor, ingestErrorMode);
        task.setParentTask(MDC.get("request"));
        return taskManagementService.execute(auth, task);
    }

    public long getStorageSpace(String accountId, String datasetId){
        Table table = bigQuery.getClient(accountId).getTable(createTableId(datasetId));
        StandardTableDefinition definition = table.getDefinition();
        long numBytes = definition.getNumBytes();
        if(definition.getStreamingBuffer() != null){
            numBytes += definition.getStreamingBuffer().getEstimatedBytes();
        }
        return numBytes;
    }

    public void retrieveStorageStatistics(String accountId, String datasetId, BookmarkStatistics statistics){
        Table table = bigQuery.getClient(accountId).getTable(createTableId(datasetId));
        StandardTableDefinition definition = table.getDefinition();
        statistics.setStorageSpace(definition.getNumBytes());
        statistics.setRows(definition.getNumRows());
        if(definition.getStreamingBuffer() != null){
            statistics.setStorageSpace(definition.getStreamingBuffer().getEstimatedBytes());
            statistics.setRows(definition.getStreamingBuffer().getEstimatedRows());
        }
    }

}
