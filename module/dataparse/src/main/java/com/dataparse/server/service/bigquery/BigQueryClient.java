package com.dataparse.server.service.bigquery;

import avro.shaded.com.google.common.collect.*;
import com.dataparse.server.auth.*;
import com.dataparse.server.config.*;
import com.dataparse.server.service.bigquery.cache.*;
import com.dataparse.server.service.bigquery.cache.serialization.*;
import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.schema.log.*;
import com.dataparse.server.service.hazelcast.*;
import com.google.auth.oauth2.*;
import com.google.cloud.bigquery.*;
import com.google.common.cache.*;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import javax.annotation.*;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Service
public class BigQueryClient {

    public final static String QUERY_LIMIT_SEMAPHORE_PREFIX = "BQ_LIMIT_SEMAPHORE_";
    public final static int MAX_CONCURRENT_QUERIES = 50;

    @Autowired
    private IntelliCache queryCache;

    @Autowired
    private HazelcastClient hazelcast;

    @Autowired
    private BookmarkActionLogService requestLogService;

    private Cache<String, JobId> recentQueries = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    private List<String> accounts;
    private Map<String, BigQuery> clients;
    private Map<String, ISemaphore> semaphores;

    @PostConstruct
    public void init() {
        Map<String, GoogleCredentials> credentials = AppConfig.getGoogleAppCredentials();

        semaphores = new HashMap<>();
        for(String accountId : credentials.keySet()) {
            SemaphoreConfig querySemaphoreConfig = new SemaphoreConfig();
            querySemaphoreConfig.setName(BigQueryClient.QUERY_LIMIT_SEMAPHORE_PREFIX + accountId);
            querySemaphoreConfig.setInitialPermits(BigQueryClient.MAX_CONCURRENT_QUERIES);
            hazelcast.getClient().getConfig().addSemaphoreConfig(querySemaphoreConfig);
            semaphores.put(accountId, hazelcast.getClient().getSemaphore(QUERY_LIMIT_SEMAPHORE_PREFIX + accountId));
        }

        clients = Maps.transformEntries(credentials, (projectId, cred) ->
                BigQueryOptions.newBuilder()
                        .setCredentials(cred)
                        .setProjectId(projectId)
                        .build()
                        .getService());

        accounts = ImmutableList.copyOf(clients.keySet());
    }

    public List<String> getAvailableAccounts(){
        return accounts;
    }

    public BigQuery getClient(String accountId) {
        BigQuery bigQuery = clients.get(accountId);
        if(bigQuery == null){
            throw new RuntimeException("Account " + accountId + " is not initialized");
        }
        return bigQuery;
    }

    public BigQueryResult querySync(BigQueryRequest request) {
        try {
            return query(request).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Can not execute query. ", e);
        }
    }

    public Future<? extends BigQueryResult> query(BigQueryRequest request){
        ExecutorService executor = Executors.newSingleThreadExecutor();
        return executor.submit(() -> {
            BigQuery bigquery = getClient(request.getRequest().getAccountId());
            ISemaphore querySemaphore = semaphores.get(request.getRequest().getAccountId());
            QueryKey key = QueryKey.of(request);

            RequestLogEntry logEntry = RequestLogEntry.of(request.getRequest());
            logEntry.setUserId(Auth.get().getUserId());
            logEntry.setBillableUserId(Auth.get().getUserId());
            logEntry.setStorageType(EngineType.BIGQUERY);
            logEntry.setQuery(request.getQuery());
            // check for results in hazelcast cache
            if (AppConfig.getBqEnableQueryCache()) {
                long start = System.nanoTime();
                BigQueryResult cachedResponse = queryCache.get(key);
                if (cachedResponse != null) {
                    logEntry.setSuccess(true);
                    logEntry.setFromCache(true);
                    logEntry.setBytesProcessed(0);
                    logEntry.setDuration((long) ((System.nanoTime() - start) / 1E6));
                    requestLogService.save(logEntry);
                    log.info("Cached results found for query {}", key);
                    cachedResponse.setRequest(request);
                    return cachedResponse;
                }
            }
            try {
                querySemaphore.acquire();
                log.info("Requesting page number {} of size {}", request.getPageNumber(), request.getPageSize());
                log.info("Query {}: \n{}", key, request.getQuery());
            } catch (InterruptedException e) {
                throw new RuntimeException("Can't acquire permit for new query", e);
            }

            try {
                QueryRequest req = QueryRequest.newBuilder(request.getQuery())
                        .setPageSize(request.getPageSize())
                        .setUseQueryCache(true)
                        .setUseLegacySql(false)
                        .setMaxWaitTime(10000L)
                        .build();
                long start = System.nanoTime();

                JobId jobId = recentQueries.getIfPresent(request.getRequestUID());
                QueryResponse response;
                TableId tableId = null;
                if(jobId == null) {
                    response = bigquery.query(req);
                    // request might not finish and we have to wait
                    while (!response.jobCompleted()) {
                        response = bigquery.getQueryResults(response.getJobId());
                    }

                    Job job = bigquery.getJob(response.getJobId());
                    QueryJobConfiguration config = job.getConfiguration();
                    tableId = config.getDestinationTable();

                    logEntry.setSuccess(!response.hasErrors());
                    logEntry.setBytesProcessed(response.getResult().getTotalBytesProcessed());
                    logEntry.setDuration((long) ((System.nanoTime() - start) / 1E6));
                    logEntry.setFromCache(response.getResult().cacheHit());
                    // if we request specific page and previous pages were taken from cache,
                    // we should read from BigQuery table with offset
                    if(request.getPageNumber() != null && request.getPageNumber() > 0){
                        response = bigquery.getQueryResults(response.getJobId(),
                                BigQuery.QueryResultsOption.startIndex(request.getPageNumber() * request.getPageSize()),
                                BigQuery.QueryResultsOption.pageSize(request.getPageSize()));
                    }
                } else {
                    response = bigquery.getQueryResults(jobId,
                            BigQuery.QueryResultsOption.startIndex(request.getPageNumber() * request.getPageSize()),
                            BigQuery.QueryResultsOption.pageSize(request.getPageSize()));
                }
                log.info("Query {} executed in {} ms", key, ((System.nanoTime() - start) / 1E6));
                BigQueryResult result;
                if(request.getPageSize() != null) {
                    recentQueries.put(request.getRequestUID(), response.getJobId());
                    result = BigQueryRawResult.ofSinglePage(response.getResult());
                } else {
                    start = System.nanoTime();
                    log.info("Loading all pages for query {}...", key);
                    result = BigQueryRawResult.ofAllPages(response.getResult());
                    log.info("Loaded all pages for query {} in {} ms", key, ((System.nanoTime() - start) / 1E6));
                }
                if(tableId != null) {
                    result.setTableId(tableId.getDataset() + ":" + tableId.getTable());
                }
                // we have to set threshold for response size, otherwise hazelcast would blow out on large aggregations
                if(AppConfig.getBqEnableQueryCache()) {
                    queryCache.put(key, result);
                }
                result.setRequest(request);
                return result;
            } catch (Exception e) {
                logEntry.setSuccess(false);
                throw e;
            } finally {
                requestLogService.save(logEntry);
                querySemaphore.release();
            }
        });
    }
}
