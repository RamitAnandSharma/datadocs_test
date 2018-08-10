package com.dataparse.server.service.bigquery.sync;

import com.dataparse.server.service.bigquery.*;
import com.dataparse.server.service.es.index.IndexRow;
import com.dataparse.server.service.flow.*;
import com.dataparse.server.service.flow.ingest.*;
import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.iterator.*;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.tasks.AbstractTask;
import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.util.*;
import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;
import lombok.extern.slf4j.*;
import org.springframework.beans.factory.annotation.*;

import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

@Slf4j
public class SyncTask extends AbstractTask<SyncRequest> {

    private static final int BIG_QUERY_STREAMING_INSERTS_BATCH_SIZE = 2000;
    private static final int MAX_PROCESSION_ERRORS = 100;

    @Autowired
    private BigQueryService bigQueryService;

    @Autowired
    private ParserFactory parserFactory;

    @JsonIgnore
    private List<Future<SyncResult>> childTasks = Lists.newCopyOnWriteArrayList();

    private List<Map<String, Object>> withAliases(List<Map<AbstractParsedColumn, Object>> batch, List<ColumnInfo> columns){
        return batch.stream()
                .map(o -> {
                    Map<String, Object> copy = new HashMap<>(o.size());
                    for(ColumnInfo column: columns){
                        DataType dataType = column.getSettings().getType().getDataType();
                        if(dataType == null){
                            dataType = column.getType().getDataType();
                        }
                        DataType finalType = dataType;
                        Object value = o.get(ParsedColumnFactory.getByColumnInfo(column));
                        if(value != null) {
                            copy.put(column.getAlias(), ListUtils.applyIfNotNull(value, o1 -> {
                                if(finalType.equals(DataType.DATE)){
                                    return ((Date) o1).getTime() / 1000;
                                } else if(finalType.equals(DataType.TIME)){
                                    return ((Time) o1).getTime() / 1000;
                                }
                                return o1;
                            }));
                        }
                    }
                    return copy;
                })
                .collect(Collectors.toList());
    }

    private Double getAllBytesCount() {
        Descriptor descriptor = getRequest()
                .getDescriptor();
        if(descriptor instanceof FileDescriptor) {
            return (double) ((FileDescriptor) descriptor)
                    .getSize() * 100.;
        } else {
            return 1.;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<TaskState, Runnable> getStates() {
        return ImmutableMap.of(SyncTaskState.SYNC, () -> {

            String newTableName = bigQueryService.createTable(getRequest().getAccountId(), getRequest().getDescriptor());
            AtomicInteger count = new AtomicInteger(0);
            List<ErrorValue> processionErrors = new ArrayList<>(MAX_PROCESSION_ERRORS);
            AtomicInteger processionErrorsCount = new AtomicInteger(0);
            try(RecordIterator it = parserFactory.getParser(getRequest().getDescriptor()).parse()) {
                Aggregator batchAggregator = new BatchAggregator(BIG_QUERY_STREAMING_INSERTS_BATCH_SIZE, indexRows -> {
                    List<Map<AbstractParsedColumn, Object>> batch = indexRows.stream().map(IndexRow::getRaw).collect(Collectors.toList());

                    List<Map<String, Object>> aliased = withAliases(batch, getRequest().getDescriptor().getColumns());
                    childTasks.add(bigQueryService.insertBatchAsync(getRequest().getAccountId(), newTableName, aliased));

                    if(Thread.currentThread().isInterrupted()){
                        log.info("Sync Task interrupted, deleting dataset {}", newTableName);
                        bigQueryService.deleteDataset(getRequest().getAccountId(), newTableName);
                        return;
                    }

                    SyncResult result = new SyncResult();
                    result.setAccountId(newTableName);
                    result.setAccountId(getRequest().getAccountId());
                    result.setTotal(count.addAndGet(batch.size()));
                    result.setAllRowsCount(getRequest()
                            .getDescriptor()
                            .getRowsCount());
                    double percent;
                    if(getRequest().getDescriptor() instanceof FileDescriptor && it.getBytesCount() > -1) {
                        percent = it.getBytesCount() / getAllBytesCount();
                    } else {
                        percent = result.getTotal() / (double) getRequest()
                                .getDescriptor()
                                .getRowsCount() * 100.;
                    }
                    result.setPercentComplete(percent > 100. ? 100. : percent);
                    setResult(result);
                    saveState();
                    log.info("Indexed {} rows", count.get());
                });

                it.forEachRemaining(o -> IngestPreprocessor.withErrorsHandling(o, getRequest().getIngestErrorMode(),
                                                                               e -> {
                                                                                   if(processionErrors.size() < MAX_PROCESSION_ERRORS) {
                                                                                       processionErrors.add(e);
                                                                                   }
                                                                                   processionErrorsCount.incrementAndGet();
                                                                               }, (row) -> batchAggregator.push(new IndexRow(row, it.getBytesCount()))));
                batchAggregator.flush();
                SyncResult result = SyncResult.reduceF(childTasks);
                result.setAccountId(getRequest().getAccountId());
                result.setTableName(newTableName);
                result.setPercentComplete(100.);
                result.addProcessionErrors(processionErrors);
                setResult(result);
            } catch (Exception e) {
                // wait until all tasks are finished, then remove index.
                // otherwise active requests will hang for a minute
                SyncResult result = SyncResult.reduceFNonInterrupted(childTasks);
                result.setSuccess(false);
                result.setAccountId(getRequest().getAccountId());
                result.setTableName(newTableName);
                result.addProcessionErrors(processionErrors);
                result.setSuccess(false);
                setResult(result);
                bigQueryService.deleteDataset(getRequest().getAccountId(), newTableName);
                throw new RuntimeException(e);
            }

        });
    }

    @Override
    public void cancel() {
        for(Future future: childTasks){
            future.cancel(true);
        }
    }
}
