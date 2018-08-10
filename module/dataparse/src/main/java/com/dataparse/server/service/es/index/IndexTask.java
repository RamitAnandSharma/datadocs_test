package com.dataparse.server.service.es.index;

import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.ingest.IngestFilter;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.ParserFactory;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.jdbc.Protocol;
import com.dataparse.server.service.tasks.AbstractTask;
import com.dataparse.server.service.tasks.ExceptionWrapper;
import com.dataparse.server.service.tasks.TaskState;
import com.dataparse.server.service.upload.DbQueryDescriptor;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.util.*;
import com.dataparse.server.util.thread.ThreadUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.Index;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.dataparse.server.service.flow.ingest.IngestPreprocessor.withErrorsHandling;

@Slf4j
@Data
public class IndexTask extends AbstractTask<IndexRequest> {

  private static final String ES_INDEX_BULK_SIZE = "ES_INDEX_BULK_SIZE";
  private static final String ES_INDEX_BULK_BYTE_SIZE = "ES_INDEX_BULK_BYTE_SIZE";

  private static final String ES_RE_INDEX_PAGE_SIZE = "ES_REINDEX_PAGE_SIZE";

  private static final int MAX_PROCESSION_ERRORS = 100;

  private static long getIndexBulkByteSize() {
    return SystemUtils.getProperty(ES_INDEX_BULK_BYTE_SIZE, 5 * 1024 * 1024);
  }

  @Autowired
  @JsonIgnore
  private IndexService indexService;

  @Autowired
  @JsonIgnore
  private IngestFilter ingestFilter;

  @Autowired
  @JsonIgnore
  private ParserFactory parserFactory;

  @JsonIgnore
  private List<Future<IndexResult>> childTasks = Lists.newCopyOnWriteArrayList();

  private CompletableFuture<Long> retrieveRowsCount(Parser parser, Descriptor descriptor) {
    if(descriptor instanceof DbQueryDescriptor) {
      return ThreadUtils.run(() -> parser.getRowsEstimateCount(0).getLeft());
    } else {
      return CompletableFuture.completedFuture(descriptor.getRowsCount());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<TaskState, Runnable> getStates() {
    return ImmutableMap.of(IndexTaskState.INDEXING, () -> {

      String newIndexName = indexService.createIndex(getRequest().getDescriptor());

      AtomicInteger count = new AtomicInteger(0);
      AtomicLong transformTime = new AtomicLong(0);
      Stopwatch timer = Stopwatch.createUnstarted();
      List<ErrorValue> processionErrors = new ArrayList<>(MAX_PROCESSION_ERRORS);
      AtomicInteger processionErrorsCount = new AtomicInteger(0);
      Descriptor descriptor = getRequest().getDescriptor();

      AtomicLong rowsCount = new AtomicLong(-1);
      AtomicReference<CompletableFuture<Void>> rowsEstimateCount = new AtomicReference<>();
      boolean isMySQL = descriptor instanceof DbQueryDescriptor && Protocol.mysql.equals(((DbQueryDescriptor) descriptor).getParams().getProtocol());

      try(Parser parser = parserFactory.getParser(getRequest().getDescriptor()); RecordIterator it = parser.parse()) {
        Aggregator batchAggregator = new ByteSizeAggregator(getIndexBulkByteSize(), indexRows -> {
          if(rowsEstimateCount.get() == null) {
            rowsEstimateCount.set(retrieveRowsCount(parser, descriptor).thenAccept(rowsCount::set));
          }
          List<Map<AbstractParsedColumn, Object>> batch = indexRows.stream().map(IndexRow::getRaw).collect(Collectors.toList());
          Optional<Long> processedBytes = indexRows.stream().map(IndexRow::getRawBytes).max(Long::compare);
          timer.reset().start();
          List<BulkableAction> actions = new ArrayList<>();
          for (Map<AbstractParsedColumn, Object> entry : batch) {
            Map insertObject;
            if (getRequest().getDescriptor().isUseTuplewiseTransform()) {
              insertObject = ingestFilter.filter(JSONTuplewiseTransform.transform(entry, getRequest().getDescriptor()));
            } else {
              insertObject = ingestFilter.filter(Transform.transform(entry, getRequest().getDescriptor()));
            }
            // custom provided IDs were removed in favor of auto-generated IDs to speed up ingestion
            Index index = new Index.Builder(insertObject).build();
            actions.add(index);
          }
          transformTime.addAndGet(timer.elapsed(TimeUnit.MILLISECONDS));
          log.info("Transformed in {}", timer.stop().toString());
          if(actions.size() > 0){
            childTasks.add(indexService.executeBulkAsync(newIndexName, actions, getRequest().getDescriptor().getColumns(), () -> {
              synchronized (count) {
                IndexResult result = new IndexResult();
                result.setTotal(count.addAndGet(batch.size()));
                result.setAllRowsCount(rowsCount.get());

                double percent;
                if(getRequest().getDescriptor() instanceof FileDescriptor && it.getBytesCount() > -1) {
                  percent = processedBytes.get() / (double) ((FileDescriptor) getRequest()
                      .getDescriptor())
                      .getSize() * 100.;
                } else {
                  percent = result.getTotal() / (double) rowsCount.get() * 100.;
                }
                result.setPercentComplete(percent > 100. ? 100. : Math.max(percent, 0.));
                setResult(result);
                saveState();
                log.info("Indexed {} rows", count.get());
              }
            }));
          }
        });
        try {
          it.forEachRemaining(o -> withErrorsHandling(o, getRequest().getIngestErrorMode(),
              e -> {
                if(processionErrors.size() < MAX_PROCESSION_ERRORS) {
                  processionErrors.add(e);
                }
                processionErrorsCount.incrementAndGet();
              }, (row) -> {
                if(rowsCount.get() == -1 && !isMySQL && descriptor.getRowsCount() == null && descriptor.getRowsEstimatedCount() == null) {
                  rowsEstimateCount.set(CompletableFuture.completedFuture(it.getRowsCount()).thenAccept(rowsCount::set));
                }

                boolean emptyRow = row == null || row.values().stream().filter(Objects::nonNull).collect(Collectors.toList()).isEmpty();
                if(!emptyRow) {
                  batchAggregator.push(new IndexRow(row, it.getBytesCount()));
                }
              }));
        } catch (Exception e) {
          throw ExceptionWrapper.wrap(e);
        }
        batchAggregator.flush();
        IndexResult result = IndexResult.reduceF(childTasks);
        result.setAllRowsCount(rowsCount.get());
        result.setReadTime(batchAggregator.getTotalTime());
        result.setTransformTime(transformTime.get());
        result.setIndexName(newIndexName);
        result.setPercentComplete(100.);
        result.addProcessionErrors(processionErrors);
        setResult(result);
        indexService.refreshIndex(newIndexName);
      } catch (Exception e) {
        // wait until all tasks are finished, then remove index.
        // otherwise active requests will hang for a minute
        IndexResult result = IndexResult.reduceFNonInterrupted(childTasks);
        result.setIndexName(newIndexName);
        result.addProcessionErrors(processionErrors);
        result.setSuccess(false);
        setResult(result);
        indexService.removeIndex(newIndexName);
        if(e instanceof RuntimeException){
          throw (RuntimeException) e;
        }
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void cancel() {
    // todo find a way to release semaphore after task is cancelled
    //        for(Future future: childTasks){
    //            future.cancel(true);
    //        }
  }
}
