package com.dataparse.server.service.export;

import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.service.bigquery.*;
import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.storage.*;
import com.dataparse.server.service.upload.*;

import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.visualization.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.github.rholder.retry.*;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.*;
import org.apache.commons.io.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

@Slf4j
@Service
public class BqExporter extends Exporter {

    @Autowired
    private VisualizationService visualizationService;

    @Autowired
    private IStorageStrategy storageStrategy;

    @Autowired
    private BigQueryClient bigQuery;

    @Autowired
    private TableRepository tableRepository;

    @Override
    public Descriptor export(final List<ExportItemParams> exportItems, final Consumer<Double> progressCallback, String datadocName) {
        if (exportItems.size() > 1) {
            throw new RuntimeException("Only one bookmark at time can be exported");
        }
        ExportItemParams exportItem = exportItems.get(0);
        TableBookmark bookmark = tableRepository.getTableBookmark(exportItem.getRequest().getTableBookmarkId());
        String[] id = exportItem.getRequest().getExternalId().split(":");
        TableId tableId = TableId.of(id[0], id[1]);
        Table table = bigQuery.getClient(bookmark.getTableSchema().getAccountId()).getTable(tableId);
        if(table == null){
            SearchIndexResponse response = visualizationService.search(exportItem.getRequest());
            id = response.getExternalId().split(":");
            tableId = TableId.of(id[0], id[1]);
        }
        String filename = UUID.randomUUID().toString().replaceAll("-", "");

        ExtractJobConfiguration configuration = ExtractJobConfiguration
                .newBuilder(tableId, "gs://datadocs/" + filename + "-*")
                .setPrintHeader(false)
                .build();
        long start = System.currentTimeMillis();
        Job job = bigQuery.getClient(bookmark.getTableSchema().getAccountId()).create(JobInfo.of(configuration));
        long files;
        long initialDelay = 45 * 1000;
        double rowsPerMs = 1; // empirical coefficient
        double approxLoadDurationMs = exportItem.getRequest().getParams().getLimit().getRawData() / rowsPerMs + initialDelay;
        try {
            Retryer<Job> retryer = RetryerBuilder.<Job>newBuilder()
                    .retryIfResult(o -> {
                        long currentDurationMs = System.currentTimeMillis() - start;
                        double percentComplete = currentDurationMs / approxLoadDurationMs;
                        if(percentComplete > 1.){
                            percentComplete = 1.;
                        }
                        progressCallback.accept(percentComplete);
                        return !o.getStatus().getState().equals(JobStatus.State.DONE);
                    })
                    .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS))
                    .withStopStrategy(StopStrategies.neverStop())
                    .build();
            job = retryer.call(job::reload);

            if (job.getStatus().getError() == null) {
                // Job completed successfully
                JobStatistics.ExtractStatistics stats = job.getStatistics();
                log.info("Exported in {} parts", stats.getDestinationUriFileCounts());
                files = stats.getDestinationUriFileCounts().get(0);
            } else {
                throw new RuntimeException(job.getStatus().getError().getMessage());
            }

        } catch (Exception e) {
            job.cancel();
            throw new RuntimeException(e);
        }
        // create composite descriptor that contains single file with CSV headers
        // and many files with CSV contents (result of BQ export)
        CompositeDescriptor compositeDescriptor = new CompositeDescriptor();
        compositeDescriptor.setDescriptors(new ArrayList<>());

        FileDescriptor headerFileDescriptor = new FileDescriptor();
        headerFileDescriptor.setStorage(StorageType.GCS);
        headerFileDescriptor.setContentType("text/csv");
        headerFileDescriptor.setPath(filename + "-header");
        String headerString = getHeaderString(bookmark.getState());
        try {
            String headerFilePath = storageStrategy.get(headerFileDescriptor).saveFile(IOUtils.toInputStream(headerString));
            headerFileDescriptor.setPath(headerFilePath);
        } catch (IOException e) {
            throw new RuntimeException("Can't save headers file", e);
        }
        compositeDescriptor.getDescriptors().add(headerFileDescriptor);
        for (int i = 0; i < files; i++) {
            FileDescriptor descriptor = new FileDescriptor();
            descriptor.setStorage(StorageType.GCS);
            descriptor.setContentType("text/csv");
            descriptor.setPath(getGcsExportFileName(filename, i));
            compositeDescriptor.getDescriptors().add(descriptor);
        }
        return compositeDescriptor;
    }

    private String getHeaderString(BookmarkState state) {
        Map<String, String> columnNames = state
                .getColumnList()
                .stream()
                .collect(Collectors.toMap(c -> c.getField(), c -> c.getName()));
        return state
                .getQueryParams()
                .getShows()
                .stream()
                .map(s -> columnNames.get(s.getField()))
                .collect(Collectors.joining(",")) + "\n";
    }

    private String getGcsExportFileName(String name, int i) {
        String numberStr = String.valueOf(i);
        while (numberStr.length() < 12) {
            numberStr = "0" + numberStr;
        }
        return name + "-" + numberStr;
    }

}
