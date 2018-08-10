package com.dataparse.server.service.export;

import com.dataparse.server.controllers.api.table.SearchIndexRequest;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.engine.EngineType;
import com.dataparse.server.service.export.event.ExportCompleteEvent;
import com.dataparse.server.service.export.event.ExportProgressEvent;
import com.dataparse.server.service.export.event.ExportStartEvent;
import com.dataparse.server.service.mail.MailService;
import com.dataparse.server.service.parser.processor.FormatProcessor;
import com.dataparse.server.service.parser.processor.Processor;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.tasks.AbstractTask;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.tasks.TaskState;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.visualization.VisualizationService;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.dataparse.server.service.zip.FileZipService;
import com.dataparse.server.util.Debounce;
import com.dataparse.server.websocket.SockJSService;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.stream.Collectors;

@Data
@Slf4j
public class ExportTask extends AbstractTask<ExportTaskRequest> {

    private static final String EXPORT_TOPIC = "/export-events";

    @Autowired
    @JsonIgnore
    private TableRepository tableRepository;

    @Autowired
    @JsonIgnore
    private ExportRepository exportRepository;

    @Autowired
    @JsonIgnore
    private VisualizationService visualizationService;

    @Autowired
    @JsonIgnore
    private MailService mailService;

    @Autowired
    @JsonIgnore
    private UserRepository userRepository;

    @Autowired
    @JsonIgnore
    private BqExporter bqExporter;

    @Autowired
    @JsonIgnore
    private CsvExporter csvExporter;

    @Autowired
    @JsonIgnore
    private XlsExporter xlsExporter;

    @Autowired
    @JsonIgnore
    private XlsWorkbookExporter xlsWorkbookExporter;

    @Autowired
    @JsonIgnore
    private SockJSService sockJSService;

    @Autowired
    @JsonIgnore
    private FileZipService fileZipService;

    @Autowired
    @JsonIgnore
    private BookmarkStateStorage bookmarkStateStorage;

    private Exporter getExporter() {
        switch (getRequest().getFormat()){
            case CSV:
                TableBookmark bookmark = tableRepository.getTableBookmark(
                        getRequest().getTableBookmarkIds().get(0));
                if(bookmark.getTableSchema().getEngineType().equals(EngineType.BIGQUERY)) {
                    if (bookmark.getState().getQueryParams().isRaw()) {
                        return bqExporter;
                    }
                }
                return csvExporter;
            case XLS:
                return xlsExporter;
            case XLS_WORKBOOK:
                return xlsWorkbookExporter;
            default:
                setFinished(true);
                throw new RuntimeException("Unknown format: " + getRequest().getFormat());
        }
    }

    @Override
    @JsonIgnore
    public Map<TaskState, Runnable> getStates() {
        return ImmutableMap.of(ExportTaskState.EXPORT, () -> {
            Exporter exporter = getExporter();
            Datadoc datadoc = tableRepository.getDatadoc(getRequest().getDatadocId());


            List<BookmarkStateId> tableBookmarkIds;
            if(getRequest().getTableBookmarkIds().isEmpty()) {
                tableBookmarkIds = tableRepository.getTableBookmarks(getRequest().getDatadocId()).stream()
                        .map(bookmark -> new BookmarkStateId(bookmark.getId(), bookmark.getCurrentState()))
                        .collect(Collectors.toList());
            } else {
                tableBookmarkIds = tableRepository.getBookmarksCurrentState(getRequest().getDatadocId(), getRequest().getTableBookmarkIds());
            }
            List<BookmarkState> states = tableBookmarkIds.stream()
                    .map(id -> bookmarkStateStorage.get(id, true).getState())
                    .collect(Collectors.toList());

            List<Exporter.ExportItemParams> exportParams = new ArrayList<>();

            if (getRequest().getParams() != null && states.size() == 1) {
                BookmarkState state = states.get(0).copy();
                state.setQueryParams(getRequest().getParams());

                TableBookmark bookmark = tableRepository.getTableBookmark(state.getTabId());
                if(bookmark.getTableSchema().getCommitted() != null) {
                    exportParams.add(getExportItemParams(state, bookmark));
                }
            } else {
                for (BookmarkState state : states) {
                    TableBookmark bookmark = tableRepository.getTableBookmark(state.getTabId());
                    if(bookmark.getTableSchema().getCommitted() != null) {
                        exportParams.add(getExportItemParams(state, bookmark));
                    }
                }
            }
            Debounce<Double> progressCallback = new Debounce<>((complete) -> {
                if(complete >= 1.){
                    complete = 1.;
                }
                sockJSService.send(getRequest().getAuth(), EXPORT_TOPIC, new ExportProgressEvent(getId(), getRequest().getDatadocId(), complete));
                saveResult(new ExportTaskResult(complete, null));
            }, 1000);

            Descriptor descriptor = exporter.export(exportParams, progressCallback, datadoc.getName());
            saveResult(new ExportTaskResult(1., descriptor));
        });
    }

    private Exporter.ExportItemParams getExportItemParams(BookmarkState state, TableBookmark bookmark) {
        SearchIndexRequest searchRequest = new SearchIndexRequest();
        searchRequest.setTableId(bookmark.getTableSchema().getId());
        searchRequest.setTableBookmarkId(bookmark.getId());
        searchRequest.setParams(state.getQueryParams());
        searchRequest.setExternalId(getRequest().getExternalId());

        List<Processor> processors = Collections.singletonList(new FormatProcessor(state));
        return new Exporter.ExportItemParams(bookmark.getName(), state, searchRequest,
                state.getQueryParams().getLimit().getRawDataExport(),
                processors);
    }

    @Override
    public void onBeforeStart(TaskInfo info) {
        sockJSService.send(getRequest().getAuth(), EXPORT_TOPIC,
                           new ExportStartEvent(getId(),
                                                getRequest().getDatadocId(),
                                                info.getStatistics().getRequestReceivedTime(),
                                                getRequest().getFormat()));
    }

    @Override
    public void onAfterFinish(TaskInfo info) {
        String email = userRepository.getUser(getRequest().getAuth().getUserId()).getEmail();
        Datadoc datadoc = tableRepository.getDatadoc(getRequest().getDatadocId());
        String filePath = ((FileDescriptor) ((ExportTaskResult) getResult()).getDescriptor()).getPath();
        exportRepository.save(new ExportEntity(UUID.fromString(filePath), new Date(), datadoc, email));
        sockJSService.send(getRequest().getAuth(), EXPORT_TOPIC,
                           new ExportCompleteEvent(getId(),
                                                   getRequest().getDatadocId(),
                                                   info.getStatistics().getRequestReceivedTime(),
                                                   info.getStatistics().getRequestCompleteTime()));
    }

    @Override
    public void cancel() {

    }
}
