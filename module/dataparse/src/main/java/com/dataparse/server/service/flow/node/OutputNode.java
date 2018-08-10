package com.dataparse.server.service.flow.node;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.flow.PreviewRequest;
import com.dataparse.server.service.bigquery.BigQueryService;
import com.dataparse.server.service.bigquery.account.IAccountSelectionStrategy;
import com.dataparse.server.service.bigquery.load.LoadResult;
import com.dataparse.server.service.bookmark.BookmarkStateService;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.engine.EngineSelectionStrategy;
import com.dataparse.server.service.engine.EngineType;
import com.dataparse.server.service.es.index.IndexResult;
import com.dataparse.server.service.es.index.IndexService;
import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.flow.FlowExecutor;
import com.dataparse.server.service.flow.FlowService;
import com.dataparse.server.service.flow.convert.ConvertService;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.schema.log.BookmarkActionLogService;
import com.dataparse.server.service.schema.log.DataIngestedLogEntry;
import com.dataparse.server.service.storage.StorageType;
import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkStateRepository;
import com.dataparse.server.util.BeanUtils;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class OutputNode extends SideEffectNode<OutputNodeSettings> {

    @Autowired
    BookmarkStateRepository bookmarkStateRepository;

    @Autowired
    private BookmarkStateService bookmarkStateService;

    @Autowired
    private IndexService indexService;

    @Autowired
    private BigQueryService bigQueryService;

    @Autowired
    private TaskManagementService taskManagementService;

    @Autowired
    private FlowExecutor flowExecutor;

    @Autowired
    private TasksRepository tasksRepository;

    @Autowired
    private IAccountSelectionStrategy accountSelectionStrategy;

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private TableService tableService;

    @Autowired
    private FlowService flowService;

    @Autowired
    private BookmarkActionLogService actionLogService;

    @Autowired
    private ConvertService convertService;

    private IngestTaskResult taskResult;

    public OutputNode(String id) {
        super(id);
    }

    private void createColumnAliases(Descriptor descriptor) {
        int i = 0;
        for (ColumnInfo column : descriptor.getColumns()) {
            column.setAlias("c" + i++);
        }
    }

    private void sortColumns(Descriptor descriptor) {
        descriptor.setColumns(descriptor.getColumns()
                .stream().sorted(Comparator.comparing(column -> column.getSettings().getIndex() == null ? 0 : column.getSettings().getIndex()))
                .collect(Collectors.toList()));
    }

    private EngineType engineType;
    private String externalId, accountId = null;
    private Long startTime;

    private LoadResult loadFileFromGCSIntoBigQuery(TableBookmark bookmark, Descriptor descriptor, Consumer<NodeState> nodeStateConsumer){
        sortColumns(descriptor);
        createColumnAliases(descriptor);
        Stopwatch timer = Stopwatch.createStarted();

        // prepare data source before loading it into BQ:
        // if data source is not in CSV format then convert to CSV
        // else if CSV format is not supported by BQ then convert to supported CSV format
        Descriptor convertedSource = convertService.convert(descriptor, StorageType.GCS, progress -> {});

        String taskId = bigQueryService.load(Auth.get(), accountId, getSettings().getBookmarkId(), convertedSource);
        DataIngestedLogEntry logEntry = DataIngestedLogEntry.of(bookmark);
        logEntry.setIngestTaskId(taskId);
        logEntry.setStartTime(new Date());
        try {
            TaskInfo taskInfo = taskManagementService.check(taskId).fastFail().retry(info -> {
                LoadResult result = (LoadResult) info.getResult();
                if (result != null) {
                    getState().setPercentComplete(result.getPercentComplete());
                    getState().setProcessedRowsCount(result.getTotal());
                    getState().setAllRowsCount(result.getAllRowsCount() == -1 ? result.getTotal() : result.getAllRowsCount());
                    nodeStateConsumer.accept(getState());
                }
            });
            LoadResult result = (LoadResult) taskInfo.getResult();
            externalId = result.getTableName();
            accountId = result.getAccountId();
            result.getProcessionErrors().stream().map(ErrorValue::getDescription).forEach(log::warn);
            if(!result.isSuccess()){
                throw ExecutionException.of("stop_on_error", result.getProcessionErrors().get(0).getDescription());
            }

            logEntry.setSuccess(result.isSuccess());
            logEntry.setDuration(timer.stop().elapsed(TimeUnit.MILLISECONDS));
            logEntry.setUserId(taskInfo.getRequest().getAuth().getUserId());
            logEntry.setBillableUserId(Auth.get().getUserId());
            logEntry.setRowsIngested(result.getTotal());
            logEntry.setStorageType(EngineType.BIGQUERY);
            logEntry.setExternalId(externalId);
            logEntry.setAccountId(accountId);
            logEntry.setSize(bigQueryService.getStorageSpace(accountId, externalId));
            log.info("Loaded " + result.getTotal() + " rows (" + result.getProcessionErrors().size() + " errors) in " + timer.toString());
            return result;
        } finally {
            actionLogService.save(logEntry);
        }
    }

    private IndexResult indexWithElasticSearch(TableBookmark bookmark, Descriptor descriptor, Consumer<NodeState> nodeStateConsumer){
        sortColumns(descriptor);
        createColumnAliases(descriptor);
        Stopwatch timer = Stopwatch.createStarted();
        String taskId = indexService.indexAsync(Auth.get(),
                                                getSettings().getBookmarkId(),
                                                true,
                                                getSettings().isPreserveHistory(),
                                                getFlow().getSettings().getIngestErrorMode(),
                                                descriptor);
        DataIngestedLogEntry logEntry = DataIngestedLogEntry.of(bookmark);
        logEntry.setIngestTaskId(taskId);
        logEntry.setStartTime(new Date());
        try {
            TaskInfo taskInfo = taskManagementService.check(taskId).fastFail().retry(info -> {
                IndexResult result = (IndexResult) info.getResult();
                if(result != null) {
                    getState().setPercentComplete(result.getPercentComplete());
                    getState().setProcessedRowsCount(result.getTotal());
                    getState().setAllRowsCount(result.getAllRowsCount() == -1 ? result.getTotal() : result.getAllRowsCount());
                    nodeStateConsumer.accept(getState());
                }
            });
            IndexResult result = (IndexResult) taskInfo.getResult();
            externalId = result.getIndexName();
            result.getProcessionErrors().forEach(e -> log.warn(e.toString()));

            logEntry.setSuccess(result.isSuccess());
            logEntry.setDuration(timer.stop().elapsed(TimeUnit.MILLISECONDS));
            logEntry.setUserId(taskInfo.getRequest().getAuth().getUserId());
            logEntry.setBillableUserId(Auth.get().getUserId());
            logEntry.setRowsIngested(result.getTotal());
            logEntry.setStorageType(EngineType.ES);
            logEntry.setExternalId(externalId);
            logEntry.setAccountId(accountId);
            logEntry.setSize(indexService.getStorageSpace(externalId));
            log.info("Loaded " + result.getTotal() + " rows ("
                     + result.getProcessionErrors().size() + " procession errors"
                     + ") in " + timer.toString());
            log.info("Total time:" +
                     "\n\tREAD: " + result.getReadTime() +
                     "\n\tTRFM: " + result.getTransformTime() +
                     "\n\tWAIT: " + result.getWaitTime() +
                     "\n\tEXEC: " + result.getExecutionTime());
            return result;
        } finally {
            actionLogService.save(logEntry);
        }
    }

    @Override
    public void execute(Consumer<NodeState> nodeStateConsumer) {
        startTime = System.currentTimeMillis();
        TableBookmark bookmark = tableRepository.getTableBookmark(this.getSettings().getBookmarkId());
        if (bookmark == null) {
            throw new RuntimeException("Bookmark doesn't exist");
        }
        Descriptor descriptor = getChildren().get(0).getResult();
        EngineSelectionStrategy engineSelectionStrategy = null;
        if(EngineSelectionStrategy.isAllowManualSelection()){
            engineSelectionStrategy = getFlow().getSettings().getEngineSelectionStrategy();
        }
        if(engineSelectionStrategy == null) {
            engineSelectionStrategy = EngineSelectionStrategy.current();
        }
        engineType = engineSelectionStrategy.getEngineType(descriptor);
        NodeState state = getState();
        state.setAllRowsCount(descriptor.getRowsCount());
        nodeStateConsumer.accept(state);
        switch (engineType){
            case BIGQUERY:
                accountId = accountSelectionStrategy.getAccount(descriptor);
                taskResult = loadFileFromGCSIntoBigQuery(bookmark, descriptor, nodeStateConsumer);
                break;
            case ES:
                taskResult = indexWithElasticSearch(bookmark, descriptor, nodeStateConsumer);
                break;
            default:
                throw new RuntimeException("Unknown engine type: " + engineType);
        }
    }

    @Override
    public void onAfterExecute(Descriptor descriptor) {
        TableBookmark bookmark = tableRepository.getTableBookmark(this.getSettings().getBookmarkId());
        Datadoc datadoc = bookmark.getDatadoc();
        // clear all ID properties (nested objects too) since we're gonna save it to DB
        BeanUtils.walk(descriptor, (k, v) -> "id".equals(k) ? null : v);
        descriptor.getColumns().sort(ColumnInfo::compareTo);
        descriptor.getColumns().forEach(c -> c.setDisableFacets(c.getSettings().isDisableFacets()));
        List<Upload> uploads = flowService.getUploads(this.getFlow());
        Long executionTime = System.currentTimeMillis() - startTime;
        taskResult.setExecutionTime(executionTime);
        Stopwatch stopwatch = Stopwatch.createStarted();
        TaskInfo pendingTask = tasksRepository.getPendingIngestTaskInfoByChildTask(this.getSettings().getParentTaskId(), Auth.get().getUserId());
        boolean sourceChanged = isSourceChanged(descriptor, bookmark);
        tableService.updateBookmarkSchema(bookmark.getId(), engineType, externalId, accountId, executionTime,
                                          descriptor, uploads,
                                          getFlow().getSettings(), taskResult, pendingTask);

        tableService.saveCleanTableBookmarkState(bookmark.getId(), bookmark.getDefaultState());
        flowExecutor.preview(Auth.get().getUserId(), new PreviewRequest(bookmark.getBookmarkStateId(), "OUTPUT", false, 1000), (ns) -> {});

        if(sourceChanged) {
            List<BookmarkStateId> statesForRemove = bookmark.getAllBookmarkStates().stream()
                    .map(bs -> new BookmarkStateId(bs.getTabId(), bs.getUuid(), bookmark.getDatadoc().getUserId()))
                    .collect(Collectors.toList());
            bookmarkStateService.modifyStatesForUser(descriptor, statesForRemove);
            bookmark.getDatadoc().getSharedUsers().forEach(userFileShare -> {
                List<BookmarkStateId> userStates = bookmarkStateRepository.getAllStatesIdsByTabAndUserId(bookmark.getId(), userFileShare.getUserId());
                bookmarkStateService.modifyStatesForUser(descriptor, userStates);
            });
        }
        List<User> sharedUsers = datadoc.getPublicShared() ? datadoc.getSharedWithOwner() : datadoc.getSharedWith();
        sharedUsers.forEach(shareInfo -> tableService.initSharedDatadocIfNeeded(datadoc, datadoc.getPublicShared(), shareInfo.getId()));

        log.info("Updating bookmark schema took {}", stopwatch.stop());
    }

    private boolean isSourceChanged(Descriptor descriptor, TableBookmark bookmark) {
        Descriptor currentBookmarkDescriptor = bookmark.getTableSchema().getDescriptor();
        if(currentBookmarkDescriptor == null) {
            return false;
        }
        currentBookmarkDescriptor.setColumns(Lists.newArrayList(currentBookmarkDescriptor.getColumns()));
        return !currentBookmarkDescriptor.equals(descriptor);
    }

    @Override
    protected String getNodeTypeName() {
        return "Final Output";
    }
}
