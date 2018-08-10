package com.dataparse.server.service.schema;

import com.dataparse.server.RestServer;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.flow.ExecutionRequest;
import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.service.bigquery.BigQueryService;
import com.dataparse.server.service.bigquery.cache.IntelliCache;
import com.dataparse.server.service.bookmark.BookmarkStateService;
import com.dataparse.server.service.docs.BookmarkStatistics;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.docs.DatadocStatistics;
import com.dataparse.server.service.engine.EngineType;
import com.dataparse.server.service.es.index.IndexService;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.files.ShareType;
import com.dataparse.server.service.files.UserFileShare;
import com.dataparse.server.service.files.UserFileShareId;
import com.dataparse.server.service.flow.FlowGenerator;
import com.dataparse.server.service.flow.FlowNotificationUtils;
import com.dataparse.server.service.flow.FlowService;
import com.dataparse.server.service.flow.settings.FlowSettings;
import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.schema.log.BookmarkActionLogRepository;
import com.dataparse.server.service.schema.log.BookmarkActionLogService;
import com.dataparse.server.service.schema.log.DataRemovedLogEntry;
import com.dataparse.server.service.share.ShareRepository;
import com.dataparse.server.service.tasks.ScheduledTaskService;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.tasks.TaskManagementService;
import com.dataparse.server.service.tasks.TaskResult;
import com.dataparse.server.service.tasks.scheduled.RefreshIndexJob;
import com.dataparse.server.service.tasks.utils.PendingIngestionRequest;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.upload.refresh.RefreshType;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.visualization.VisualizationService;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkBuilderFactory;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.dataparse.server.service.flow.FlowNotificationUtils.getErrorMessageByField;

@Slf4j
@Service
public class TableService {

    @Autowired
    private BookmarkStateService bookmarkStateService;

    @Autowired
    private ShareRepository shareRepository;

    @Autowired
    private TaskManagementService taskManagementService;

    @Autowired
    private FlowService flowService;

    @Autowired
    private VisualizationService visualizationService;

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    @Autowired
    private BookmarkStateStorage bookmarkStateStorage;

    @Autowired
    private BookmarkStateRepository bookmarkStateRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    @Autowired
    private IntelliCache queryCache;

    @Autowired
    private IndexService indexService;

    @Autowired
    private BigQueryService bigQueryService;

    @Autowired
    private BookmarkActionLogRepository actionLogRepository;

    @Autowired
    private BookmarkActionLogService actionLogService;

    @Autowired
    private UploadRepository uploadRepository;

    @PostConstruct
    protected void init() {
        // schedule refresh jobs
        if (RestServer.isMaster()) {
            tableRepository.getScheduledBookmarks().forEach(bookmark -> {
                if(bookmark.getTableSchema().getRefreshSettings() != null) {
                    scheduledTaskService.schedule(new RefreshIndexJob(bookmark.getDatadoc().getUserId(), bookmark));
                }
            });
        }
    }

    public TableBookmark createTableBookmark(Long datadocId, Long duplicateId, UUID duplicateStateId) {
        return createTableBookmark(datadocId, duplicateId, duplicateStateId, null);
    }

    // todo refactor
    public TableBookmark saveCleanTableBookmarkState(Long bookmarkId, UUID stateId) {
        TableBookmark tableBookmark = tableRepository.getTableBookmark(bookmarkId);
        BookmarkState newState = createOrDuplicateState(bookmarkId, stateId, tableBookmark);
        newState.setStateName("Clean");
        newState.setCleanState(true);
        newState.setChangeable(false);
        bookmarkStateStorage.init(newState, true);
        tableBookmark.addBookmarkState(newState);
        return tableRepository.updateTableBookmark(tableBookmark);
    }

    public TableBookmark saveTableBookmarkView(Long bookmarkId, UUID stateId, String stateName) {
        TableBookmark tableBookmark = tableRepository.getTableBookmark(bookmarkId);
        BookmarkState newState = createOrDuplicateState(bookmarkId, stateId, tableBookmark);
        newState.setStateName(stateName);
        newState.setChangeable(false);
        bookmarkStateStorage.init(newState, true);
        tableBookmark.addBookmarkState(newState);
        TableBookmark result = tableRepository.updateTableBookmark(tableBookmark);
        result.setCurrentState(newState.getId());
        return result;
    }

    public TableBookmark createTableBookmark(Long datadocId, Long duplicateId, UUID duplicateStateId, String name){
        TableBookmark bookmark = tableRepository.initTableBookmark(name, duplicateId);
        BookmarkState state = createOrDuplicateState(duplicateId, duplicateStateId, bookmark);
        TableBookmark savedBookmark = tableRepository.saveBookmark(bookmark, datadocId, state);
        state.getBookmarkStateId().setTabId(savedBookmark.getId());
        bookmarkStateStorage.init(state, true);
        return bookmark;
    }

    private BookmarkState createOrDuplicateState(Long duplicateTabId, UUID duplicateStateId, TableBookmark tableBookmark) {
        if(duplicateTabId == null) {
            return bookmarkBuilderFactory.create(tableBookmark).build();
        } else {
            BookmarkStateId stateId = new BookmarkStateId(duplicateTabId, duplicateStateId);
            BookmarkState state = bookmarkStateStorage.get(stateId, true).getState().copy();
            state.setBookmarkStateId(new BookmarkStateId(tableBookmark.getId(), UUID.randomUUID()));
            return state;
        }
    }

    public void initSharedDatadocIfNeeded(Datadoc datadoc, Boolean publicAccess, Long currentUserId) {
        User currentUser = userRepository.getUser(currentUserId);
        if (datadoc.getUser().getId().equals(currentUser.getId()) && !datadoc.getPublicShared()) {
            return;
        }
        List<TableBookmark> tableBookmarks = tableRepository.getTableBookmarks(datadoc.getId(), false);
        if (publicAccess) {
            UserFileShareId shareId = new UserFileShareId(currentUser, datadoc);
            UserFileShare shareInfo = new UserFileShare(shareId, ShareType.VIEW, datadoc.getUser(), datadoc.getUser());
            shareRepository.saveShareDatadoc(shareInfo);
        }
        tableBookmarks.forEach(bookmark -> {
            BookmarkStateId stateId = new BookmarkStateId(bookmark.getId(), bookmark.getDefaultState(), currentUserId);
            BookmarkStateHistoryWrapper bookmarkStateHistoryWrapper = bookmarkStateStorage.get(stateId, false);
            BookmarkState state = bookmarkStateHistoryWrapper == null
                    ? initSharedBookmarkStates(currentUserId, bookmark, true)
                    : bookmarkStateHistoryWrapper.getState();
            bookmark.setState(state);
        });
    }

    private BookmarkState initSharedBookmarkStates(Long shareToUserId, TableBookmark bookmark, Boolean clean) {
        BookmarkState state = null;
        List<BookmarkState> allBookmarkStates = bookmarkStateRepository.getAllBookmarkStates(bookmark.getId(), bookmark.getCreatedByUser().getId());
        for (BookmarkState bookmarkState : allBookmarkStates) {
            boolean isDefaultState = bookmark.getDefaultState().equals(bookmarkState.getId());
            BookmarkState duplicateState = bookmarkState.copy();
            duplicateState.getBookmarkStateId().setUserId(shareToUserId);
            duplicateState.setUserId(shareToUserId);
            duplicateState.setShared(true);
            if (isDefaultState) {
                duplicateState.setCleanState(clean);
                state = duplicateState;
            }
            bookmarkStateStorage.init(duplicateState, true);
        }
        return state;
    }

    public void removeTableBookmark(Long bookmarkId){
        TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
        removeExternalTable(bookmark,
                            bookmark.getTableSchema().getEngineType(),
                            bookmark.getTableSchema().getAccountId(),
                            bookmark.getTableSchema().getExternalId());
        if(bookmark.getTableSchema().getRefreshSettings() != null) {
            scheduledTaskService.cancel(new RefreshIndexJob(Auth.get().getUserId(), bookmark));
        }
        tableRepository.removeTableBookmark(bookmarkId);
        bookmark.getAllBookmarkStates().forEach(st -> bookmarkStateStorage.evict(new BookmarkStateId(st.getTabId(), st.getUuid())));
    }

    public Datadoc createDatadoc(CreateDatadocRequest request) {
        Datadoc datadoc = new Datadoc();
        datadoc.setShareId(UUID.randomUUID());
        datadoc.setName(request.getName());
        if(request.getParentId() != null) {
            datadoc.setParent(uploadRepository.getFile(request.getParentId()));
        }
        datadoc.setEmbedded(request.getEmbedded());
        datadoc.setPreSaved(request.getPreSave());
        datadoc.setUser(userRepository.getUser(Auth.get().getUserId()));
        datadoc.setCreated(new Date());
        tableRepository.saveDatadoc(datadoc);

        if(StringUtils.isBlank(request.getSourcePath())) {
            return createTableBookmark(datadoc.getId(), null, null).getDatadoc();
        } else {
            Long fileId = uploadRepository.resolveFileIdByPath(request.getSourcePath());
            if(fileId == null){
                throw new RuntimeException("Can't resolve file path: " + request.getSourcePath());
            }
            AbstractFile baseFile = uploadRepository.getFile(fileId, false, true, false);
            if(!(baseFile instanceof Upload)){
                throw new RuntimeException("File can not be parsed");
            }
            Upload baseUpload = (Upload) baseFile;
            List<Upload> sections = Lists.newArrayList();

            if(baseUpload.getDescriptor().isComposite()){
                if(!baseUpload.getSections().isEmpty()) {
                    DataFormat format = baseUpload.getDescriptor().getFormat();
                    if(DataFormat.XLS.equals(format) || DataFormat.XLSX.equals(format)){
                        sections = baseUpload.getSections();
                    } else if (!(baseUpload.getDescriptor() instanceof DbDescriptor)){
                        sections.add(baseUpload.getSections().get(0));
                    }
                }
            }
            if(sections.size() == 0) {
                sections.add(baseUpload);
            }
            List<String> tasks = sections.stream().map(upload -> {
                TableBookmark bookmark = createTableBookmark(datadoc.getId(), null, null, upload.getName());
                BookmarkState state = bookmark.getState();
                state.setFlowJSON(FlowGenerator.singleSourceFlow(upload, bookmark.getId()).toString());
                bookmarkStateStorage.init(state, true);
                bookmark.setState(state);
                if (request.isAutoIngest()) {
                    ExecutionRequest executionRequest = new ExecutionRequest(bookmark.getId(), false, EngineType.ES, true);
                    return flowService.execute(Auth.get(), executionRequest);
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
            if(!request.getPreSave()) {
                PendingIngestionRequest pendingRequest = new PendingIngestionRequest(tasks, request.getPreProcessionTime());
                datadoc.setGathererTask(taskManagementService.execute(Auth.get(), pendingRequest));
            }

            datadoc.setLastFlowExecutionTasks(tasks);
            return datadoc;
        }
    }

    public Datadoc updateDatadoc(Long datadocId, UpdateDatadocRequest request) {
        Datadoc datadoc = tableRepository.getDatadoc(datadocId);
        datadoc.setName(request.getName());
        datadoc.setPreSaved(request.getPreSave());
        datadoc.setEmbedded(request.getEmbedded());
        tableRepository.saveDatadoc(datadoc);
        return datadoc;
    }

    public void removeDatadoc(Long datadocId) {
        Datadoc datadoc = tableRepository.getDatadoc(datadocId);
        if (datadoc == null) {
            throw new RuntimeException("Table doesn't exist");
        }
        List<TableBookmark> bookmarks = tableRepository.getTableBookmarks(datadocId);
        for(TableBookmark bookmark : bookmarks) {
            removeTableBookmark(bookmark.getId());
        }
        tableRepository.deleteDatadoc(datadocId);
    }

    private void applyRefreshSettings(TableBookmark bookmark){
        if (bookmark.getTableSchema().getRefreshSettings().getType().equals(RefreshType.NONE)) {
            scheduledTaskService.cancel(new RefreshIndexJob(Auth.get().getUserId(), bookmark));
        } else {
            scheduledTaskService.schedule(new RefreshIndexJob(Auth.get().getUserId(), bookmark));
        }
    }

    public RefreshSettings updateRefreshSettings(Long bookmarkId, UpdateRefreshSettingsRequest request){
        TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
        TableSchema schema = bookmark.getTableSchema();
        schema.setRefreshSettings(request.getSettings());
        if(schema.getCommitted() != null) {
            applyRefreshSettings(bookmark);
        }
        tableRepository.saveTableSchema(schema);
        return schema.getRefreshSettings();
    }

    private Long removeExternalTable(TableBookmark bookmark,
                                     EngineType engineType, String accountId, String externalId) {
        if (engineType == null) {
            return 0L;
        }
        Date started = new Date();
        Stopwatch timer = Stopwatch.createStarted();
        Long storageSpace;
        try {
            switch (engineType) {
                case BIGQUERY:
                    storageSpace = bigQueryService.getStorageSpace(accountId, externalId);
                    queryCache.evict(externalId);
                    bigQueryService.deleteDataset(accountId, externalId);
                    break;
                case ES:
                    storageSpace = indexService.getStorageSpace(externalId);
                    indexService.removeIndex(externalId);
                    break;
                default:
                    throw new RuntimeException("Unknown engine type:" + engineType);
            }

        } catch (Exception e){
            log.error("Can't determine storage space", e);
            return 0L;
        }
        DataRemovedLogEntry dataRemovedLogEntry = DataRemovedLogEntry.of(bookmark);
        dataRemovedLogEntry.setUserId(Auth.get().getUserId());
        dataRemovedLogEntry.setBillableUserId(Auth.get().getUserId());
        dataRemovedLogEntry.setSize(storageSpace);
        dataRemovedLogEntry.setStartTime(started);
        dataRemovedLogEntry.setDuration(timer.stop().elapsed(TimeUnit.MILLISECONDS));
        dataRemovedLogEntry.setExternalId(externalId);
        dataRemovedLogEntry.setAccountId(accountId);
        actionLogService.save(dataRemovedLogEntry);
        return storageSpace;
    }

    public void updateBookmarkSchema(Long bookmarkId, EngineType engineType, String externalId, String accountId,
                                     Long executionTime, Descriptor newDescriptor, List<Upload> newUploads,
                                     FlowSettings flowSettings, TaskResult taskResult, TaskInfo pendingTask){
        TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
        Datadoc datadoc = bookmark.getDatadoc();
        TableSchema schema = bookmark.getTableSchema();
        String oldExternalId = schema.getExternalId();
        String oldAccountId = schema.getAccountId();
        EngineType oldEngineType = schema.getEngineType();
        executionTime += 4000;
        if(schema.getFirstCommitted() == null){
            schema.setFirstCommitted(schema.getCommitted());
        }
        schema.setCommitted(new Date());
        schema.setExternalId(externalId);
        schema.setAccountId(accountId);
        schema.setEngineType(engineType);
        schema.setLastCommitDuration(executionTime);

        datadoc.setCommitted(schema.getCommitted());
        tableRepository.saveDatadoc(datadoc);
        BookmarkState state = bookmarkStateStorage.get(bookmark.getBookmarkStateId(), true).getState();
        state.setBookmarkStateId(bookmark.getBookmarkStateId());
        state.setPendingFlowJSON(state.getFlowJSON());
        state.setPageMode(PageMode.VIZ);
        state = bookmarkBuilderFactory.create(bookmark).update(state, newDescriptor, true);
        String errorMessage = getErrorMessageByField(taskResult, flowSettings);
        if(!StringUtils.isEmpty(errorMessage)) {
            state.getNotifications().add(
                    new Notification(schema.getCommitted(), 1,
                            errorMessage));
        }
        if(pendingTask == null) {
            String successMessage = FlowNotificationUtils.getProcessionTotalString(taskResult, executionTime);
            if(successMessage != null) {
                state.getNotifications().add(new Notification(schema.getCommitted(), 0, successMessage));
            }
        }

        bookmarkStateStorage.init(state, true);

        schema.setDescriptor(newDescriptor);
        schema.setUploads(newUploads);
        tableRepository.saveTableSchema(schema);

        if(!externalId.equals(oldExternalId) && oldExternalId != null){
            Long totalIngested = schema.getTotalIngested();
            if(totalIngested == null){
                totalIngested = 0L;
            }
            long storageSpace = removeExternalTable(bookmark, oldEngineType, oldAccountId, oldExternalId);
            totalIngested += storageSpace;
            schema.setTotalIngested(totalIngested);
            tableRepository.saveTableSchema(schema);
        }
        this.doCacheRequest(bookmark.getId(), state.getQueryParams());
        applyRefreshSettings(bookmark);
    }

    private SearchIndexResponse doCacheRequest(Long bookmarkId, QueryParams queryParams) {
        SearchIndexRequest searchRequest = new SearchIndexRequest();
        searchRequest.setTableBookmarkId(bookmarkId);
        searchRequest.setParams(queryParams);
        return visualizationService.search(searchRequest);
    }

    private void retrieveStorageStatistics(TableBookmark bookmark, BookmarkStatistics statistics){
        switch (bookmark.getTableSchema().getEngineType()){
            case ES:
                indexService.retrieveStorageStatistics(bookmark.getTableSchema().getExternalId(), statistics);
                break;
            case BIGQUERY:
                bigQueryService.retrieveStorageStatistics(bookmark.getTableSchema().getAccountId(), bookmark.getTableSchema().getExternalId(), statistics);
                break;
            default:
                throw new RuntimeException("Unknown storage type: " + bookmark.getTableSchema().getEngineType());
        }
    }

    public DatadocStatistics getDatadocStatistics(Long datadocId){
        DatadocStatistics datadocStatistics = new DatadocStatistics();

        Datadoc datadoc = tableRepository.getDatadoc(datadocId);
        datadocStatistics.setDatadocId(datadocId);
        datadocStatistics.setDatadocName(datadoc.getName());

        List<TableBookmark> bookmarks = tableRepository.getTableBookmarks(datadocId, false);
        datadocStatistics.setBookmarks(new ArrayList<>(bookmarks.size()));
        bookmarks.forEach(bookmark -> {
            BookmarkStatistics bookmarkStatistics = new BookmarkStatistics();
            bookmarkStatistics.setBookmarkId(bookmark.getId());
            bookmarkStatistics.setBookmarkName(bookmark.getName());
            bookmarkStatistics.setCreated(bookmark.getCreated());
            bookmarkStatistics.setCreatedByUserId(datadoc.getUserId()); // todo edit when sharing implemented
            bookmarkStatistics.setCreatedByUserName(datadoc.getUserName());
            if(bookmark.getTableSchema().getCommitted() != null) {
                bookmarkStatistics.setColumns(bookmark.getTableSchema().getDescriptor().getColumns().size());                String storageType = null;
                switch (bookmark.getTableSchema().getEngineType()) {
                    case BIGQUERY:
                        storageType = "Large";
                        break;
                    case ES:
                        storageType = "Small";
                        break;
                }
                bookmarkStatistics.setStorageType(storageType);
                bookmarkStatistics.setLastSaved(bookmark.getTableSchema().getCommitted());
                bookmarkStatistics.setLastSavedByUserId(datadoc.getUserId()); // todo edit when sharing implemented
                bookmarkStatistics.setLastSavedByUserName(datadoc.getUserName());

                if(new DateTime(bookmark.getCreated()).hourOfDay().roundFloorCopy()
                        .equals(new DateTime(bookmark.getTableSchema().getCommitted()).hourOfDay().roundFloorCopy())){
                    bookmarkStatistics.setCreatedAndLastSavedAreSameToHours(true);
                }
                retrieveStorageStatistics(bookmark, bookmarkStatistics);
                bookmarkStatistics.setDataProcessed(actionLogRepository.getTotalBytesByBookmark(bookmark.getId()));
                Long ingested = bookmark.getTableSchema().getTotalIngested();
                if(bookmarkStatistics.getStorageSpace() != null) {
                    if(ingested == null){
                        ingested = 0L;
                    }
                    ingested += bookmarkStatistics.getStorageSpace();
                    bookmarkStatistics.setDataIngested(ingested);
                }
            }
            datadocStatistics.getBookmarks().add(bookmarkStatistics);
        });
        datadocStatistics.setSheets(bookmarks.size());
        datadocStatistics.setCreated(datadoc.getCreated());
        datadocStatistics.setCreatedByUserId(datadoc.getUserId());
        datadocStatistics.setCreatedByUserName(datadoc.getUserName());
        datadocStatistics.setLastSaved(datadocStatistics.getBookmarks()
                                               .stream()
                                               .map(b -> b.getLastSaved())
                                               .filter(Objects::nonNull)
                                               .reduce((saved, lastSaved) -> saved.after(lastSaved) ? saved : lastSaved)
                                               .orElse(null));
        datadocStatistics.setDataProcessed(datadocStatistics.getBookmarks()
                                                   .stream()
                                                   .map(b -> b.getDataProcessed())
                                                   .filter(Objects::nonNull)
                                                   .reduce((processed, total) -> total + processed)
                                                   .orElse(null));
        datadocStatistics.setDataIngested(datadocStatistics.getBookmarks()
                                                   .stream()
                                                   .map(b -> b.getDataIngested())
                                                   .filter(Objects::nonNull)
                                                   .reduce((ingested, total) -> total + ingested)
                                                   .orElse(null));
        datadocStatistics.setStorageSpace(datadocStatistics.getBookmarks()
                                                  .stream()
                                                  .map(b -> b.getStorageSpace())
                                                  .filter(Objects::nonNull)
                                                  .reduce((space, total) -> total + space)
                                                  .orElse(null));
        datadocStatistics.setTotalRows(datadocStatistics.getBookmarks()
                                               .stream()
                                               .map(b -> b.getRows())
                                               .filter(Objects::nonNull)
                                               .reduce((rows, total) -> total + rows)
                                               .orElse(null));
        datadocStatistics.setStorageTypes(datadocStatistics.getBookmarks()
                                                  .stream()
                                                  .map(b -> b.getStorageType())
                                                  .filter(Objects::nonNull)
                                                  .distinct()
                                                  .collect(Collectors.joining(", ")));
        return datadocStatistics;
    }

}
