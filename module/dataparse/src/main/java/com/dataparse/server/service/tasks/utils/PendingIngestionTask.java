package com.dataparse.server.service.tasks.utils;

import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.flow.FlowExecutionResult;
import com.dataparse.server.service.flow.FlowNotificationUtils;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.storage.IStorageStrategy;
import com.dataparse.server.service.storage.StorageSelectionStrategy;
import com.dataparse.server.service.storage.StorageType;
import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.service.upload.UploadService;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.event.NotificationAddEvent;
import com.dataparse.server.service.visualization.bookmark_state.state.Notification;
import com.dataparse.server.util.thread.SilentThreadWithRetries;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.mongojack.DBQuery;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
// todo generalize
public class PendingIngestionTask extends AbstractTask<PendingIngestionRequest> {

    public static final Integer FIRST_BOOKMARK_INDEX = 0;
    @Autowired
    @JsonIgnore
    TasksRepository tasksRepository;

    @Autowired
    @JsonSerialize
    private IStorageStrategy storageStrategy;

    @Autowired
    @JsonIgnore
    BookmarkStateStorage stateStorage;

    @Autowired
    @JsonIgnore
    StorageSelectionStrategy storageSelectionStrategy;

    @Autowired
    @JsonIgnore
    TableRepository tableRepository;

    @Autowired
    @JsonIgnore
    UploadService uploadService;

    @Autowired
    @JsonIgnore
    UploadRepository uploadRepository;


    @Override
    public Map<TaskState, Runnable> getStates() {
        return ImmutableMap.of(PendingIngestionTaskState.WAIT, () -> {

            Long userId = getRequest().getAuth().getUserId();

            Retryer<List<TaskInfo>> retryer = RetryerBuilder.<List<TaskInfo>>newBuilder()
                    .retryIfResult(pending -> pending.size() != 0)
                    .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS))
                    .withStopStrategy(StopStrategies.neverStop())
                    .build();
            try {
                retryer.call(() -> tasksRepository.getPendingTasksByIds(getRequest().getPendingTasks(), userId));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void onAfterFinish(TaskInfo info) {
        long start = System.currentTimeMillis();
        List<TaskInfo> tasks = tasksRepository.getFinishedTasksInfo(getRequest().getPendingTasks());
        Long maxTaskDuration = tasks.stream().map(task -> task.getStatistics().getDuration()).max(Long::compareTo).get();
        long createDatadocTime = maxTaskDuration + getRequest().getPreProcessionTime();

        List<TableBookmark> processedBookmarks = tasks.stream().map(task -> ((FlowExecutionResult) task.getResult()).getBookmarkId()).
                map(bookmarkId -> tableRepository.getTableBookmark(bookmarkId, false)).
                collect(Collectors.toList());

        TableBookmark firstBookmark = processedBookmarks.stream().
                filter((bookmark -> FIRST_BOOKMARK_INDEX.equals(bookmark.getPosition()))).
                findFirst().get();

        Optional<Long> totalProcessedRows = tasks.stream().map((task) -> {
            Set<String> parentTasks = Sets.newHashSet(task.getId());
            DBQuery.Query parentTaskQuery = DBQuery.exists("parentTask");
            IngestTaskResult indexResult = (IngestTaskResult) tasksRepository.getTasksHierarchy(parentTasks, parentTaskQuery, 0).
                    get(0).getResult();
            return indexResult.getTotal();
        }).reduce(Long::sum);

        createDatadocTime += (System.currentTimeMillis() - start) + 3000; // +3s? Yes.
        addSuccessNotification(createDatadocTime, firstBookmark, totalProcessedRows, tasks.size());
        copySourceFileToGCS(tasks);
    }

    private void copySourceFileToGCS(List<TaskInfo> tasks) {
        Set<Long> processedUploads = tasks.stream().flatMap(task -> ((FlowExecutionResult) task.getResult()).getSourceIds()
                .stream())
                .collect(Collectors.toSet());
        List<AbstractFile> files = uploadRepository.getFiles(processedUploads);
        Upload upload = (Upload) files.get(0);
        if(upload.getDescriptor() instanceof FileDescriptor) {
            String gcsFileName = UUID.randomUUID().toString();
            FileDescriptor sampleDescriptor = (FileDescriptor) upload.getDescriptor();
            String fileId = sampleDescriptor.getPath();
            if(!StorageType.GD.equals(sampleDescriptor.getStorage())) {
                return;
            }
            new SilentThreadWithRetries(false, () -> {
                List<FileDescriptor> descriptorForUpdate = files.stream().map(file -> {
                    FileDescriptor descriptor = (FileDescriptor) ((Upload) file).getDescriptor();
                    descriptor.setStorage(StorageType.GCS);
                    descriptor.setPath(gcsFileName);
                    return descriptor;
                }).collect(Collectors.toList());
                uploadService.copyFileToGCS(fileId, gcsFileName);
                log.info("Update descriptors: {}", descriptorForUpdate.stream().map(FileDescriptor::getId).collect(Collectors.toList()));
                uploadRepository.updateDescriptors(descriptorForUpdate);
                storageStrategy.get(StorageType.GD).removeFile(fileId);
                return true;
            }).start();
        }
    }

    private void addSuccessNotification(long createDatadocTime, TableBookmark firstBookmark, Optional<Long> totalProcessedRows, Integer tasksCount) {
        NotificationAddEvent event = new NotificationAddEvent(getSuccessNotification(createDatadocTime, totalProcessedRows.get(), tasksCount));
        event.setTabId(firstBookmark.getId());
        event.setStateId(firstBookmark.getBookmarkStateId().getStateId());
        event.setUser(getRequest().getAuth().getUserId());
        stateStorage.add(event);
    }

    private Notification getSuccessNotification(Long totalTime, Long totalCount, Integer tasksCount) {
        String message = FlowNotificationUtils.getProcessionTotalString(totalCount, totalTime, tasksCount);
        return new Notification(new Date(), 0, message);
    }

    @Override
    public void cancel() {

    }
}
