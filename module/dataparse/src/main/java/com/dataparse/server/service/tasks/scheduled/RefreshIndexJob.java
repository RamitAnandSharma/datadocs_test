package com.dataparse.server.service.tasks.scheduled;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.flow.FlowExecutionRequest;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.tasks.TaskManagementService;
import com.dataparse.server.service.tasks.TasksRepository;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@Slf4j
@Data
public class RefreshIndexJob extends ScheduledJob {

    @Autowired
    TaskManagementService taskManagementService;

    @Autowired
    TasksRepository tasksRepository;

    @Autowired
    BookmarkStateStorage bookmarkStateStorage;

    private Long userId;
    private TableBookmark bookmark;

    public RefreshIndexJob(Long userId, TableBookmark bookmark) {
        this.userId = userId;
        this.bookmark = bookmark;
        this.setCronExpression(bookmark.getTableSchema().getRefreshSettings().getCronExpression());
        this.setTimeZone(bookmark.getTableSchema().getRefreshSettings().defineTimeZone());
    }

    @Override
    public void run() {
        List<TaskInfo> pendingTasks = tasksRepository.getPendingFlowTasksAsListByBookmark(userId, bookmark.getId());
        if(!pendingTasks.isEmpty()) {
            log.info("Pending tasks detected. Skipping the scheduled task.");
            return;
        }
        Long datadocId = bookmark.getDatadoc().getId();
        BookmarkStateId bookmarkStateId = bookmark.getBookmarkStateId();
        bookmarkStateId.setUserId(userId);
        BookmarkState state = bookmarkStateStorage.get(bookmarkStateId, true).getState();
        FlowExecutionRequest request = FlowExecutionRequest.builder()
                .datadocId(datadocId)
                .flowJSON(state.getFlowJSON())
                .flowSettings(state.getFlowSettings())
                .scheduled(true)
                .preserveHistory(bookmark.getTableSchema().getRefreshSettings().isPreserveHistory())
                .build();
        taskManagementService.execute(new Auth(userId, null), request);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RefreshIndexJob that = (RefreshIndexJob) o;
        return bookmark.getId().equals(that.bookmark.getId());
    }

    @Override
    public int hashCode() {
        return bookmark.getId().hashCode();
    }
}
