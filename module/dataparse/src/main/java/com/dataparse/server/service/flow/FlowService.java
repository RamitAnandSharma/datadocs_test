package com.dataparse.server.service.flow;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.flow.*;
import com.dataparse.server.service.flow.node.*;
import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.tasks.TaskManagementService;
import com.dataparse.server.service.tasks.TasksRepository;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class FlowService {

    @Autowired
    private BookmarkStateStorage bookmarkStateStorage;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private TaskManagementService taskManagementService;

    @Autowired
    private UploadRepository uploadRepository;

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private TasksRepository tasksRepository;

    public Flow createFlow(Long ownerId, String flowJSON, FlowSettings settings){
        return Flow.create(ownerId, flowJSON, settings, applicationContext);
    }

    public Flow createFlow(Long ownerId, String flowJSON, FlowSettings settings, String rootNode){
        return Flow.create(ownerId, flowJSON, settings, rootNode, applicationContext);
    }

    public List<TaskInfo> getActiveFlowExecutionTasks(Long userId){
        return tasksRepository.getPendingTasksAsList(Collections.singleton(FlowExecutionTask.class.getSimpleName()), userId);
    }

    public TaskInfo getState(GetStateRequest request){
       return tasksRepository.getTaskInfo(request.getTaskId());
    }

    public String execute(Auth auth, ExecutionRequest request){
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getBookmarkId());
        UUID currentState = tableBookmark.getCurrentState();
        BookmarkStateId stateId = new BookmarkStateId(request.getBookmarkId(), currentState);
        BookmarkState state = bookmarkStateStorage.get(stateId, true).getState();

        FlowExecutionRequest flowExecutionRequest = FlowExecutionRequest.builder()
                .flowJSON(state.getFlowJSON())
                .flowSettings(state.getFlowSettings())
                .datadocId(tableBookmark.getDatadoc().getId())
                .userInitiated(request.getUserInitiated())
                .autoIngest(request.isAutoIngest())
                .build();
        return taskManagementService.execute(auth, flowExecutionRequest);
    }

    public boolean cancel(CancelRequest request) {
        return taskManagementService.stop(request.getTaskId());
    }

    public List<Upload> getUploads(Flow flow){
        return flow.getGraph().values().stream()
                .filter(n -> n instanceof InputNode)
                .map(n -> (Upload) uploadRepository.getFile(((InputNodeSettings) n.getSettings()).getUploadId()))
                .collect(Collectors.toList());
    }

}
