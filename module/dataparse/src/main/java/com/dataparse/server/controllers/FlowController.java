package com.dataparse.server.controllers;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.flow.*;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.flow.*;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadRepository;
import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/flow")
@Api(value="Flow", description="Flow operations")
public class FlowController extends ApiController {

    @Autowired
    private FlowExecutor flowExecutor;

    @Autowired
    private FlowService flowService;

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private UploadRepository uploadRepository;

    @ApiOperation(value="Execute flow")
    @RequestMapping(value = "/execute", method = RequestMethod.POST, produces = "text/plain")
    public String execute(@RequestBody ExecutionRequest request){
        return flowService.execute(Auth.get(), request);
    }

    @ApiOperation(value="Cancel flow execution")
    @RequestMapping(value = "/cancel", method = RequestMethod.POST, produces = "text/plain")
    public String cancel(@RequestBody CancelRequest request){
        return String.valueOf(flowService.cancel(request));
    }

    @ApiOperation(value="Execute flow preview")
    @RequestMapping(value = "/preview", method = RequestMethod.POST)
    public PreviewResponse preview(@RequestBody PreviewRequest request) throws Exception {
        PreviewResponse preview = flowExecutor.preview(Auth.get().getUserId(), request, ns -> {});
        return preview;
    }

    @ApiOperation(value = "Validate flow and get errors if any")
    @RequestMapping(value = "/validate", method = RequestMethod.POST)
    public List<FlowValidationError> validate(@RequestBody ValidationRequest request){
        return flowExecutor.validate(Auth.get().getUserId(), request);
    }

    @ApiOperation(value = "Get flow execution task state")
    @RequestMapping(value = "/get_state", method = RequestMethod.POST)
    public TaskInfo getState(@RequestBody GetStateRequest request){
        return flowService.getState(request);
    }

    @ApiOperation(value = "Get flow execution task state")
    @RequestMapping(value = "/get_active_tasks", method = RequestMethod.POST)
    public List<FlowExecutionTasksResponse> getActiveTasks(){
        Map<Long, Datadoc> datadocsCache = new HashMap<>();
        Map<Long, Upload> uploadsCache = new HashMap<>();

        List<TaskInfo> activeTasks = flowService.getActiveFlowExecutionTasks(Auth.get().getUserId());
        return activeTasks.stream().map(task -> {
            FlowExecutionRequest request = (FlowExecutionRequest) task.getRequest();
            FlowExecutionResult result = (FlowExecutionResult) task.getResult();
            if (result != null) {
                Long sourceId = result.getSourceIds().stream().min(Long::compareTo).get();

                Datadoc datadoc = datadocsCache.computeIfAbsent(request.getDatadocId(), tableRepository::getDatadoc);
                datadoc.getLastFlowExecutionTasks().add(task.getId());

                Upload upload = uploadsCache.computeIfAbsent(sourceId, uploadRepository::getUploadById);

                return new FlowExecutionTasksResponse(task, datadoc, upload);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    // todo do we need this? it could be replaced with /preview
//    todo review this \now\
    @RequestMapping(value = "/columns", method = RequestMethod.POST)
    public List<String> getResultColumns(@RequestBody PreviewRequest request){
        PreviewResponse tmp = flowExecutor.preview(Auth.get().getUserId(), request, ns -> {});
        Set<String> result = new HashSet<>();
        return Lists.newArrayList(result);
    }

}
