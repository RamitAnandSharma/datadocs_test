package com.dataparse.server.controllers;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.flow.CancelRequest;
import com.dataparse.server.service.export.*;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.tasks.TaskManagementService;
import com.dataparse.server.service.tasks.TasksRepository;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;


@Slf4j
@RestController
@RequestMapping("/api/export")
@Api(value = "Export", description = "Export visualization results")
public class ExportController extends ApiController {

    @Autowired
    private TaskManagementService taskManagementService;

    @Autowired
    private ExportService exportService;

    @Autowired
    private ExportRepository exportRepository;

    @Autowired
    private TasksRepository tasksRepository;

    @ApiOperation(value = "Export data")
    @RequestMapping(value = "/prepare", method = RequestMethod.POST)
    public Map<String, Object> prepareExport(@RequestBody ExportTaskRequest request){
        return ImmutableMap.of("requestId", taskManagementService.execute(Auth.get(), request));
    }

    @ApiOperation("Download prepared data export")
    @RequestMapping(value = "/download", method = RequestMethod.GET)
    public void download(HttpServletResponse response, @RequestParam(required =  false) String requestId) {
        exportService.withExportResults(requestId, (is, descriptor) -> {
            try {
                doFileResponse(descriptor.getContentType(), descriptor.getOriginalFileName(), is, response);
                String.valueOf(tasksRepository.removeTask(descriptor.getPath()));
                exportRepository.remove(UUID.fromString(descriptor.getPath()));
            } catch (IOException e) {
                throw new RuntimeException("Can't download export results", e);
            }
        });
    }

    @ApiOperation("Get pending data exports")
    @RequestMapping(value = "/pending", method = RequestMethod.GET)
    public List<TaskInfo> getPendingExports(@RequestParam Long datadocId){
        return tasksRepository.getTaskInfo(Sets.newHashSet(ExportTask.class.getSimpleName()),
                (cursor) -> cursor.is("finished", false).is("removed", false));
    }

    @ApiOperation("Cancel data export")
    @RequestMapping(value = "/cancel", method = RequestMethod.POST, produces = "text/plain")
    public String cancel(@RequestBody CancelRequest request){
        tasksRepository.removeTask(request.getTaskId());
        return String.valueOf(taskManagementService.stop(request.getTaskId()));
    }

    @ApiOperation("Remove data export")
    @RequestMapping(value = "/delete", method = RequestMethod.POST, produces = "text/plain")
    public String remove(@RequestBody CancelRequest request){
        String result = String.valueOf(tasksRepository.removeTask(request.getTaskId()));
        exportRepository.remove(UUID.fromString(request.getTaskId()));
        return result;
    }

}
