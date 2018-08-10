package com.dataparse.server.service.flow;

import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.flow.event.FlowExecutionCompleteEvent;
import com.dataparse.server.service.flow.event.FlowExecutionProgressEvent;
import com.dataparse.server.service.flow.event.FlowExecutionStartEvent;
import com.dataparse.server.service.flow.node.InputNodeSettings;
import com.dataparse.server.service.flow.node.Node;
import com.dataparse.server.service.flow.node.NodeState;
import com.dataparse.server.service.flow.node.OutputNode;
import com.dataparse.server.service.mail.DisconnectedSourceEmail;
import com.dataparse.server.service.mail.MailService;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.tasks.AbstractTask;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.tasks.TaskState;
import com.dataparse.server.service.upload.DbDescriptor;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.service.visualization.VisualizationService;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.util.Debounce;
import com.dataparse.server.websocket.SockJSService;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Data
@Slf4j
public class FlowExecutionTask extends AbstractTask<FlowExecutionRequest> {

    private final static String BASE_FLOW_TOPIC = "/flow-events";

    @Autowired
    @JsonIgnore
    private FlowExecutor executor;

    @Autowired
    @JsonIgnore
    private MailService mailService;

    @Autowired
    @JsonIgnore
    private FlowService flowService;

    @Autowired
    @JsonIgnore
    private TableRepository tableRepository;

    @Autowired
    @JsonIgnore
    private SockJSService sockJSService;

    @Autowired
    @JsonIgnore
    private VisualizationService visualizationService;

    @Autowired
    @JsonIgnore
    private TableService tableService;

    @Autowired
    @JsonIgnore
    private UploadRepository uploadRepository;

    @Autowired
    @JsonIgnore
    private BookmarkStateStorage bookmarkStateStorage;

    private String getFlowTopic(Boolean withDatadoc) {
        return withDatadoc ? String.format(BASE_FLOW_TOPIC + "/%s", getRequest().getDatadocId()) : BASE_FLOW_TOPIC;
    }

    private List<Long> getSourceIds(Flow flow){
        List<Long> inputSourceIds = new ArrayList<>(flow.getInputs().stream()
                .map(n -> ((InputNodeSettings) n.getSettings()).getUploadId())
                .collect(Collectors.toList()));
        // alongside with sources we'll send their parents (if we're ingesting source section)
        List<Upload> uploads = uploadRepository.getSources(inputSourceIds);
        inputSourceIds.addAll(uploads.stream()
                                      .filter(u -> u.getDescriptor().isSection())
                                      .map(AbstractFile::getParentId)
                                      .collect(Collectors.toList()));
        return inputSourceIds;
    }

    @Override
    public Map<TaskState, Runnable> getStates() {
        return ImmutableMap.of(FlowExecutionTaskState.EXECUTE, () -> {
            Map<String, NodeState> state = new ConcurrentHashMap<>();
            Flow flow = flowService.createFlow(getRequest().getAuth().getUserId(), getRequest().getFlowJSON(), getRequest().getFlowSettings());
            for (Node node : flow.getGraph().values()) {
                state.put(node.getId(), node.getState());
            }
            Long bookmarkId;
            List<Long> sourceIds = getSourceIds(flow);
            OutputNode outputNode;
            if (flow.getRoot() instanceof OutputNode) {
                outputNode = (OutputNode) flow.getRoot();
                bookmarkId = outputNode.getSettings().getBookmarkId();
                // override preserve history option
                outputNode.getSettings().setPreserveHistory(getRequest().isPreserveHistory());
                outputNode.getSettings().setParentTaskId(this.getId());
            } else {
                throw new RuntimeException("Flow should have an output");
            }

            Debounce<NodeState> progressCallback = new Debounce<>((ns) -> {
                state.put(ns.getNodeId(), ns);
                sockJSService.send(getRequest().getAuth(), getFlowTopic(!getRequest().isAutoIngest()),
                                   new FlowExecutionProgressEvent(getId(), bookmarkId, null, sourceIds, state), !getRequest().isAutoIngest());
                saveResults(bookmarkId, sourceIds, state);
            }, 1000);

            saveResults(bookmarkId, sourceIds, state);
            executor.execute(flow, progressCallback);
        });
    }

    private void saveResults(Long bookmarkId, List<Long> sourceIds, Map<String, NodeState> state) {
        setResult(new FlowExecutionResult(bookmarkId, sourceIds, state));
        saveState();
    }

    @Override
    public void cancel() {

    }

    @Override
    public void onBeforeStart(TaskInfo info) {
        Flow flow = flowService.createFlow(getRequest().getAuth().getUserId(), getRequest().getFlowJSON(), getRequest().getFlowSettings());
        Long bookmarkId = null;
        if (flow.getRoot() instanceof OutputNode) {
            bookmarkId = ((OutputNode) flow.getRoot()).getSettings().getBookmarkId();
        }
        TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
        sockJSService.send(getRequest().getAuth(), getFlowTopic(!getRequest().isAutoIngest()),
                           new FlowExecutionStartEvent(getId(),
                                                       bookmark.getId(),
                                                       bookmark.getName(),
                                                       getSourceIds(flow),
                                                       info.getStatistics().getRequestReceivedTime()), !getRequest().isAutoIngest());
    }

    @Override
    public void onAfterFinish(TaskInfo info) {
        Long user = getRequest().getAuth().getUserId();
        Flow flow = flowService.createFlow(user, getRequest().getFlowJSON(), getRequest().getFlowSettings());
        Long bookmarkId = null;
        if (flow.getRoot() instanceof OutputNode) {
            bookmarkId = ((OutputNode) flow.getRoot()).getSettings().getBookmarkId();
        }
        TableBookmark bookmark = tableRepository.getTableBookmarkForUser(bookmarkId, user);

        String errorCode = null, errorMessage = null;
        boolean error = info.isError();
//         TODO: Removing datadoc when ingesting cancelled. It needs review.
        Datadoc datadoc = bookmark.getDatadoc();

        if (this.isInterrupted() && bookmark.getTableSchema().getFirstCommitted() == null && !getRequest().getUserInitiated()) {
            try {
                tableRepository.deleteDatadoc(datadoc.getId());
                tableRepository.removeTableBookmark(bookmarkId);
            } catch (Exception e) {
                /* do ignore */
            }
        }
        if(error) {
            if (info.getStatistics().getErrors() != null) {
                errorCode = info.getStatistics().getErrors().get(0).getCode();
                errorMessage = info.getStatistics().getErrors().get(0).getMessage();
            } else {
                errorMessage = info.getStatistics().getLastErrorRootCauseMessage();
            }
        }
        if((getRequest().getScheduled() != null && getRequest().getScheduled()) && getSourceIds(flow).size() == 1) {
            handleDbIngestError(flow, datadoc, error, errorCode, errorMessage);
        }
        sockJSService.send(getRequest().getAuth(), getFlowTopic(!getRequest().isAutoIngest()),
                           new FlowExecutionCompleteEvent(getId(),
                                                          datadoc.getId(),
                                                          bookmark.getId(),
                                                          bookmark.getName(),
                                                          getSourceIds(flow),
                                                          info.getStatistics().getRequestReceivedTime(),
                                                          info.getStatistics().getRequestCompleteTime(),
                                                          error,
                                                          false,
                                                          errorCode,
                                                          errorMessage), !getRequest().isAutoIngest());
    }

    private void handleDbIngestError(Flow flow, Datadoc datadoc, boolean hasError, String errorCode, String errorMessage) {
        Upload upload = uploadRepository.getUploadById(getSourceIds(flow).get(0));
        if(upload.getDescriptor() instanceof DbDescriptor) {
            DbDescriptor descriptor = (DbDescriptor) upload.getDescriptor();
            if(descriptor != null && !hasError && !descriptor.isValid()) {
                descriptor.setDisconnectedTime(null);
                descriptor.setErrorString(errorMessage);
                descriptor.setErrorCode(errorCode);
                descriptor.setValid(true);
                uploadRepository.updateDescriptor(descriptor);
            } else if(descriptor != null && hasError && descriptor.isValid()){
                descriptor.setErrorString(errorMessage);
                descriptor.setErrorCode(errorCode);
            descriptor.setValid(false);
                if(descriptor.getDisconnectedTime() == null) {
                    descriptor.setDisconnectedTime(new Date());
                }
                mailService.send(datadoc.getUser().getEmail(), new DisconnectedSourceEmail(upload, datadoc, true));
                uploadRepository.updateDescriptor(descriptor);
            }
        }
    }

    @Override
    public void onTaskStopped(TaskInfo info) {
        FlowExecutionRequest request = getRequest();
        Flow flow = flowService.createFlow(request.getAuth().getUserId(), getRequest().getFlowJSON(), getRequest().getFlowSettings());
        Long bookmarkId = null;
        if (flow.getRoot() instanceof OutputNode) {
            bookmarkId = ((OutputNode) flow.getRoot()).getSettings().getBookmarkId();
        }

        FlowExecutionCompleteEvent event = FlowExecutionCompleteEvent.builder()
                .taskId(getId())
                .datadocId(request.getDatadocId())
                .bookmarkId(bookmarkId)
                .stopped(true)
                .build();
        sockJSService.send(getRequest().getAuth(), getFlowTopic(!getRequest().isAutoIngest()), event, !getRequest().isAutoIngest());
    }
}
