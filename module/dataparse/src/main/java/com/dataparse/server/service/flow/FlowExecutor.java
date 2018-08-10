package com.dataparse.server.service.flow;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.flow.PreviewRequest;
import com.dataparse.server.controllers.api.flow.PreviewResponse;
import com.dataparse.server.controllers.api.flow.ValidationRequest;
import com.dataparse.server.service.db.DbQueryHistoryItem;
import com.dataparse.server.service.flow.cache.FlowPreviewResultCache;
import com.dataparse.server.service.flow.cache.FlowPreviewResultCacheKey;
import com.dataparse.server.service.flow.cache.FlowPreviewResultCacheValue;
import com.dataparse.server.service.flow.node.*;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.ParserFactory;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableSchema;
import com.dataparse.server.service.storage.IStorageStrategy;
import com.dataparse.server.service.tasks.ExceptionWrapper;
import com.dataparse.server.service.upload.CollectionDelegateDescriptor;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FlowExecutor {

    private static final int DEFAULT_PREVIEW_LIMIT = 1000;

    @Autowired
    FlowService flowService;

    @Autowired
    ParserFactory parserFactory;

    @Autowired
    IStorageStrategy storageStrategy;

    @Autowired
    TableRepository tableRepository;

    @Autowired
    FlowPreviewResultCache flowPreviewResultCache;

    @Autowired
    BookmarkStateStorage bookmarkStateStorage;

    private void cleanup(Flow flow) {
        // cleanup tmp files
        List<String> inputPaths = flow.getSteps().stream()
                .flatMap(Collection::stream)
                .filter(n -> n instanceof DataSourceInputNode && n.getResult() instanceof FileDescriptor)
                .map(n -> ((FileDescriptor) n.getResult()).getPath())
                .collect(Collectors.toList());

        for (List<Node> step : flow.getSteps()) {
            for (Node node : step) {
                if (node.getResult() instanceof FileDescriptor) {
                    FileDescriptor fileDescriptor = (FileDescriptor) node.getResult();
                    String path = fileDescriptor.getPath();
                    if (!inputPaths.contains(path)) {
                        try {
                            storageStrategy.get(fileDescriptor).removeFile(((FileDescriptor) node.getResult()).getPath());
                        } catch (Exception e) {
                            // do nothing
                        }
                    }
                }
            }
        }
    }

    private void validate(Flow flow) {
        if (flow.getSteps().isEmpty()) {
            throw new RuntimeException("Flow is empty");
        }
        for (List<Node> step : flow.getSteps()) {
            step.forEach(Node::validate);
        }
    }

    private void executeInternal(Flow flow, boolean preview, Consumer<NodeState> nodeStateConsumer) {
        validate(flow);
        for (List<Node> step : flow.getSteps()) {
            step.forEach((n) -> {
                n.setFlow(flow);
                n.run(preview, nodeStateConsumer);
            });
        }
    }

    private List<Map<AbstractParsedColumn, Object>> postProcessData(Descriptor descriptor, List<Map<AbstractParsedColumn, Object>> data) {
        data = removeErrors(descriptor, data);
        return data;
    }

    private List<Map<AbstractParsedColumn, Object>> removeErrors(Descriptor descriptor, List<Map<AbstractParsedColumn, Object>> data) {
        Iterator<Map<AbstractParsedColumn, Object>> it = data.iterator();
        while (it.hasNext()) {
            Map<AbstractParsedColumn, Object> o = it.next();
            cols:
            for (ColumnInfo columnInfo : descriptor.getColumns()) {
                Object value = o.get(ParsedColumnFactory.getByColumnInfo(columnInfo));
                if (value instanceof ErrorValue && columnInfo.getSettings().isRemoveErrors()) {
                    it.remove();
                    break;
                } else if (value instanceof List) {
                    List l = (List) value;
                    for (Object listValue : l) {
                        if (listValue instanceof ErrorValue && columnInfo.getSettings().isRemoveErrors()) {
                            it.remove();
                            break cols;
                        }
                    }
                }
            }
        }
        return data;
    }

    public List<FlowValidationError> validate(Long userId, ValidationRequest request) {
//        todo change front
        BookmarkStateId stateId = new BookmarkStateId(request.getBookmarkId(), request.getStateId());
        BookmarkState state = bookmarkStateStorage.get(stateId, true).getState();
        return flowService.createFlow(userId, state.getFlowJSON(), state.getFlowSettings()).validate();
    }

    /**
     * Get last preview result from cache and ensure that sub-tree of a node has not changed since last request.
     */
    private Descriptor getCachedPreviewResult(Long userId, Long bookmarkId, String rootId, Flow flow) {
        Node root = flow.getGraph().get(rootId);
        FlowPreviewResultCacheValue cachedResults = flowPreviewResultCache.get(
                new FlowPreviewResultCacheKey(bookmarkId, rootId));
        if (cachedResults != null) {
            boolean sideEffect = false;
            if (root instanceof SideEffectNode) {
                root = Flow.findSource(root);
                Flow.findSource(flow.getGraph().get(rootId));
                sideEffect = true;
            }
            Flow cachedFlow = flowService.createFlow(userId, cachedResults.getFlowJSON(), cachedResults.getSettings());
            Node cachedRoot = cachedFlow.getGraph().get(root.getId());
            if (cachedRoot != null) {
                List<Node> cachedG = FlowTraverse.getBfsOrderedList(cachedRoot);
                List<Node> g = FlowTraverse.getBfsOrderedList(root);
                if (!sideEffect) {
                    cachedG.remove(cachedRoot);
                    g.remove(root);
                }
                if (g.equals(cachedG)) {
                    cachedResults.getDescriptor().setColumnTransforms(new ArrayList<>());
                    return Node.postProcess(cachedResults.getDescriptor(), root.getSettings());
                }
            }
        }
        return null;
    }

    private Boolean doesSourceExists(TableBookmark tableBookmark) {
        TableSchema tableSchema = tableBookmark.getTableSchema();
        if(tableSchema != null && tableSchema.getUploads().size() == 1) {
            return !tableSchema.getUploads().get(0).isDeleted();
        }
        return false;
    }

    private String chooseWriteFlowJson(TableBookmark tableBookmark, BookmarkState state, Boolean force) {
        boolean exists = doesSourceExists(tableBookmark);
        if(force && exists) {
            return state.getFlowJSON();
        } else {
            return state.getPendingFlowJSON() == null || !exists ? state.getFlowJSON() : state.getPendingFlowJSON();
        }
    }

    public PreviewResponse preview(Long userId, PreviewRequest request, Consumer<NodeState> nodeStateConsumer) {
        Stopwatch preparation = Stopwatch.createStarted();

        BookmarkStateId bookmarkStateId = request.getBookmarkStateId();
        bookmarkStateId.setUserId(userId);
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getTabId());
        BookmarkState state = bookmarkStateStorage.get(bookmarkStateId, true).getState();

        String json = chooseWriteFlowJson(tableBookmark, state, request.getForce());
        Flow flow = flowService.createFlow(userId, json, state.getFlowSettings(), request.getRootNode());
        Descriptor descriptor = request.getForce() ? null : getCachedPreviewResult(Auth.get().getUserId(), tableBookmark.getId(), request.getRootNode(), flow);

        boolean cached = true;
        List<DbQueryHistoryItem> queryHistoryItems = new ArrayList<>();
        log.info("Preview preparation took {}", preparation.stop());
        if (descriptor == null) {
            Stopwatch flowExecution = Stopwatch.createStarted();
            cached = false;
            executeInternal(flow, true, nodeStateConsumer);
            Node root = flow.getRoot();
            if (root instanceof SideEffectNode) {
                root = Flow.findSource(root);
            }
            descriptor = root.getResult();

            queryHistoryItems = flow.getGraph().values().stream()
                    .filter(n -> n instanceof DbQueryInputNode)
                    .map(n -> ((DbQueryInputNode) n).getQueryHistoryItem())
                    .collect(Collectors.toList());
            log.info("Execute flow internal took {}", flowExecution.stop());
        }

        for (ColumnInfo column : descriptor.getColumns()) {
            column.setErrorCount(0);
        }

        List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
        List<Map<AbstractParsedColumn, Object>> rawResult = new ArrayList<>();

        int skippedCount = 0;
        long count = 0;
        long limit = Optional.ofNullable(request.getLimit()).orElse(DEFAULT_PREVIEW_LIMIT);
        Stopwatch startParserInitialization = Stopwatch.createStarted();
        try (Parser parser = parserFactory.getParser(descriptor, true)) {
            try (RecordIterator it = parser.parse()) {
                log.info("Iterator initialization took {}", startParserInitialization.stop());
                Stopwatch startTime = Stopwatch.createStarted();
                while (it.hasNext() && count++ < limit) {
                    Map<AbstractParsedColumn, Object> o = it.next();
                    for (ColumnInfo column : descriptor.getColumns()) {
                        AbstractParsedColumn header = ParsedColumnFactory.getByColumnInfo(column);
                        Object value = o.get(header);
                        if (value instanceof ErrorValue) {
                            column.setErrorCount(column.getErrorCount() + 1);

                            if (column.getFirstError() == null) {
                                column.setFirstError((ErrorValue) value);
                            }
                        }
                    }
                    result.add(o);
                    rawResult.add(o);
                }
                log.info("Retrieve preview rows {} took {}", count, startTime.stop());
            }
        } catch (IOException e) {
            throw ExceptionWrapper.wrap(e);
        }

        int sizeBefore = result.size();
        result = postProcessData(descriptor, result);
        skippedCount = sizeBefore - result.size();
        if (!cached && descriptor.isCacheable()) { // todo improve to cache union node results etc
            flowPreviewResultCache.put(
                    new FlowPreviewResultCacheKey(request.getTabId(), request.getRootNode()),
                    new FlowPreviewResultCacheValue(state.getFlowJSON(), state.getFlowSettings(), new CollectionDelegateDescriptor(descriptor, rawResult)));
        }
        cleanup(flow);
        if (descriptor instanceof CollectionDelegateDescriptor) {
            descriptor = ((CollectionDelegateDescriptor) descriptor).getD();
        }
        return new PreviewResponse(descriptor, result, cached, queryHistoryItems, skippedCount);
    }

    public void execute(Flow flow, Consumer<NodeState> nodeStateConsumer) {
        executeInternal(flow, false, nodeStateConsumer);
        cleanup(flow);
    }

}
