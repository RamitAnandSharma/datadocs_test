package com.dataparse.server.service.flow.builder;

import com.dataparse.server.service.flow.builder.FlowContainerDTO.*;
import com.dataparse.server.service.flow.node.*;
import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.upload.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.*;

public class FlowBuilder {

    private FlowContainerDTO flowContainer;

    private FlowBuilder(final FlowContainerDTO flowContainer) {
        this.flowContainer = flowContainer;
    }


    public static FlowBuilder create(){
        return new FlowBuilder(new FlowContainerDTO());
    }

    public static FlowBuilder create(String flowJSON){
        FlowContainerDTO flowContainer = FlowContainerDTO.fromString(flowJSON);
        return new FlowBuilder(flowContainer);
    }

    @SuppressWarnings("unchecked")
    public FlowBuilder withDataSource(String id, Upload upload){
        NodeItem in;
        InputNodeSettings settings;
        if(upload.getDescriptor() instanceof DbDescriptor) {
            in = new DbQueryInputNodeItem();
            in.setLabel("custom_query");
            settings = new DbQueryInputNodeSettings();
            settings.setColumns(new ArrayList<>());
        } else {
            in = new DataSourceInputNodeItem();
            in.setLabel(upload.getName());
            settings = new InputNodeSettings();
            AtomicInteger counter = new AtomicInteger();
            settings.setColumns(upload.getDescriptor().getColumns().stream()
                                        .map(column -> {
                                            ColumnSettings columnSettings = new ColumnSettings();
                                            columnSettings.setName(column.getName());
                                            columnSettings.setInitialIndex(column.getInitialIndex());
                                            columnSettings.setType(column.getType());
                                            columnSettings.setIndex(column.getInitialIndex() == null ? counter.getAndIncrement() : column.getInitialIndex());
                                            switch (column.getType().getDataType()){
                                                case STRING:
                                                    columnSettings.setSearchType(SearchType.EDGE);
                                                    break;
                                                case DECIMAL:
                                                    columnSettings.setSearchType(SearchType.EXACT_MATCH);
                                                    break;
                                            }
                                            columnSettings.setPkey(column.isPkey());
                                            return columnSettings;
                                        }).collect(Collectors.toList()));
        }
        in.setId(id);
        settings.setTransforms(new ArrayList<>());
        settings.setUploadId(upload.getId());
        in.setSettings(settings);
        flowContainer.getCells().add(in);
        return this;
    }

    public FlowBuilder withOutput(String id, Long bookmarkId){
        OutputNodeItem out = new OutputNodeItem();
        out.setId(id);
        out.setLabel("Output");
        OutputNodeSettings settings = new OutputNodeSettings();
        settings.setBookmarkId(bookmarkId);
        settings.setTransforms(new ArrayList<>());
        out.setSettings(settings);
        flowContainer.getCells().add(out);
        return this;
    }

    public FlowBuilder withLink(String id, String sourceId, String targetId){
        flowContainer.getCells().add(new LinkItem(id, new LinkNode(sourceId), new LinkNode(targetId)));
        return this;
    }

    public FlowContainerDTO build(){
        return flowContainer;
    }

}
