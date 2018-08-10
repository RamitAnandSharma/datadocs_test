package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.upload.*;
import org.springframework.beans.factory.annotation.*;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class UnionNode extends Node<UnionNodeSettings> {

    @Autowired
    UploadRepository uploadRepository;

    @Autowired
    ParserFactory parserFactory;

    public UnionNode(String id) {
        super(id);
    }

    @Override
    protected Descriptor execute(boolean preview, Consumer<NodeState> nodeStateConsumer) {
        List<Descriptor> descriptors = getChildren()
                .stream().map(Node::getResult)
                .collect(Collectors.toList());
        CompositeDescriptor descriptor = new CompositeDescriptor();
        descriptor.setRowsExactCount(descriptors.stream().mapToLong(Descriptor::getRowsCount).sum());
        descriptor.setDescriptors(descriptors);
        descriptor.setColumns(ColumnInfo.aggregateColumns(descriptors));
        return descriptor;
    }

    @Override
    protected String getNodeTypeName() {
        return "Union";
    }

    @Override
    protected int getExpectedChildrenCount() {
        return AT_LEAST_ONE_CHILD;
    }
}
