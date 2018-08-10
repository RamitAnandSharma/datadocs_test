package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.upload.Descriptor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.function.Consumer;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class SideEffectNode<T extends Settings> extends Node<T> {

    public SideEffectNode(String id) {
        super(id);
    }

    abstract public void execute(Consumer<NodeState> nodeStateConsumer);

    abstract public void onAfterExecute(Descriptor descriptor);

    @Override
    protected Descriptor execute(boolean preview, Consumer<NodeState> nodeStateConsumer) {
        Descriptor descriptor = getChildren().get(0).getResult();
        if(!preview) {
            execute(nodeStateConsumer);
            descriptor.setColumns(ColumnInfo.aggregateColumns(Collections.singletonList(descriptor)));
            onAfterExecute(descriptor);
        }
        return descriptor;
    }

    @Override
    public boolean applySettings() {
        return false;
    }

    @Override
    protected int getExpectedChildrenCount() {
        return 1;
    }

}
