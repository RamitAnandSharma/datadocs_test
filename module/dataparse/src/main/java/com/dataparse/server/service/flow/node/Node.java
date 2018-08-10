package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.flow.*;
import com.dataparse.server.service.flow.settings.ColumnSettings;
import com.dataparse.server.service.flow.transform.ColumnTransform;
import com.dataparse.server.service.flow.transform.TableTransform;
import com.dataparse.server.service.flow.transform.Transform;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.upload.Descriptor;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Data
@EqualsAndHashCode(exclude = {"children", "parent"})
public abstract class Node<T extends Settings> {

    protected final static int ANY_CHILDREN_NUMBER = -1;
    protected final static int AT_LEAST_ONE_CHILD = -2;

    private Flow flow;

    public Node(String id){
        this.id = id;
        this.state = new NodeState(id);
    }

    protected abstract String getNodeTypeName();
    protected abstract Descriptor execute(boolean preview, Consumer<NodeState> nodeStateConsumer);
    protected abstract int getExpectedChildrenCount();

    public String getNodeName(){
        return StringUtils.isBlank(label) ? getNodeTypeName() : "\"" + label + "\"";
    }

    public List<FlowValidationError> getValidationErrors(){
        List<FlowValidationError> errors = new ArrayList<>();
        if(ANY_CHILDREN_NUMBER == getExpectedChildrenCount()){
            // do nothing
        } else if (AT_LEAST_ONE_CHILD == getExpectedChildrenCount()) {
            if(children.isEmpty()){
                 errors.add(new FlowValidationError(id, getNodeName() + " needs to have at least one input"));
            }
        } else if(children.size() != getExpectedChildrenCount()){
            errors.add(new FlowValidationError(id, getNodeName() + " needs to have exactly " + getExpectedChildrenCount() + " inputs"));
        }
        if(!root && parent == null){
            errors.add(new FlowValidationError(id, getNodeName() + " needs to have an output"));
        }
        return errors;
    }

    public void validate(){
        List<FlowValidationError> errors = getValidationErrors();
        if(!errors.isEmpty()){
            throw ExecutionException.of(errors);
        }
    }

    public void run(boolean preview, Consumer<NodeState> nodeStateConsumer){
        this.state.setPercentComplete(0.);
        this.state.setState(NodeStateEnum.RUNNING);
        nodeStateConsumer.accept(state);
        this.result = execute(preview, nodeStateConsumer);
        if(applySettings()) {
            this.result = postProcess(this.result, settings);
        }
        this.state.setPercentComplete(100.);
        this.state.setState(NodeStateEnum.FINISHED);
        nodeStateConsumer.accept(state);
    }

    public boolean applySettings(){
        return true;
    }

    // todo this is potentially long operation. how do we deal with it?
    public static Descriptor postProcess(Descriptor result, Settings settings) {
        if(result != null) {
            if(settings.getColumns() != null && settings.getColumns().size() > 0 && result.getColumns() != null) {
                Map<AbstractParsedColumn, ColumnSettings> columnSettings = Maps.uniqueIndex(settings.getColumns(), ParsedColumnFactory::getByColumnSettings);
                for(ColumnInfo columnInfo: result.getColumns()){
                    ColumnSettings s = columnSettings.get(ParsedColumnFactory.getByColumnInfo(columnInfo));
                    if(s != null) {
                        columnInfo.getSettings().copyFrom(s);
                    }
                }
            }

            if(settings.getTransforms() != null && settings.getTransforms().size() > 0) {
                List<ColumnTransform> currentStepTransforms = new ArrayList<>();
                LinkedHashMap<TableTransform, List<ColumnTransform>> steps = new LinkedHashMap<>();
                for(Transform transform : settings.getTransforms()){
                    if(transform instanceof TableTransform){
                        steps.put((TableTransform) transform, currentStepTransforms);
                        currentStepTransforms = new ArrayList<>();
                    } else if (transform instanceof ColumnTransform){
                        currentStepTransforms.add((ColumnTransform) transform);
                    }
                }
                for(TableTransform tableTransform : steps.keySet()){
                    result.getColumnTransforms().addAll(steps.get(tableTransform));
                    // todo how to cancel? how to track progress? cleanup intermediate descriptors!
                    result = tableTransform.apply(result);
                }
                result.getColumnTransforms().addAll(currentStepTransforms);
            }
        }
        return result;
    }

    private boolean root;
    private String id;
    private NodeState state;
    private String label;
    private Descriptor result;
    private T settings;
    private Node parent;
    private List<Node> children;
}
