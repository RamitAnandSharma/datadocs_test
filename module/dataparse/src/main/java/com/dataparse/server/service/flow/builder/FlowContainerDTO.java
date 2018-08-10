package com.dataparse.server.service.flow.builder;

import com.dataparse.server.service.flow.node.*;
import com.dataparse.server.util.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
public class FlowContainerDTO {

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
                          @JsonSubTypes.Type(value = DataSourceInputNodeItem.class, name = "html.SourceInputNode"),
                          @JsonSubTypes.Type(value = DbQueryInputNodeItem.class, name = "html.QueryInputNode"),
                          @JsonSubTypes.Type(value = DbTableInputNodeItem.class, name = "html.TableInputNode"),
                          @JsonSubTypes.Type(value = OutputNodeItem.class, name = "html.OutputNode"),
                          @JsonSubTypes.Type(value = LinkItem.class, name = "link")
                  })
    public static class FlowItem {
        private String id;
        private String label;
    }

    public static abstract class NodeItem<T extends Node, S extends Settings> extends FlowItem {
        public abstract T toNode();
        public abstract void setSettings(S s);
    }

    @Data
    @NoArgsConstructor
    public static class DbQueryInputNodeItem extends NodeItem<InputNode, DbQueryInputNodeSettings> {
        private DbQueryInputNodeSettings settings;

        @Override
        public DbQueryInputNode toNode() {
            DbQueryInputNode node = new DbQueryInputNode(getId());
            node.setSettings(settings);
            return node;
        }
    }

    @Data
    @NoArgsConstructor
    public static class DbTableInputNodeItem extends NodeItem<InputNode, DbTableInputNodeSettings> {
        private DbTableInputNodeSettings settings;

        @Override
        public DbTableInputNode toNode() {
            DbTableInputNode node = new DbTableInputNode(getId());
            node.setSettings(settings);
            return node;
        }
    }

    @Data
    @NoArgsConstructor
    public static class DataSourceInputNodeItem extends NodeItem<InputNode, InputNodeSettings> {
        private InputNodeSettings settings;

        @Override
        public DataSourceInputNode toNode() {
            DataSourceInputNode node = new DataSourceInputNode(getId());
            node.setSettings(settings);
            return node;
        }
    }

    @Data
    @NoArgsConstructor
    public static class OutputNodeItem extends NodeItem<OutputNode, OutputNodeSettings> {
        private OutputNodeSettings settings;

        @Override
        public OutputNode toNode() {
            OutputNode node = new OutputNode(getId());
            node.setSettings(settings);
            return node;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LinkNode {
        private String id;
    }

    @Data
    @NoArgsConstructor
    public static class LinkItem extends FlowItem {
        private LinkNode source;
        private LinkNode target;

        public LinkItem(final String id, final LinkNode source, final LinkNode target) {
            this.source = source;
            this.target = target;
            this.setId(id);
        }
    }

    private List<FlowItem> cells = new ArrayList<>();

    public String toString() {
        return JsonUtils.writeValue(this);
    }

    public static FlowContainerDTO fromString(String flowJSON) {
        return JsonUtils.readValue(flowJSON, FlowContainerDTO.class);
    }

}
