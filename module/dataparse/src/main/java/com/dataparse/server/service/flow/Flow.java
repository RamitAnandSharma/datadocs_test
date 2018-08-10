package com.dataparse.server.service.flow;

import com.dataparse.server.service.flow.builder.*;
import com.dataparse.server.service.flow.builder.FlowContainerDTO.*;
import com.dataparse.server.service.flow.node.*;
import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.flow.transform.Transform;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flow is a directed non-cyclic graph (tree) that can be represented with a two-dimensional array, eg
 *     D
 *   /
 * A -- B -- C
 * This graph can be represented as [[A], [B, D], [C]] and it is executed in reverse.
 */
@Data
public class Flow {

    private Long userId;
    private FlowSettings settings;
    private Map<String, Node> graph;
    private List<List<Node>> steps;
    private Node root;

    private Flow(){
        // instantiate through build method only
    }

    @SuppressWarnings("unchecked")
    public List<FlowValidationError> validate(){
        List<FlowValidationError> errors = graph.values().stream()
                .flatMap(x -> (Stream<FlowValidationError>) x.getValidationErrors().stream())
                .collect(Collectors.toList());
        if(root == null){
            errors.add(new FlowValidationError(null, "No Output is added to model"));
        }
        return errors;
    }

    public List<InputNode> getInputs(){
        return graph.values().stream()
                .filter(n -> n instanceof InputNode)
                .map(n -> (InputNode) n)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static void collectLevels(Node node, int level, Multimap<Integer, Node> levels, Set<String> visited){
        if(visited.contains(node.getId())) {
            throw new RuntimeException("Graph should not contain cycles!");
        }
        visited.add(node.getId());
        levels.put(level, node);
        List<Node> children = node.getChildren();
        for(Node child : children){
            collectLevels(child, level + 1, levels, visited);
        }
    }

    private static String findRootNode(List<FlowItem> items){
        for(FlowItem item: items){
            if(item instanceof OutputNodeItem){
                return item.getId();
            }
        }
        return null;
    }

    public static Node findSource(Node root){
        Node node = root;
        while(node instanceof SideEffectNode){
            node = (Node) node.getChildren().get(0);
        }
        return node;
    }

    public static Flow create(Long userId, String flowJSON, FlowSettings settings, ApplicationContext applicationContext){
        return create(userId, flowJSON, settings, null, applicationContext);
    }

    public static Flow create(Long userId, String flowJSON, FlowSettings settings, String rootNode, ApplicationContext applicationContext){
        FlowContainerDTO flowContainer = FlowContainerDTO.fromString(flowJSON);

        Map<String, Node> graph = new HashMap<>();
        List<Pair<String, String>> edges = new ArrayList<>();
        for(FlowItem item : flowContainer.getCells()){
            if (item instanceof LinkItem) {
                LinkItem linkItem = ((LinkItem) item);
                edges.add(Pair.of(linkItem.getSource().getId(), linkItem.getTarget().getId()));
            } else if (item instanceof NodeItem){
                graph.put(item.getId(), ((NodeItem) item).toNode());
            }
        }

        String rootNodeID = rootNode == null ? findRootNode(flowContainer.getCells()) : rootNode;
        for(Node node : graph.values()){
            node.setParent(graph.get(edges.stream()
                    .filter(e -> e.getLeft() != null && e.getLeft().equals(node.getId()))
                    .findFirst().orElseGet(() -> Pair.of(null, null)).getLeft()));
            node.setChildren(edges.stream()
                    .filter(e -> e.getRight() != null && e.getRight().equals(node.getId()))
                    .map(p -> graph.get(p.getLeft()))
                    .collect(Collectors.toList()));
            applicationContext.getAutowireCapableBeanFactory()
                    .autowireBeanProperties(node, AutowireCapableBeanFactory.AUTOWIRE_BY_NAME, false);
            for(Transform transform : node.getSettings().getTransforms()){
                applicationContext.getAutowireCapableBeanFactory()
                        .autowireBeanProperties(transform, AutowireCapableBeanFactory.AUTOWIRE_BY_NAME, false);
            }
        }

        return Flow.create(graph, settings, rootNodeID, userId);
    }

    private static Flow create(Map<String, Node> graph, FlowSettings settings, String root, Long userId){
        Node rootNode = null;
        Multimap<Integer, Node> levels = HashMultimap.create();
        if(root != null) {
            rootNode = graph.get(root);
            if (rootNode != null) {
                rootNode.setRoot(true);
            }
            collectLevels(rootNode, 0, levels, Sets.newHashSet());
        }
        List<List<Node>> steps = new ArrayList<>(levels.keySet().size());
        for(Integer level : levels.keySet()){
            steps.add(level, new ArrayList<>(levels.get(level)));
        }
        Collections.reverse(steps);
        Flow flow = new Flow();
        flow.setUserId(userId);
        flow.setRoot(rootNode);
        flow.setSteps(steps);
        flow.setSettings(settings);
        flow.setGraph(graph);
        return flow;
    }

}
