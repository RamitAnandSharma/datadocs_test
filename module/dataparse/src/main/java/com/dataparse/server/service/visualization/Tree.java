package com.dataparse.server.service.visualization;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.collections.ListUtils;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;

@Data
public class Tree<T> implements Serializable {

    public static final String ROOT_KEY = "root";
    private Node<T> root;

    public Tree() {
        this(0);
    }

    public Tree(int size) {
        this.root = new Tree.Node<>(size);
        this.root.key = ROOT_KEY;
    }

    public Tree(Tree.Node<T> root) {
        this.root = root;
    }

    public void create(List<Object> key, T data){
        Tree.Node<T> node = root;
        for(int i = 0; i < key.size(); i++){
            Object keyPart = key.get(i);
            Tree.Node<T> child = null;
            if(i < key.size() - 1){ // search if not last level, saves lots of time
                child = node.getChildren().stream().filter(n -> n.equalsKey(keyPart)).findFirst().orElse(null);
            }
            if(child == null){
                child = new Node<>();
                child.setKey(keyPart);
                child.setParent(node);
                node.getChildren().add(child);
            }
            node = child;
        }
        node.setData(data);
    }

    public void update(List<Object> key, Function<T, T> updateFn){
        Tree.Node<T> node = root;
        for(int i = 0; i < key.size(); i++){
            Object keyPart = key.get(i);
            Tree.Node<T> child = node.getChildren().stream().filter(n -> n.equalsKey(keyPart)).findFirst().orElse(null);
            if(child == null){
                child = new Node<>();
                child.setKey(keyPart);
                child.setParent(node);
                node.getChildren().add(child);
            }
            node = child;
        }
        node.setData(updateFn.apply(node.getData()));
    }

    @Data
    @ToString(exclude = {"parent"})
    public static class Node<T> implements Serializable {

        private Object key;

        public Node(){
            this(0);
        }

        public Node(T data){
            this(0);
            this.data = data;
        }

        public Node(Object key, T data){
            this(0);
            this.key = key;
            this.data = data;
        }

        public Node(int children){
            this.children = children > 0 ? new ArrayList<>(children) : new ArrayList<>();
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private T data;
        @JsonIgnore
        private Node<T> parent;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private List<Node<T>> children;

        public void addChild(Node<T> child){
            child.setParent(this);
            this.children.add(child);
        }

        public boolean equalsKey(Object key){
            return Objects.equals(this.key, key);
        }

        @SuppressWarnings("unchecked")
        public List<Node<T>> path(){
            return ListUtils.union(getParent() == null ? new ArrayList() : getParent().path(), Collections.singletonList(this));
        }

        public int depth() {
            return getParent() == null ? 1 : getParent().depth() + 1;
        }

        public Iterator<Node<T>> iterator() {
            return new PreWalkTreeIterator();
        }

        public Iterator<Node<T>> iterator(Map<String, Boolean> expandedState){
            return new PreWalkTreeIterator(expandedState);
        }

        class PreWalkTreeIterator implements Iterator<Node<T>> {

            private Node<T> cached;
            private Map<String, Boolean> expandedState;

            // All the iterators of all of the sub-trees that weren't null.
            List<Iterator<Node<T>>> iterators = new LinkedList<>();
            // Have we delivered the element?
            private boolean deliveredElement = false;

            PreWalkTreeIterator(){
            }

            PreWalkTreeIterator(Map<String, Boolean> expandedState){
                this.expandedState = expandedState;
            }

            @Override
            public boolean hasNext() {
                if(cached != null){
                    return true;
                } else {
                    cached = next();
                    return cached != null;
                }

            }

            @Override
            public Node<T> next() {
                if(cached != null){
                    Node<T> tmp = cached;
                    cached = null;
                    return tmp;
                }
                // We now deliver our element.
                if(!deliveredElement){
                    List<Object> keysPath = TreeUtils.getParentKeysPath(Node.this);
                    String fullKey = TreeUtils.getField(keysPath);
                    if(expandedState == null || keysPath.isEmpty() || expandedState.containsKey(fullKey) && expandedState
                            .get(fullKey)) {
                        List<Node<T>> children = Node.this.getChildren();
                        iterators.addAll(children.stream()
                                                 .map(node -> node.iterator(expandedState))
                                                 .collect(Collectors.toList()));
                    }
                    deliveredElement = true;
                    return Node.this;
                } else {
                    // First consume the iterators.
                    while (iterators.size() > 0) {
                        // Grab the first one.
                        Iterator<Node<T>> it = iterators.get(0);
                        // Has it got an entry?
                        if (it.hasNext()) {
                            // Return it's next.
                            return it.next();
                        } else {
                            // It's exhausted - remove it.
                            iterators.remove(it);
                        }
                    }
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove() is not supported.");
            }

        }
    }
}