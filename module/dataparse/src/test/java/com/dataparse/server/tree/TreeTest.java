package com.dataparse.server.tree;

import com.dataparse.server.service.visualization.*;
import com.google.common.collect.*;
import lombok.extern.slf4j.*;
import org.junit.*;

import java.util.*;
import java.util.stream.*;

import static org.junit.Assert.assertEquals;

@Slf4j
public class TreeTest {

  @Test
  public void testExpandableTree() {
    Tree<Integer> tree = new Tree<>();
    tree.getRoot().setData(0);
    tree.getRoot().setKey(0);
    Tree.Node<Integer> leftChild = new Tree.Node<>(1, 1);
    leftChild.addChild(new Tree.Node<>(2, 2));
    leftChild.addChild(new Tree.Node<>(3, 3));
    tree.getRoot().addChild(leftChild);
    Tree.Node<Integer> rightChild = new Tree.Node<>(4, 4);
    rightChild.addChild(new Tree.Node<>(5, 5));
    rightChild.addChild(new Tree.Node<>(6, 6));
    tree.getRoot().addChild(rightChild);

    List<Integer> result = Lists.newArrayList(tree.getRoot().iterator(ImmutableMap.of("0", true, "0_4", true))).stream()
        .map(Tree.Node::getData)
        .collect(Collectors.toList());

    result.forEach(o -> log.info("{}", o));
    assertEquals(Lists.newArrayList(0, 1, 4, 5, 6), result);

  }

}
