package com.dataparse.server.service.parser.iterator;

import com.dataparse.server.service.visualization.*;

import java.util.*;

public interface ProgressAwareIterator<E> extends Iterator<E> {

    Tree.Node<Map<String, Object>> getHeaders();

    Map<String, Object> getTotals();

    Double getComplete();

}
