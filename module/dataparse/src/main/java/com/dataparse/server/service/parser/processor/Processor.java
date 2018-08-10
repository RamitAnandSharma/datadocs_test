package com.dataparse.server.service.parser.processor;

import com.dataparse.server.service.visualization.bookmark_state.state.*;

import java.util.*;

public interface Processor {

    Map<String, Object> process(Map<String, Object> o, Map<String, Show> show);

    Object format(Object o, Col col, AggOp op);
}
